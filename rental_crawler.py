
import datetime
import logging
import re

from bs4 import BeautifulSoup
from elasticsearch import Elasticsearch
import requests

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)
log_filename = datetime.datetime.now().strftime("log - %m_%d_%H_%M_%S.log")
fh = logging.FileHandler(log_filename)
ch = logging.StreamHandler()
log_format = '[%(levelname)s][%(filename)s][%(lineno)d][%(funcName)s] %(message)s. (%(asctime)s)'
log_datefmt = '%m-%d %H:%M:%S'
formatter = logging.Formatter(log_format, log_datefmt)
fh.setFormatter(formatter)
ch.setFormatter(formatter)
log.addHandler(fh)
log.addHandler(ch)

objnum_per_page = 30
region_dict = {
    '台北市': '1',
    '新北市': '3'
}

def get_rental_objs(region_list):
    rental_objs = {}
    for region in region_list:
        rental_obj_links = []
        idx = 0
        headers = {'Cookie': 'urlJumpIp=' + region_dict[region]}
        log.info(f'Get the list of rental objects by region {region}')
        while True:
            # Get the list of rental objects by region and row number
            response = requests.get(f'https://rent.591.com.tw/?kind=0&region={region_dict[region]}&firstRow={str(idx)}', headers=headers)
            if not response.ok:
                log.error(f'Cannot get the rental object of {region}. Url: {response.url}. Status: {response.status_code}')
                continue
                
            soup = BeautifulSoup(response.text, 'html.parser')
            # Get all links of rental objects in this page
            for item in soup.find(id='content').findChildren('ul', recursive=False):
                info_content = item.find('li', class_='infoContent')
                if info_content:
                    obj_link = info_content.find('h3').find('a').get('href')
                    if obj_link:
                        rental_obj_links.append('https:' + obj_link)
                        
            # Go to next page if it's not the last page
            if 'last' in soup.find('a', class_='pageNext').get('class'):
                break
            idx += objnum_per_page
        log.info(f'Done. Total {len(rental_obj_links)} links')

        for i, link in enumerate(rental_obj_links):
            rental_obj = {}
            rental_obj['物件號'] = re.search(r'(\d+).html', link).group(1)
            # Get rental object information
            log.info(f'{i} Get rental object information from link {link}')
            response = requests.get(link)
            if not response.ok:
                log.warning(f'Request {link} failed')
                continue
            
            soup = BeautifulSoup(response.text, 'html.parser')
            if soup.findChild('dl', class_='error_img'):
                log.warning(f'Rental object cannot be found')
                continue
            # Get all required information
            # Get information from propNav
            prop_nav = soup.findChild(id='propNav')
            rental_obj['地址'] = prop_nav.findChild('span', class_='addr').text
            rental_obj['縣市'] = rental_obj['地址'][0:3]
            if rental_obj['縣市'] != region:
                log.warning(f'Found an object from {rental_obj["縣市"]}. Ignore.')
                continue
            rental_obj['鄉鎮區'] = rental_obj['地址'][3:6]
            rental_obj['收藏'] = int(re.search(r'\((.*)\)', prop_nav.findChild(id='j_addfav').text).group(1))
            # Get information from pageView
            page_view = soup.findChild('div', class_='pageView')
            rental_obj['瀏覽次數'] = {}
            rental_obj['瀏覽次數']['電腦'] = int(re.search(r'\d+', page_view.findChild('span', class_='pc').find_next_sibling().text).group())
            rental_obj['瀏覽次數']['手機'] = int(re.search(r'\d+', page_view.findChild('span', class_='mobile').find_next_sibling().text).group())
            rental_obj['瀏覽次數']['共'] = rental_obj['瀏覽次數']['電腦'] + rental_obj['瀏覽次數']['手機']
            # Get information from the right area
            rightbox = soup.find('div', class_='rightBox')
            detail_info = rightbox.findChild('div', class_='detailInfo')
            rental_obj['租金'] = int(re.search(r'\d+', detail_info.findChild('div', class_='price').text.strip().replace(',', '')).group())
            rental_obj['租金包含'] = detail_info.findChild('div', class_='explain').text
            for i in detail_info.findChild('ul').findChildren('li', recursive=False):
                info = i.text.replace('\xa0', '').split(':')
                rental_obj[info[0]] = info[1]
            room_match = re.search(r'(^\d+)房', rental_obj.get('格局') or '')
            if room_match:
                rental_obj['房間數'] = int(room_match.group())
            sqft_match = re.search(r'(^\d+)坪', rental_obj.get('坪數') or '')
            if sqft_match:
                rental_obj['坪'] = int(sqft_match.group())
            user_info = rightbox.findChild('div', class_='userInfo')
            avatar_right_match = re.search(r'(?P<name>.*)[（(](?P<role>.*)', user_info.findChild('div', class_='avatarRight').text)
            rental_obj['出租者'] = avatar_right_match.group('name')
            rental_obj['出租者身份'] = avatar_right_match.group('role')[0:2]
            rental_obj['聯絡電話'] = (user_info.findChild('span', class_='dialPhoneNum') or {}).get('data-value') or ''
            # Get information from the left-center area
            label_list = soup.find('ul', class_='labelList')
            odd_word = '非於政府免付費公開資料可查詢'
            for i in zip(label_list.findChildren('div', class_='one'), label_list.findChildren('div', class_='two')):
                label = i[0].text.replace(' ','')
                value = i[1].findChild('em').text.strip()
                if odd_word in label:
                    label = label.replace(odd_word, '')
                    value = value + ' (' + odd_word + ')'
                rental_obj[label] = value
            # Get 屋況說明
            rental_obj['屋況說明'] = soup.find('div', class_='houseIntro').text.replace('\xa0', '')
            
            # Add this rental object to object list
            rental_objs[rental_obj['物件號']] = rental_obj
            # log.info(f'{rental_obj}\n')
    return rental_objs


def save_data(data):
    es = Elasticsearch()
    idx = 'rental_objs'
    # es.indices.delete(index=idx, ignore=[400, 404])
    for obj_id in data:
        es.index(index=idx, body=data[obj_id], id=obj_id)



if __name__ == '__main__':
    rental_objs = get_rental_objs(['台北市', '新北市'])
    save_data(rental_objs)
