import time
import requests
from collections import namedtuple
from typing import List, Tuple, Iterator, Dict

import bs4
from bs4 import BeautifulSoup
import pandas as pd
from pandas import Series, DataFrame
from tqdm import tqdm


Room = namedtuple('Room', [
    'name',
    'address',
    'location0',
    'location1',
    'location2',
    'age',
    'height',
    'floor',
    'rent',
    'admin',
    'deposit',
    'gratuity',
    'floor_plan',
    'area',
    'architecture',
])


def get_page_num(url: str) -> int:

    def li_content2int(li: bs4.element.Tag) -> int:
        s =li.text
        return int(s) if s.isdigit() else 0

    def max_page_number(pages: bs4.element.Tag) -> int:
        page_nums = list(map(li_content2int, pages.find_all('li')))
        return max(page_nums)

    result = requests.get(url)
    soup = BeautifulSoup(result.content, 'html5lib')

    body = soup.find('body')
    pages = body.find('div', {'class': 'pagination pagination_set-nav'})

    return max_page_number(pages) if pages is not None else 0


def get_building_info(cassette: bs4.element.Tag) -> Tuple[str]:
    subtitle = cassette.find(
        'div', {'class': 'cassetteitem_content-title'}).text
    subaddress = cassette.find(
        'li', {'class': 'cassetteitem_detail-col1'}).text

    sublocations = cassette.find(
        'li', {'class': 'cassetteitem_detail-col2'}
    ).find_all(
        'div', {'class': 'cassetteitem_detail-text'})

    loc0, loc1, loc2 = list(map(lambda x: x.text, sublocations))[:3]

    age, height = list(map(
        lambda x: x.text, cassette.find('li', {'class': 'cassetteitem_detail-col3'}).find_all('div')))

    return subtitle, subaddress, loc0, loc1, loc2, age, height


def get_room_info(tbody: bs4.element.Tag) -> Tuple[str]:
    floor = tbody.find('tr', 'js-cassette_link').find_all('td')[2].text.strip()
    rent = tbody.find(
        'span', {'class': 'cassetteitem_price cassetteitem_price--rent'}).text
    admin = tbody.find(
        'span', {'class': 'cassetteitem_price cassetteitem_price--administration'}).text
    deposit = tbody.find(
        'span', {'class': 'cassetteitem_price cassetteitem_price--deposit'}).text
    gratuity = tbody.find(
        'span', {'class': 'cassetteitem_price cassetteitem_price--gratuity'}).text
    floor_plan = tbody.find('span', {'class': 'cassetteitem_madori'}).text
    area = tbody.find('span', {'class': 'cassetteitem_menseki'}).text
    return floor, rent, admin, deposit, gratuity, floor_plan, area


def scrape_pages(url: str, district: str = 'default') -> List[Room]:
    rooms = []
    for i, arch in enumerate(['鉄筋系', '鉄骨系', '木造', 'ブロックその他']):
        url_kz = url + f'&kz={i + 1}'
        n = get_page_num(url_kz)
        time.sleep(1.05)

        for page in tqdm(range(n), position=0, desc=f'{district} + {arch}'):
            result = requests.get(
                url_kz + (f'&pn={page + 1}' if page > 0 else ''))
            start = time.time()
            soup = BeautifulSoup(result.content, 'html5lib')
            summary = soup.find('div', {'id': 'js-bukkenList'})
            cassetteitems = summary.find_all('div', {'class': 'cassetteitem'})
            for c in cassetteitems:
                b_info = get_building_info(c)
                for tbody in c.find_all('tbody'):
                    r_info = get_room_info(tbody)
                    r = Room(*b_info, *r_info, arch)
                    rooms.append(r)
            time.sleep(max(0, 1.05 - (time.time() - start)))
    return rooms


def scrape_district_ids(url: str) -> Dict[str, str]:
    result = requests.get(url)
    soup = BeautifulSoup(result.content, 'html5lib')

    ids = {}
    districts = soup.find('table', {'class': 'searchtable'}).find_all('li')
    for d in districts:
        if d.input.has_attr('value'):
            d_name = d.label.span.text
            val = d.input['value']
            ids[d_name] = str(val)
    return ids


def main():
    district_ids = scrape_district_ids('https://suumo.jp/chintai/tokyo/city/')
    base_url = 'https://suumo.jp/jj/chintai/ichiran/FR301FC001/?ar=030&bs=040&ta=13&cb=0.0&ct=9999999&et=9999999&cn=9999999&mb=0&mt=9999999&shkr1=03&shkr2=03&shkr3=03&shkr4=03&fw2='

    id_stack = [list(district_ids.items())[-5]]
    pbar = tqdm(total=len(id_stack), position=1, desc='All progress')
    while id_stack:
        d, Id = id_stack.pop(0)
        try:
            data = scrape_pages(base_url + f'&sc={Id}', d)
            df = pd.DataFrame(data=data)
            df.to_csv(f'data/suumo_{d}.csv')
            pbar.update(1)
        except:
            id_stack.append((d, Id))
        time.sleep(1)


if __name__ == "__main__":
    main()
