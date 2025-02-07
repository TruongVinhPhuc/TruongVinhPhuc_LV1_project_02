import requests
import json
import pandas as pd
import time
import re
from concurrent.futures import ThreadPoolExecutor
import threading
import queue

sub_category_queue = queue.Queue(0)
category_queue = queue.Queue(0)
lock = threading.Lock()
thread_local = threading.local()


def get_session_for_thread():
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()

    return thread_local.session

def get_category_id_from_homepage():
    url = "https://api.tiki.vn/raiden/v2/menu-config?platform=desktop"
    headers = {'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'}
    response = requests.get(url, headers=headers)
    data = response.json()
    category_id = []
    category = data['menu_block']['items']

    for item in category:
        link = item['link'].split('/')
        category_id.append(link[-1][1:])

    return category_id

def get_sub_category_id():
    stack = []
    headers = {'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'}
    session = get_session_for_thread()
    while not category_queue.empty():
        sub_category = category_queue.get()
        print(f"Scraping sub_category {sub_category}")        
        stack.append(sub_category)
        while len(stack):
            category_id = stack.pop()
            # print(category_id)
            url = f"https://api.tiki.vn/v2/categories?include=children&parent_id={category_id}"
            response = session.get(url,headers=headers)
            try:
                api_data = response.json()
                data = api_data['data']
                for item in data:
                    if not item['is_leaf']:
                        stack.append(item['id'])
                    else:
                        sub_category_queue.put({
                            'id': item['id'],
                            'url_key': item['url_key']
                        })
            except Exception as e:
                print("Error!")

def get_product_id_from_sub_category():
    product_id = pd.DataFrame(columns=['id'])
    headers = {'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'}
    session = get_session_for_thread()
    while not sub_category_queue.empty():     
        try:
            item = sub_category_queue.get()
            id = item['id']
            url_key = item['url_key']
            url = f"https://api.tiki.vn/personalish/v1/blocks/listings?category={id}&urlKey={url_key}"
            response = session.get(url,headers=headers)
            api_data = response.json()
            last_page = api_data['paging']['last_page']
            for i in range(last_page):
                scrape_url = f"https://api.tiki.vn/personalish/v1/blocks/listings?category={id}&page={i + 1}&urlKey={url_key}"
                product_response = session.get(scrape_url, headers=headers)
                product_data = product_response.json()
                for product in product_data['data']:
                    print(product['id'])
                    product_id._append({'id': product['id']}, ignore_index=True)
        except Exception as e:
            print(e)

    try:
        global df
        with lock:
            df = pd.concat([df, product_id], ignore_index=True)
        print(len(df))
    except Exception as e:
        print(e)

def thread_get_sub_category_id():
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(get_sub_category_id) for _ in range(20)]

    print(f"Queue size after sub-category scraping: {sub_category_queue.qsize()}")

def thread_get_product_id():
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(get_product_id_from_sub_category) for _ in range(20)]

def main():
    category = get_category_id_from_homepage()

    for item in category:
        category_queue.put(item.strip())

    columns = ["id"]
    global df
    df = pd.DataFrame(columns=columns)

    thread_get_sub_category_id()

    thread_get_product_id()

    df.to_csv("/home/phuc/Desktop/Project/Project_2/product_id.csv", index=False)


if __name__ == "__main__":
    main()
