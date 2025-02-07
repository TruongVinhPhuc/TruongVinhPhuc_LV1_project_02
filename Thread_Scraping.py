import requests
import json
import pandas as pd
import time
from bs4 import BeautifulSoup
import re
from concurrent.futures import ThreadPoolExecutor
import threading
import queue
import logging

thread_local = threading.local()
lock = threading.Lock()
q = queue.Queue(0)

logging.basicConfig(
    filename='/home/phuc/Desktop/Project/Project_2/scrape.log',
    encoding='utf-8',
    filemode='a',
    format='{asctime} - {levelname} - {message}',
    style='{',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.INFO
)


def get_session_for_thread():
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()

    return thread_local.session


def scrape_api():
    global total_product_scraped
    while not q.empty():
        try:
            product_number = q.get()

            url = f"https://api.tiki.vn/product-detail/api/v1/products/{product_number}"
            headers = {'user-agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:134.0) Gecko/20100101 Firefox/134.0'}

            session = get_session_for_thread()
            response = session.get(url, headers=headers)

            if response.status_code != 200:
                # print(response)
                with lock:
                    global bad_products
                    total_product_scraped += 1
                    bad_products.append({
                        'product_number': product_number,
                        'status_code': response.status_code
                    })

            else:
                api_data = response.json()

                base_url = []
                large_url = []
                medium_url = []
                small_url = []
                thumbnail_url = []
                for i in api_data['images']:
                    base_url.append(i['base_url'])
                    large_url.append(i['large_url'])
                    medium_url.append(i['medium_url'])
                    small_url.append(i['small_url'])
                    thumbnail_url.append(i['thumbnail_url'])


                if api_data['id'] is not None:
                    id = api_data['id']
                else:
                    id = ""

                if api_data['name'] is not None:
                    name = api_data['name']
                else:
                    name = ""

                if api_data['url_key'] is not None:
                    url_key = api_data['url_key']
                else:
                    url_key = ""
                
                if api_data['price'] is not None:
                    price = api_data['price']
                else:
                    price = ""
                
                if api_data['description'] is not None:
                    # Extract and clean the description
                    description_html = api_data['description']
                    soup = BeautifulSoup(description_html, 'html.parser')

                    description_text = soup.get_text(separator='').strip()
                    description_text = '\n'.join(line.strip() for line in description_text.splitlines() if line.strip())
                else:
                    description_text = ""

                product_data = {
                    'id': id,
                    'name': name,
                    'url_key': url_key,
                    'price': price,
                    'description': description_text,
                    'images': {
                        'base_url': base_url,
                        'large_url': large_url,
                        'medium_url': medium_url,
                        'small_url': small_url,
                        'thumbnail_url': thumbnail_url,
                    },
                }

                # print(f"Scraping product {product_number}")

                with lock:
                    global df, file_count
                    total_product_scraped += 1
                    df.append(product_data)

                    if len(df) >= 1000:
                        with open(f'/home/phuc/Desktop/Project/Project_2/product_{file_count}.json', 'w') as fp:
                            json.dump(df, fp)
                        df.clear()
                        file_count += 1

            with lock:
                if total_product_scraped % 1000 == 0:
                    logging.info(f"Scraped {total_product_scraped} products")

        except Exception as e:
            print(f"Error: {e}")

def scrape_all():
    with ThreadPoolExecutor(max_workers=20) as executor:
        for _ in range(20):
            executor.submit(scrape_api)

def read_log():
    last_total_scrape = 0
    last_scraped = 0
    done_scrape = False
    try:
        with open(f'/home/phuc/Desktop/Project/Project_2/scrape.log','r') as log:
            lines = log.readlines()
            if not lines:
                return 0,0,False

            for line in reversed(lines):
                if "done" in line:
                    return 200,200000,True
                 
                match = re.search(r"Scraped (\d+) products", line)
                if match:
                    last_total_scrape = int(match.group(1))
                    last_scraped = int((int(match.group(1)) / 1000)) + 1
                    break

    except FileNotFoundError:
        print("Log file not found, starting from 0 products.")
    
    return last_scraped, last_total_scrape, done_scrape

def main():
    start = time.time()

    global file_count
    global df
    df = []

    global total_product_scraped

    global bad_products
    bad_products = []    

    file_count, total_product_scraped, done_scraped = read_log()
    # print(file_count, total_product_scraped)
    if done_scraped:
        return
    
    file = open("/home/phuc/Desktop/Project/Project_2/products.csv", "r")
    next(file)

    if file_count != 0:
        for _ in range(file_count * 1000):
            next(file)
    
    else:
        file_count = 1

    for line in file:
        q.put(line.strip())

    scrape_all()
    with open(f'/home/phuc/Desktop/Project/Project_2/bad_products.json', 'w') as fp:
        json.dump(bad_products, fp)

    with open(f'/home/phuc/Desktop/Project/Project_2/product_{file_count}.json', 'w') as fp:
        json.dump(df, fp)

    logging.info("Scraping done")

    end = time.time()
    print(f"Time taken: {end - start}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt as k:
        logging.error(f"KeyboardInterrupt")
    except Exception as e:
        logging.error(f"{e}")
