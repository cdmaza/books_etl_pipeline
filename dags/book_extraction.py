#created to extract data from amazon, goodreads, and local bookstore datas
#load data to s3 bucket
#tasks : 1) fetch multiple data (extract) 2) validate data to match column format 3) store data in datalake (s3)

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from datetime import datetime
import os
import time
import pandas as pd
import config as config
import boto3

# Set up Chrome options
chrome_options = Options()
chrome_options.add_argument("--headless")  # Ensure GUI is off
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")

# Set up the Chrome driver
service = ChromeService(executable_path=ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=chrome_options)
# cService = webdriver.ChromeService(executable_path=’C:/Users/MyUsername/Downloads/chromedriver-win64/chromedriver.exe’)

bucket_name = 'books_pipeline_bucket'
current_date = current_year_month = datetime.now().strftime("%Y-%m")

def get_amazon_data_books():
    amazon_data = []
    base_url = f"https://www.amazon.com/s?k=non+fiction+books"
    page = 1
    stop_page = 25

    try:           
        while page < stop_page:
            if page == 1:
                url = f"{base_url}"
            else:
                url = f"{base_url}&page={page}&xpid=sHj4aq8iJKbv9&qid=1738618247&ref=sr_pg_{page}"
            
            driver.get(url)
            time.sleep(3)
            html = driver.page_source
            driver.quit()

            soup = BeautifulSoup(html, 'html.parser')

            if not soup:
                print("No books found")
            else:
                # get elements
                boxs = soup.find_all('div', class_='a-section a-spacing-base')

                for box in boxs:
                    
                    title_link = box.find('a', class_='a-link-normal s-line-clamp-4 s-link-style a-text-normal').find('h2',class_='a-size-base-plus a-spacing-none a-color-base a-text-normal').find('span')
                    if title_link:
                        title_text = title_link.get_text()
                    else:
                        title_text = 'N/A'

                    price_span = box.find('a', class_='a-link-normal s-no-hover s-underline-text s-underline-link-text s-link-style a-text-normal').find('span',class_='a-price').find('span', class_='a-offscreen')
                    if price_span:
                        if len(price_span) > 1:
                            if int(price_span[0].get_text()) < int(price_span[1].get_text()):
                                price_text = price_span[0].get_text()
                                discount_text = price_span[0].get_text()
                        else:
                            price_text = price_span.get_text()
                            discount_text = 'N/A'
                    else:
                        price_text = 'N/A'
                        discount_text = 'N/A'

                    author_link = box.find('div', class_='a-row a-size-base a-color-secondary').find('span', class_='a-size-base a-link-normal s-underline-text s-underline-link-text s-link-style')
                    if author_link:                                                                                   
                        author_text = author_link.get_text()
                    else:
                        author_text = 'N/A'

                    rating_link = box.find('div', class_='a-row a-size-small').find('a', class_='a-popover-trigger a-declarative').find('i', class_='a-icon a-icon-star-small a-star-small-4-5').find('span')
                    if author_link:                                                                                   
                        rating_text = rating_link.get_text()
                    else:
                        rating_text = 'N/A'
                    
                    review_link = box.find('div', class_='a-row a-size-small').find('div', class_='s-csa-instrumentation-wrapper alf-search-csa-instrumentation-wrapper').find('a', class_='a-link-normal s-underline-text s-underline-link-text s-link-style').find('span')
                    if review_link:                                                                                   
                        review_text = review_link.get_text()
                    else:
                        review_text = 'N/A'
                    
                    amazon_data.append({
                        "title": title_text, 
                        "price": price_text, 
                        "discount":discount_text, 
                        "author": author_text, 
                        "rating": rating_text,
                        "review": review_text, 
                        "source": "Amazon", 
                        "time_extracted": current_date
                        })
        
            page += 1

        amazon_df = pd.DataFrame(amazon_data)
        
        return amazon_df
    except FileNotFoundError as e:
        print(e)
    except Exception as e:
        print(f"An error occurred: {e}")    


def get_goodrewads_data_books():
    goodreads_data = []
    #in-progress


def get_csv():
    folder_path = 'c:/Users/User/Desktop/Project/data_project/book_analysis_pipeline/dags/data'

    try:
        files = os.listdir(folder_path)

        if not files:
            print("No files found in the folder.")
            return None

        #only look for csv that match the current year and month
        csv_files = [file for file in files if file.endswith('.csv')and current_year_month in file]
        
        if not csv_files:
            print("No CSV files found in the folder.")
            return None
        
        csv_data = pd.read_csv(csv_files)
        
        return csv_data
    
    except FileNotFoundError as e:
        print(e)
    except Exception as e:
        print(f"An error occurred: {e}") 

def validate_data(amazon_data, goodreads_data, csv_data):
    
    try:
        # Check if the DataFrame has the expected columns
        expected_columns = ["title", "author", "price"]
        if not all(col in amazon_data.columns for col in expected_columns):
            raise ValueError("Amazon data does not contain the expected columns")
        if not all(col in goodreads_data.columns for col in expected_columns):
            raise ValueError("Goodreads data does not contain the expected columns")
        if not all(col in csv_data.columns for col in expected_columns):
            raise ValueError("CSV data does not contain the expected columns")
    
    except FileNotFoundError as e:
        print(e)
    except Exception as e:
        print(f"An error occurred: {e}")

def load_to_s3(amazon_df, goodread_df, csv_df):

    try:
        iterate = 1
        datas = [amazon_df, goodread_df, csv_df]

        for data in datas:
            if iterate == 1:
                name = 'amazon'
            elif iterate == 2:
                name = 'goodreads'
            else:
                name = 'local_csv'

            s3 = boto3.client('s3',
                            aws_access_key_id=config.KEY,
                            aws_secret_access_key=config.SECRET)
            csv_buffer = data.to_csv(None).encode()
            s3.put_object(Bucket=bucket_name,
                    Key=f'{current_date}{name}.csv',
                        Body=csv_buffer)
            iterate += 1
            
    except FileNotFoundError as e:
        print(e)
    except Exception as e:
        print(f"An error occurred: {e}")

def run_extract_data():
    amazon_df = get_amazon_data_books()
    goodread_df = get_goodrewads_data_books()
    csv_df = get_csv()
    validate_data(amazon_df, goodread_df, csv_df)
    load_to_s3(amazon_df, goodread_df, csv_df)
