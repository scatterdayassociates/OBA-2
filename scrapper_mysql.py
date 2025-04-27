import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import random

def scrape_data():
    """Scrape data from the NYC procurement website"""
    cookies = {
        'ak_bmsc': '0E08843E4257240434F033A03D6714DD~000000000000000000000000000000~YAAQSJ42F2yUkS+TAQAAc5QEQBk6tyeodmKbK0+6Uz4uJ6d/3sUD6zZEk5sfq+ZaP84RgD+ji4onlCBFYFbPm1iycFE72h894XCCgh0ZiTRMWZZ/1Yg9WVZfeyFNfvkR6mg3zLN9nSOfADBrWbtFfvP1bfyiB5Rzqywa88H1NTeX2qvUsS9B9IAiTiRPav/KfjSEuSYEioe4jl6pG14gNkwyjK6FAKUJkQX64e3Q9RVFC+plwrylg+Sg89pWKkt0ED/H9gTMe7s6ix9IyQuYrB7Mdu2QKXDogTeTQTH7C+deNFiHQwcy2c3Sj7iB5mIP8dIRWi2Z2a9YwKksra6mtcu51Zg+BeqaY4rEwy6tS65hmF7yfTCJpw5nUn54Ev4=',
        '_ga': 'GA1.2.1749921805.1731945974',
        '_gid': 'GA1.2.739001020.1731945974',
        '_gat': '1',
        '_ga_KFWPDKF16D': 'GS1.2.1731945975.1.1.1731947398.0.0.0',
        # Your cookie data here...
    }

    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'max-age=0',
        'content-type': 'application/x-www-form-urlencoded',
        'origin': 'https://a856-cityrecord.nyc.gov',
        'priority': 'u=0, i',
        'referer': 'https://a856-cityrecord.nyc.gov/Section',
        'sec-ch-ua': '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Linux"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
    }
    
    data_data = []
    
    try:
        for i in range(1, 41):
            print(f"Scraping page {i}...")
            data = {
                'SectionId': '6',
                'SectionName': '\r\n                                                \r\n                                                    \r\n                                                \r\n                                                Procurement\r\n                                            ',
                'NoticeTypeId': '0',
                'PageNumber': f'{i}',
            }

            # Add rate limiting with a random delay
            time.sleep(random.uniform(1.0, 3.0))
            
            try:
                response = requests.post(
                    'https://a856-cityrecord.nyc.gov/Section', 
                    cookies=cookies, 
                    headers=headers, 
                    data=data,
                    timeout=30  # Add a timeout
                )
                
                # Check response status
                if response.status_code != 200:
                    print(f"Error: Received status code {response.status_code} on page {i}")
                    continue
                
                soup = BeautifulSoup(response.text, 'html.parser')
                notice_items = soup.find_all('div', class_='notice-container')

                if not notice_items:
                    print(f"Warning: No notice items found on page {i}")
                
                for item in notice_items:
                    try:
                        title_elem = item.find('h1')
                        agency_elem = item.find('strong')
                        award_date_elems = item.find_all('small')
                        category_elem = item.find('i', class_='fa fa-tag')
                        desc_elem = item.find('p', class_='short-description')
                        
                        # Check if all required elements exist
                        if not all([title_elem, agency_elem, award_date_elems, category_elem, desc_elem]):
                            print("Warning: Missing required elements in a notice item")
                            continue
                        
                        title = title_elem.text.strip()
                        agency = agency_elem.text.strip()
                        award_date = award_date_elems[-1].text.strip().split('\n')[-1].strip()
                        category = category_elem.next_sibling.strip()
                        description = desc_elem.text.strip()

                        data_data.append({
                            'Agency': agency,
                            'Title': title,
                            'Award Date': award_date,
                            'Description': description,
                            'Category': category
                        })
                    except Exception as e:
                        print(f"Error processing notice item: {e}")
                        continue
                
            except requests.exceptions.RequestException as e:
                print(f"Request error on page {i}: {e}")
                continue
                
    except Exception as e:
        print(f"Global scraping error: {e}")
    
    print(f"Scraping complete. Found {len(data_data)} records.")
    return pd.DataFrame(data_data)

def scraper(conn):
    print("🟢 Scraper started...")
    try:
        scraped_df = scrape_data()
        if scraped_df.empty:
            print("⚠️ No data scraped. Exiting.")
            return False

        print(f"✅ Scraped {len(scraped_df)} records")

        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS nycproawards4 (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    Agency VARCHAR(255),
                    Title TEXT,
                    `Award Date` DATE,
                    Description TEXT,
                    Category VARCHAR(255)
                )
            """)
            conn.commit()

            for _, row in scraped_df.iterrows():
                try:
                    cursor.execute("SELECT COUNT(*) FROM nycproawards4 WHERE Title=%s", (row['Title'],))
                    exists = cursor.fetchone()[0]

                    if exists:
                        cursor.execute("""
                            UPDATE nycproawards4 
                            SET Agency=%s, `Award Date`=%s, Description=%s, Category=%s 
                            WHERE Title=%s
                        """, (row['Agency'], row['Award Date'], row['Description'], row['Category'], row['Title']))
                    else:
                        cursor.execute("""
                            INSERT INTO nycproawards4 (Agency, Title, `Award Date`, Description, Category) 
                            VALUES (%s, %s, %s, %s, %s)
                        """, (row['Agency'], row['Title'], row['Award Date'], row['Description'], row['Category']))

                except Exception as err:
                    print(f"⚠️ Database error processing record: {err}")
                    continue

            conn.commit()
            print("✅ Scraper data uploaded successfully")

    except Exception as e:
        print(f"❌ Unexpected scraper error: {e}")

    finally:  # ✅ Ensure connection always closes!
        if conn and conn.is_connected():
            conn.close()
            print("🔴 Scraper connection closed")

    return True



