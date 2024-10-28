import requests
import gspread
from google.oauth2.service_account import Credentials
from bs4 import BeautifulSoup
import re
import time

def dump(restaurant_url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3',
        'Cookie': 'PHPSESSID=74d74a0c236a0bfb925f965324e29f9b'
    }

    # Send GET request to the restaurant URL
    response = requests.get(restaurant_url, headers=headers)

    # Parse the HTML content
    soup = BeautifulSoup(response.content, 'html.parser')

    # Example 1: Look for restaurant ID in meta tags or script content

    script_tag = soup.find('script', string=re.compile("resId"))

    # cleaned_str = script_tag.replace('\\', '')

    # Regular expression to capture the dynamic res_id value
    html_response = script_tag.string
    cleaned_content = html_response.replace('\\', '')
    match = re.search(r'"resId":(\d+)', cleaned_content)
    res_id = match.group(1)
    '''if match:
        res_id = match.group(1)
        return res_id
    else:
        print("res_id not found.")
    res_id = resid()'''
    page_url = f"https://www.zomato.com/webroutes/reviews/loadMore?sort=dd&filter=reviews-dd&res_id={res_id}&page=1"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3',
        'Cookie': 'PHPSESSID=74d74a0c236a0bfb925f965324e29f9b'
    }
    response = requests.get(f"{page_url}", headers=headers)
    out = (response.json())
    no_of_pages = out['page_data']['sections']['SECTION_REVIEWS']['numberOfPages']
    final_reviews=[]

    def fetch_reviews(page_url):
        extracted_info = []
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3',
            'Cookie': 'PHPSESSID=74d74a0c236a0bfb925f965324e29f9b'
        }
        response = requests.get(f"{page_url}", headers=headers)
        out=(response.json())
        reviews=out['entities']['REVIEWS']
        #print(out['REVIEWS']['userName'])
        # Iterate through the dictionary and get required information
        for key, review in reviews.items():
            user_info = {'reviewID':review.get('reviewId'),'userName': review.get('userName'),'timestamp': review.get('timestamp'),'reviewText': review.get('reviewText'),'rating':review.get('ratingV2'),'dining/delivery type':review.get('experience')}
            extracted_info.append(user_info)
        return extracted_info
    for i in range(1,no_of_pages):
        page_url = f"https://www.zomato.com/webroutes/reviews/loadMore?sort=dd&filter=reviews-dd&res_id=20420088&page={i}"
            #reviews = fetch_reviews(restaurant_url)
        final_reviews.append(fetch_reviews(page_url))
    #print(final_reviews)
    reviews = [review for sublist in final_reviews for review in sublist]# Flatten the nested list into a single list of dictionaries
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file("credentials.json", scopes=scopes)
    client = gspread.authorize(creds)

    # Open the spreadsheet by its ID
    sheet_id = "1qS_5S0zephNVV2FcvwdFlRRDiMhpOGsGhfyzym-Qg_U"
    workbook = client.open_by_key(sheet_id)

    # Select the first sheet in the workbook
    sheet = workbook.sheet1
    sheet_name = f"{res_id}"
    sheet.update_title(sheet_name)


    # Create the headers in the first row
    headers = ['reviewID', 'userName', 'timestamp', 'reviewText', 'rating', 'dining/delivery type']
    sheet.insert_row(headers, 1)

    # Function to chunk the data into smaller pieces
    def chunk_data(data, chunk_size=500):
        for i in range(0, len(data), chunk_size):
            yield data[i:i + chunk_size]

    # Prepare the data as rows
    rows = [
        [
            review.get('reviewID', ''),
            review.get('userName', ''),
            review.get('timestamp', ''),
            review.get('reviewText', ''),
            review.get('rating', ''),
            review.get('dining/delivery type', '')
        ]
        for review in reviews
    ]

    # Batch insert the data in chunks with delay to avoid exceeding quota
    for chunk in chunk_data(rows):
        try:
            sheet.append_rows(chunk, value_input_option='RAW')
            time.sleep(60)  # Sleep for 60 seconds between batches to avoid quota limit
        except gspread.exceptions.APIError as e:
            print(f"APIError occurred: {e}")
            time.sleep(120)  # Wait 2 minutes before retrying after error

    header = sheet.row_values(1)

    # Get the first review (second row)
    first_review = sheet.row_values(2)

    # Find the indices of 'reviewID' and 'timestamp'
    review_id_index = header.index('reviewID')
    timestamp_index = header.index('timestamp')

    # Get the reviewID and timestamp from the first review
    review_id = first_review[review_id_index]
    timestamp = first_review[timestamp_index]

    print(f"LatestReviewID: {review_id}")
    print(f"latestReviewDate: {timestamp}")

if __name__ == "__main__":
    #restaurant_url = "https://www.zomato.com/webroutes/getPage?page_url=/chennai/secret-story-nungambakkam/reviews"# Replace with desired restaurant URL
    restaurant_url="https://www.zomato.com/chennai/secret-story-nungambakkam"
    dump(restaurant_url)




