from amazon_asin_scrape.common_utility import CommonUtility
import requests


def upload(file):
    file_upload_url = "https://app.etraky.com/json_upload.php"
    with open(file, "rb") as a_file:
        file_dict = {"image[]": a_file}
        r = requests.post(url=file_upload_url, files=file_dict)
    return r.status_code


if __name__ == '__main__':
    file = './json_response_data/' + str('B08PDLKMZ7') + '_IN.json'
    print(upload(file))
