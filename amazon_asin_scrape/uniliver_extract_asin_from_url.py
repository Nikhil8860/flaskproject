from bs4 import BeautifulSoup
import requests
import json
from time import sleep


class Extractor:
    def __init__(self, store_url):
        self.url = store_url
        self.base_url = 'https://www.amazon.in'

    def find_urls(self):
        with open('uniliver_brand.json', 'r') as json_file:
            file = json.load(json_file)

        with requests.session() as s:
            s.headers[
                "user-agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.146 Safari/537.36"
            s.headers[
                "accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9"
            s.headers['accept-encoding'] = "gzip, deflate, br"
            s.headers['accept-language'] = "en-US,en;q=0.9"
            s.headers['cache-control'] = "max-age=0"
            for i in file.keys():
                for p in file[i]:
                    data = s.get(self.url.format(p, 1, 1), headers=s.headers)
                    sleep(1)
                    soup = BeautifulSoup(data.text, 'html.parser')
                    try:
                        page_number = soup.find('ul', class_='a-pagination')
                        page_number = int(page_number.text.splitlines()[-2])

                        for i in range(1, page_number + 1):
                            print(self.url.format(p, i, i))
                            urls = self.url.format(p, i, i)
                            data = s.get(urls, headers=s.headers)
                            sleep(4)
                            soup = BeautifulSoup(data.text, 'html.parser')
                            asin_list = [i['href'].split("/dp/")[1].split('/')[0] for i in
                                         soup.find_all('a', class_='a-link-normal a-text-normal', href=True) if
                                         '/dp/' in i['href']]
                            yield asin_list, s
                    except Exception as e:
                        asin_list = [i['href'].split("/dp/")[1].split('/')[0] for i in
                                     soup.find_all('a', class_='a-link-normal a-text-normal', href=True) if
                                     '/dp/' in i['href']]
                        yield asin_list, s


if __name__ == '__main__':
    url = "https://www.amazon.in/s?k={}&page={}&ref=sr_pg_{}"
    Extractor(url).find_urls()
