import requests
from bs4 import BeautifulSoup
import json


class FlipkartUrlFinder:
    def __init__(self, store_url):
        self.url = store_url
        self.base_url = 'https://www.flipkart.com'

    def process_url(self):
        with open('uniliver_brand.json', 'r') as json_file:
            file = json.load(json_file)

        with requests.session() as s:
            s.headers[
                'User-Agent'] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.146 Safari/537.36"
            s.headers[
                "Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9"
            s.headers['Accept-Encoding'] = "gzip, deflate, br"
            s.headers['Accept-Language'] = "en-US,en;q=0.9"
            s.headers['Host'] = "www.flipkart.com"
            s.headers['Sec-Fetch-Dest'] = "document"
            s.headers['Sec-Fetch-Mode'] = "navigate"
            for i in file.keys():
                for p in file[i]:
                    data = s.get(self.url.format(p, 1), headers=s.headers)
                    soup = BeautifulSoup(data.text, 'html.parser')
                    page_number = soup.find('span', class_='_10Ermr')
                    try:
                        page_number = (int(page_number.text.split('of')[1].split()[0].replace(",", ""))) // 40 + 1
                    except AttributeError:
                        continue
                    for i in range(1, page_number + 1):
                        print(self.url.format(p, i))
                        data = requests.get(self.url.format(p, i), headers=s.headers)
                        soup = BeautifulSoup(data.text, 'html.parser')
                        link = [
                            i['href'].split("pid=")[1].split('&')[0] for i in
                            soup.find_all('a', class_='s1Q9rs', href=True)]
                        if link:
                            print(link)
                            yield link, s


if __name__ == '__main__':
    with open('uniliver_brand.json', 'r') as json_file:
        file = json.load(json_file)
    url = "https://www.flipkart.com/search?q={}&page={}"
    FlipkartUrlFinder(url).process_url()
