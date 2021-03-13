from bs4 import BeautifulSoup
import pymysql.cursors
import requests
import re
from time import sleep


class Extractor:
    def __init__(self):
        self.base_url = 'https://www.amazon.com'

    def connect(self):
        # self.conn = pymysql.connect('localhost', 'root', '', 'amazon')
        self.conn = pymysql.connect('13.233.239.105', 'root', 'evanik@2019', 'appetraky')

    def query(self, sql):
        self.connect()
        try:
            cursor = self.conn.cursor()
            try:
                cursor.execute(sql)
            except Exception as e:
                print(e)
            if 'select' in sql:
                return cursor.fetchall()
            else:
                self.conn.commit()
                print(cursor.rowcount, "record(s) affected")
        except (AttributeError, pymysql.err.InterfaceError, pymysql.OperationalError, pymysql.err.OperationalError):
            self.connect()
            cursor = self.conn.cursor()
            try:
                cursor.execute(sql)
            except Exception as e:
                print(e)
            if 'select' in sql:
                return cursor.fetchall()
            else:
                self.conn.commit()
                print(cursor.rowcount, "record(s) affected Success")
        finally:
            cursor.close()
            self.conn.close()
        return cursor

    def get_store_url(self):
        query = "select user_id, iso_code, amazon_link from amazon_stores where amazon_link LIKE '%stores%' "
        data = self.query(query)
        return data

    def find_urls(self):
        with requests.session() as s:
            s.headers[
                "user-agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.146 Safari/537.36"
            s.headers[
                "accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9"
            s.headers['accept-encoding'] = "gzip, deflate, br"
            s.headers['accept-language'] = "en-US,en;q=0.9"
            s.headers['cache-control'] = "max-age=0"

            for i in self.get_store_url():
                prod_link = []
                user_id = i[0]
                iso_code = i[1]
                url = i[2]
                data = s.get(url, headers=s.headers)

                soup = BeautifulSoup(data.text, 'html.parser')
                link = [i.find(href=True) for i in soup.find_all('li', class_='style__overlay__SVhoV')]
                if len(link) == 0:
                    link = [i.find(href=True) for i in soup.find_all('li', class_='style__hasChildren__okeZi')]
                    if len(link) == 0:
                        link = [i.find(href=True) for i in soup.find_all('li', class_='style__isHeading__2UmI2')]
                for i in link:
                    if 'href' in str(i):
                        product_url = self.base_url + i['href']
                        prod_link.append(product_url)
                yield prod_link, s, user_id, iso_code

    def find_all_asin(self):
        asin_all_list = []
        data = self.find_urls()
        for links in data:
            session = links[1]
            user_id = links[2]
            iso_code = links[3]
            for j in links[0]:
                session.headers['sec-fetch-dest'] = "document"
                session.headers['sec-fetch-mode'] = "navigate"
                session.headers['sec-fetch-site'] = "none"
                session.headers['sec-fetch-user'] = "?1"
                # print(j)
                data = session.get(j, headers=session.headers)
                soup = BeautifulSoup(data.text, 'html.parser')
                # asin = [i for i in soup.find_all('a', class_='style__overlay__SVhoV', href=True)]
                # print(asin)
                asin = [i.find('script') for i in soup.find_all('div', class_='a-row stores-row stores-widget-atf')]
                asin_list = re.findall("ASINList(.*)includeOutOfStock", str(asin))
                if not asin_list:
                    continue
                asin_list = asin_list[0].replace('":', '').replace('],"', '').replace("[", '').replace('"', '').split(
                    ",")
                asin_all_list.extend(asin_list)
                print(asin_list)
                yield asin_list, user_id, iso_code


if __name__ == '__main__':
    """
    Function Execution Start here
    """
    obj = Extractor()
    obj.find_all_asin()
    # obj.get_store_url()
