"""
Modules used in the code
"""
import json
import os
import requests
import pandas as pd
import re
from bs4 import BeautifulSoup
from amazon_asin_scrape.get_main_data import Scraper
from amazon_asin_scrape.common_utility import CommonUtility
from amazon_asin_scrape.flipkart_url import FlipkartUrlFinder


class DetailsAsin:
    def __init__(self, product_asin):
        self.url = "https://www.flipkart.com/product/p/itme?pid=" + product_asin
        self.pid = product_asin
        self.iso_code = 'IN'
        self.channel = 'flipkart'
        self.all_images = None
        self.buy_box = list()
        self.buy_box_seller = []
        self.buy_box_seller_price = []
        self.buy_box_seller_rating_count = []
        print(self.url)

    def buy_box_details(self):
        header = {
            'X-user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.129 Safari/537.36 FKUA'
                            '/website/42/website/Desktop'}
        payload = {"requestContext": {"productId": self.pid}}
        buy_box_url = 'https://www.flipkart.com/api/3/page/dynamic/product-sellers'
        params = json.dumps(payload).encode('utf8')
        data = requests.post(buy_box_url, data=params, headers=header)
        data = data.json()['RESPONSE']['data']['product_seller_detail_1']['data']
        for i in data:
            df = pd.DataFrame(i)['value'].dropna()
            try:
                buy_box_seller = df['sellerInfo']['value']['name']
            except KeyError:
                buy_box_seller = 'NA'
            try:
                buy_box_seller_rating_count = df['sellerInfo']['value']['rating']['average']
            except KeyError:
                buy_box_seller_rating_count = 'NA'
            try:
                buy_box_seller_price = df['pricing']['value']['finalPrice']['decimalValue']
            except KeyError:
                buy_box_seller_price = "NA"
            self.buy_box_seller.append(buy_box_seller)
            self.buy_box_seller_price.append(buy_box_seller_price)
            self.buy_box_seller_rating_count.append(buy_box_seller_rating_count)

    @staticmethod
    def find_extra_details(node, kv):
        if isinstance(node, list):
            for i in node:
                for x in DetailsAsin.find_extra_details(i, kv):
                    yield x
        elif isinstance(node, dict):
            if kv in node:
                yield node[kv]
            for j in node.values():
                for x in DetailsAsin.find_extra_details(j, kv):
                    yield x

    def get_data(self):
        """
        Main process of the code
        :return:
        """
        json_data = {}
        #  get html data and session
        try:
            data, session = Scraper(self.url).get_url_data()
        except TypeError:
            return {
                "status": "error",
                "data": [],
                "message": "Page %s was blocked by Amazon. Please try using better proxies\n" % self.url
            }
        soup = BeautifulSoup(data, 'html.parser')
        try:
            title = soup.select_one('.B_NuCI').text.strip()
        except (TypeError, AttributeError):
            title = 'NA'
        try:
            mrp = soup.select_one('._2p6lqe').text
        except (TypeError, AttributeError):
            mrp = 'NA'
        try:
            price = soup.select_one('._16Jk6d').text
        except (TypeError, AttributeError):
            price = 'NA'
        try:
            keywords = soup.find('meta', attrs={'name': 'Keywords'})['content']
        except (TypeError, AttributeError):
            keywords = 'NA'
        try:
            category = [i.text for i in soup.select('._2whKao', limit=2)]
        except (TypeError, AttributeError):
            category = ['NA', 'NA']

        # All seller related details

        try:
            seller = soup.select_one('#sellerName span').text

            seller = "".join(re.findall(r'[a-zA-Z-\s+]*', seller))
        except (TypeError, AttributeError, IndexError):
            seller = 'NA'

        #  Buy Box Details
        self.buy_box_details()

        b = []
        if self.buy_box_seller:
            for b_seller in self.buy_box_seller:
                if b_seller.__contains__(seller):
                    b.append(1)
                else:
                    b.append(0)
            self.buy_box = b
        else:
            self.buy_box = 0

        # Bullets Point
        try:
            features = [i.text for i in soup.select('._21Ahn-')]
        except (TypeError, AttributeError):
            features = 'NA'

        try:
            product_desc = soup.select_one('.RmoJUa').text.replace("\n", " ")
        except (TypeError, AttributeError):
            product_desc = 'NA'

        try:
            images = [i['style'].replace("background-image:url(", "").replace(")", "") for i in
                      soup.select_one('._2FHWw4').find_all('div', class_='q6DClP')]
            self.all_images = images
        except (TypeError, AttributeError):
            self.all_images = 'NA'
        try:
            brand = json.loads("".join(re.findall(r'\[.*]', str(soup.find('script', id='jsonLD')))))
            brand = brand[0]['brand']['name']
        except (KeyError, AttributeError, json.decoder.JSONDecodeError):
            brand = 'NA'

        try:
            extra_details = json.loads("".join(re.findall(r'\{.*}', str(soup.find('script', id='is_script')))))[
                'pageDataV4']
        except (KeyError, AttributeError, json.decoder.JSONDecodeError):
            extra_details = 'NA'

        #  call function for to find key in nested list

        reviews_data = list(DetailsAsin.find_extra_details(extra_details, 'reviewData'))
        reviews_text = list(DetailsAsin.find_extra_details(reviews_data, 'text'))
        reviews_heading = list(DetailsAsin.find_extra_details(reviews_data, 'title'))
        reviews_date = [CommonUtility.another_parse_date(i) for i in
                        list(DetailsAsin.find_extra_details(reviews_data, 'created'))]
        reviews_rating = list(DetailsAsin.find_extra_details(reviews_data, 'rating'))
        status = list(DetailsAsin.find_extra_details(extra_details, 'productStatus'))[0]

        bsr = list(DetailsAsin.find_extra_details(extra_details, 'attributes'))
        key = list(DetailsAsin.find_extra_details(bsr, 'name'))
        values = list(DetailsAsin.find_extra_details(bsr, 'values'))

        tmp = {}
        for i, j in zip(key, values):
            tmp[i] = "".join(j)

        reviews_count = len(reviews_text)
        reviews_len = [len(t) for t in reviews_text]

        rating_reviews_data = list(DetailsAsin.find_extra_details(extra_details, 'ratingsAndReviews'))

        rating = list(DetailsAsin.find_extra_details(rating_reviews_data, 'count'))
        customer_reviews = list(DetailsAsin.find_extra_details(rating_reviews_data, 'average'))

        #  Word Cloud Generation processing
        positive, negative, neutral = CommonUtility.separate_reviews(reviews_rating, reviews_text)

        positive_word_cloud, pos_server_file_path = CommonUtility.generate_word_cloud(positive, 'pos', self.pid)
        negative_word_cloud, neg_server_file_path = CommonUtility.generate_word_cloud(negative, 'neg', self.pid)
        neutral_word_cloud, neu_server_file_path = CommonUtility.generate_word_cloud(neutral, 'neu', self.pid)

        #  Upload word cloud from local machine to aws server
        CommonUtility.upload_to_aws(positive_word_cloud, CommonUtility.BUCKET_NAME, self.pid + '_pos' + '.png')
        CommonUtility.upload_to_aws(negative_word_cloud, CommonUtility.BUCKET_NAME, self.pid + '_neg' + '.png')
        CommonUtility.upload_to_aws(neutral_word_cloud, CommonUtility.BUCKET_NAME, self.pid + '_neu' + '.png')

        # remove the word cloud from the local machine

        os.remove(positive_word_cloud)
        os.remove(negative_word_cloud)
        os.remove(neutral_word_cloud)
        print("Clean up activity done !!!!")

        #  make json data
        json_response = []
        json_data['channel_sku'] = self.pid
        json_data['product_url'] = self.url
        json_data['title'] = title
        json_data['mrp'] = mrp.replace("₹", "").replace(",", "")
        json_data['price'] = price.replace("₹", "").replace(",", "")
        json_data['status'] = status
        json_data['seller'] = seller
        json_data['category'] = category[0]
        json_data['sub_category'] = category[1]
        json_data['bullets_point'] = features
        json_data['brand'] = brand
        json_data['reviews_heading'] = reviews_heading
        json_data['rating'] = rating
        json_data['reviews_date'] = reviews_date
        json_data['customer_reviews'] = customer_reviews
        json_data['keywords'] = keywords
        json_data['reviews_len'] = reviews_len
        json_data['reviews_rating'] = reviews_rating
        json_data['reviews_count'] = reviews_count
        json_data['reviews_text'] = reviews_text
        json_data['images'] = self.all_images
        json_data['product_description'] = product_desc.replace("\n", "")
        json_data['buy_box'] = self.buy_box
        json_data['buy_box_seller'] = self.buy_box_seller
        json_data['buy_box_seller_rating_count'] = self.buy_box_seller_rating_count
        json_data['buy_box_seller_price'] = self.buy_box_seller_price
        json_data['positive_wordcloud_url'] = pos_server_file_path
        json_data['negative_wordcloud_url'] = neg_server_file_path
        json_data['neutral_wordcloud_url'] = neu_server_file_path
        json_data['extra_details'] = tmp
        json_response.append(json_data)
        iso_dict = {"iso_code": self.iso_code, "channel": self.channel, "asin": self.pid, "data": json_response}
        print(iso_dict)
        file = './json_response_data/' + self.pid + '_IN.json'
        with open(file, 'w', encoding='utf-8') as json_file:
            json_file.write(json.dumps(iso_dict, indent=4))

        CommonUtility.json_file_upload(file)
        os.remove(file)
        return {
            "status": "True",
            "data": json_response,
            "message": "Success"
        }


def main():
    url = "https://www.flipkart.com/search?q={}&page={}"
    for i in FlipkartUrlFinder(url).process_url():
        for j in i[0]:
            print(DetailsAsin(j).get_data())


def main1(url):
    print(DetailsAsin(url).get_data())


if __name__ == '__main__':
    # main()
    main1('TDSFVA4RWZWWZWJM')
