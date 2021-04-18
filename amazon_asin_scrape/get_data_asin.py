"""
Modules used in the code
"""
import json
import os
import re
from time import sleep
from bs4 import BeautifulSoup
from amazon_asin_scrape.get_main_data import Scraper

from amazon_asin_scrape.common_utility import CommonUtility
from amazon_asin_scrape.uniliver_extract_asin_from_url import Extractor


class DetailsAsin:
    def __init__(self, product_asin):
        self.url = "https://www.amazon.in/dp/" + product_asin
        self.file_upload_url = "http://app.etraky.com/image_upload.php"
        self.iso_code = 'IN'
        self.channel = 'amazon'
        self.asin = product_asin
        self.all_images = None
        self.buy_box = list()
        self.buy_box_seller = None
        self.buy_box_seller_price = None
        self.buy_box_seller_rating_count = None

    def get_data(self):
        """
        Main process of the code
        :return:
        """
        json_data = {}

        channel_dict = {}
        #  get html data and session
        try:
            data, _ = Scraper(self.url).get_url_data()
        except TypeError:
            return {
                "status": "error",
                "data": [],
                "message": "Page %s was blocked by Amazon. Please try using better proxies\n" % self.url
            }
        soup = BeautifulSoup(data, 'html.parser')
        try:
            title = soup.find('span', id='productTitle').text.strip()
        except (TypeError, AttributeError):
            title = 'NA'
        try:
            mrp = soup.select_one('.priceBlockStrikePriceString').text
        except (TypeError, AttributeError):
            mrp = 'NA'
        try:
            price = soup.select_one('#priceblock_ourprice').text
        except (TypeError, AttributeError):
            try:
                price = soup.select_one('#priceblock_dealprice').text
            except (TypeError, AttributeError):
                price = 'NA'
        if price == 'NA':
            try:
                price = soup.select_one('#priceblock_saleprice').text
            except (TypeError, AttributeError):
                price = 'NA'
        try:
            status = soup.select_one('#availability').text.strip()
        except (TypeError, AttributeError):
            status = 'NA'
        try:
            keywords = soup.find('meta', attrs={'name': 'keywords'})['content']
        except (TypeError, AttributeError):
            keywords = 'NA'

        try:
            category = [i.strip() for i in soup.select_one('#wayfinding-breadcrumbs_container').text.splitlines() if
                        len(i.strip()) > 2]

        except (TypeError, AttributeError):
            category = ['NA', 'NA']
        # All seller related details
        for __ in range(10):
            # seller_url = 'https://www.amazon.in/gp/offer-listing/' + self.asin
            seller_url = 'https://www.amazon.in/gp/aod/ajax/ref=olp_aod_redir?qty=1&asin=' + self.asin + '&pc=dp'
            try:
                data, _ = Scraper(seller_url).get_url_data()
            except (TypeError, AttributeError):
                return {
                    "status": "error",
                    "data": [],
                    "message": "Page %s was blocked by Amazon. Please try using better proxies\n" % self.url
                }
            seller_soup = BeautifulSoup(data, 'html.parser')

            self.buy_box_seller = [slr.text.replace('\n', '') for slr in
                                   seller_soup.select('#aod-offer-soldBy .a-link-normal')][1:]

            self.buy_box_seller_rating_count = ["".join(re.findall(r'\(\d+', rating.text)).replace("(", "") for rating
                                                in seller_soup.select('#aod-offer-seller-rating')][1:]

            self.buy_box_seller_price = [
                                            price.text.replace('₹', '').replace(',', '').replace("\n", "").strip() for
                                            price in
                                            seller_soup.select('.a-offscreen')][1:]

            print(self.buy_box_seller, __, sep='--')
            print(self.buy_box_seller_price)
            print(self.buy_box_seller_rating_count)
            if self.buy_box_seller_price or self.buy_box_seller or self.buy_box_seller_rating_count:
                break
        sleep(1)
        try:
            seller = soup.select_one('#sellerProfileTriggerId').text.strip()
        except (TypeError, AttributeError):
            seller = 'NA'
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
        try:
            features = [bullets for bullets in soup.select_one('#feature-bullets').text.strip().splitlines() if
                        bullets and bullets != '›' and bullets != 'See more product details']
        except (TypeError, AttributeError):
            features = 'NA'

        try:
            product_desc = soup.select_one('#productDescription').text
        except (TypeError, AttributeError):
            product_desc = 'NA'
        try:
            images = soup.find_all('script', type='text/javascript')
            for img in images:
                if str(img).__contains__('ImageBlockATF'):
                    try:
                        data = re.findall(r'(\[.*])', str(img))[0]
                        data = json.loads(str(data))
                        self.all_images = data
                    except (IndexError, TypeError, json.decoder.JSONDecodeError):
                        self.all_images = 'NA'
        except (TypeError, AttributeError):
            self.all_images = 'NA'
        try:
            extra_details = {}
            bsr = [i.strip() for i in soup.select_one('#detailBulletsWrapper_feature_div').text.splitlines() if i]
            if 'Product details' in bsr[0]:
                bsr = bsr[1:]
            for i, j in zip(range(0, len(bsr) - 3, 3), range(2, len(bsr) - 1, 3)):
                extra_details[bsr[i]] = bsr[j]
            bsr = extra_details
        except (TypeError, AttributeError):
            bsr = 'NA'
        if bsr == 'NA' or not bsr:
            try:
                extra_details = {}
                key = [i.text for i in soup.select('.prodDetSectionEntry')]
                values = [i.text for i in soup.select('#prodDetails td')]
                for i, j in zip(key, values):
                    extra_details[i.replace("\n", '')] = j.replace("\n", '')
                bsr = extra_details
            except (TypeError, AttributeError):
                bsr = 'NA'
        # Reviews Related Details
        reviews_url = "https://www.amazon.in/product-reviews/" + self.asin
        try:
            reviews_data, session = Scraper(reviews_url).get_url_data()
        except (TypeError, AttributeError):
            return {
                "status": "error",
                "data": [],
                "message": "Page %s was blocked by Amazon. Please try using better proxies\n" % self.url
            }
        reviews_soup = BeautifulSoup(reviews_data, 'html.parser')
        try:
            rating = reviews_soup.select_one('#cm_cr-product_info .a-color-secondary').text.split()[0]
        except (TypeError, AttributeError):
            rating = 'NA'
        brand = reviews_soup.select_one('#cr-arp-byline .a-link-normal').text
        reviews_heading = [r.text for r in reviews_soup.select('.a-text-bold span')]
        reviews_text = [t.text.replace('\n', '') for t in reviews_soup.select('.review-text-content span')]
        reviews_date = [CommonUtility.date_parse(d.text, 'IN') for d in
                        reviews_soup.select('#cm_cr-review_list .review-date')]
        reviews_count = len(reviews_text)
        reviews_len = [len(t) for t in reviews_text]
        reviews_rating = [float(r.text.split()[0]) for r in reviews_soup.select('#cm_cr-review_list .review-rating')]
        try:
            customer_reviews = reviews_soup.select_one('#cm_cr-product_info .a-color-base').text
            customer_reviews = customer_reviews.split()[0]
        except (TypeError, AttributeError):
            customer_reviews = 'NA'
        #  Word Cloud Generation processing
        positive, negative, neutral = CommonUtility.separate_reviews(reviews_rating, reviews_text)

        positive_word_cloud, pos_server_file_path = CommonUtility.generate_word_cloud(positive, 'pos', self.asin)
        negative_word_cloud, neg_server_file_path = CommonUtility.generate_word_cloud(negative, 'neg', self.asin)
        neutral_word_cloud, neu_server_file_path = CommonUtility.generate_word_cloud(neutral, 'neu', self.asin)

        #  Upload word cloud from local machine to aws server
        CommonUtility.upload_to_aws(positive_word_cloud, CommonUtility.BUCKET_NAME, self.asin + '_pos' + '.png')
        CommonUtility.upload_to_aws(negative_word_cloud, CommonUtility.BUCKET_NAME, self.asin + '_neg' + '.png')
        CommonUtility.upload_to_aws(neutral_word_cloud, CommonUtility.BUCKET_NAME, self.asin + '_neu' + '.png')

        # remove the word cloud from the local machine

        os.remove(positive_word_cloud)
        os.remove(negative_word_cloud)
        os.remove(neutral_word_cloud)
        print("Clean up activity done !!!!")

        #  make json data
        json_response = []
        json_data['channel_sku'] = self.asin
        json_data['product_url'] = self.url
        json_data['title'] = title
        json_data['mrp'] = mrp.replace("₹\xa0", "").replace(",", "")
        json_data['price'] = price.replace("₹\xa0", "").replace(",", "")
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
        json_data['extra_details'] = bsr
        json_response.append(json_data)
        iso_dict = {"iso_code": self.iso_code, "channel": self.channel, "asin": self.asin, "data": json_response}
        print(iso_dict)

        file = './json_response_data/' + self.asin + '_IN.json'
        with open(file, 'w', encoding='utf-8') as json_file:
            json_file.write(json.dumps(iso_dict, indent=4))

        CommonUtility.json_file_upload(file)
        os.remove(file)
        print(json_response)
        print("Json File remove!!")

        return {
            "status": "True",
            "data": json_response,
            "message": "Success"
        }


def main():
    import requests
    url = "https://www.amazon.in/s?k={}&page={}&ref=sr_pg_{}"
    for i in Extractor(url).find_urls():
        for j in i[0]:
            print(j)
            data = requests.get("http://flask.evanik.com/amazon/api/in/asin-data/" + j)
            file = './json_response_data/' + j + '_IN.json'
            with open(file, 'w', encoding='utf-8') as json_file:
                json_file.write(json.dumps(data.json(), indent=4))

            CommonUtility.json_file_upload(file)
            os.remove(file)
            print(CommonUtility.insert_record(data.json()))
            # print(DetailsAsin(j).get_data())


def main1():
    return DetailsAsin('B07X2L5Z8C').get_data()


if __name__ == '__main__':
    main1()
    # print(main1())
# quit()
# if __name__ == '__main__':
#     a = ['B079WZNJ9Y', 'B07X2YGSTW', 'B07W3SN1L8', 'B081S7GDKL', 'B01LX3B5OE', 'B01LZNNG71', 'B01EYOCOJC', 'B0156LU3MM',
#          'B01G6OC4O8', 'B08575G4WR', 'B0816VNKV6', 'B00VX26UR6', 'B075LRXKVJ', 'B07GXKTPGZ', 'B07VTPR56N', 'B07W6G7B1D',
#          'B07G39K4FC', 'B08H5G5Q1N', 'B0778QR9HN', 'B07LFZDQ7R', 'B07KQXXMXR', 'B089FJN8YH', 'B07LG3BMTQ', 'B07KWZPDY6',
#          'B017ICUEUI', 'B07B4W9446', 'B07CSQTSNZ', 'B073Y7W2N4', 'B01MYM4L3P', 'B07XNVGKMN', 'B07X637M3F', 'B07KWY5CVG',
#          'B06WD234HH', 'B072FJWP8G', 'B075LWJPFN', 'B001FB63NQ', 'B0711HFDBP', 'B07WZNB2JL', 'B07D16KN2H', 'B075LRJ9CD',
#          'B07WZY6TRH', 'B07T2PKPNB', 'B07X3SNK94', 'B081S7GGMW', 'B00S0NYCCG', 'B01G6P672M', 'B081Y5MJKV', 'B082B1NZW1',
#          'B07NCZGKM7', 'B07K3THJ55', 'B07HBVL9G9', 'B00O8WODFE', 'B08CJMBQHT', 'B075LRK17H', 'B07WZN8K1T', 'B075823379',
#          'B072BM9CF8', 'B015DVQO28', 'B019S2L03G', 'B0733B774W', 'B07DCKVFTW', 'B017ICN86A', 'B074V3QHQ4', 'B07X61HX8Y',
#          'B07X3T2TLB', 'B07W7L79SQ', 'B074V4WTHY', 'B074V4S9XT', 'B074V483FQ', 'B074V45W8J', 'B074V3Y1ZY', 'B079H3CWQX']
#     for i in a:
#         print(i)
#         print(main(i))
