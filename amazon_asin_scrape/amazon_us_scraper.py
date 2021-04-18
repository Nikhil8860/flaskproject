"""
Modules used in the code
"""
import json
import os
import re
from bs4 import BeautifulSoup
from amazon_asin_scrape.get_main_data import Scraper
from amazon_asin_scrape.common_utility import CommonUtility
from amazon_asin_scrape.asin_from_store_url import Extractor


class DetailsAsin:
    def __init__(self, product_asin, user_id, iso_code):
        self.url = "https://www.amazon.com/dp/" + product_asin
        self.asin = product_asin
        self.all_images = None
        self.iso_code = iso_code
        self.channel = 'amazon'
        self.user_id = user_id
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
            mrp = soup.select_one('.a-text-strike').text
        except (TypeError, AttributeError):
            mrp = 'NA'
        try:
            price = soup.select_one('#priceblock_ourprice').text
        except (TypeError, AttributeError):
            try:
                price = soup.select_one('#priceblock_saleprice').text
            except (TypeError, AttributeError):
                price = 'NA'
        if price == 'NA':
            try:
                price = soup.select_one("#priceblock_dealprice").text
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
            # seller_url = 'https://www.amazon.com/gp/offer-listing/' + self.asin
            seller_url = 'https://www.amazon.com/gp/aod/ajax/ref=olp_aod_redir?qty=1&asin=' + self.asin + '&pc=dp'
            try:
                data, _ = Scraper(seller_url).get_url_data()
            except TypeError:
                return {
                    "status": "error",
                    "data": [],
                    "message": "Page %s was blocked by Amazon. Please try using better proxies\n" % self.url
                }

            seller_soup = BeautifulSoup(data, 'html.parser')
            self.buy_box_seller = [slr.text.replace("\n", '') for slr in
                                   seller_soup.select('#aod-offer-soldBy .a-link-normal')]

            self.buy_box_seller_rating_count = ["".join(re.findall(r'\(\d+', rating.text)).replace("(", "") for rating
                                                in seller_soup.select('#aod-offer-seller-rating span')]

            self.buy_box_seller_price = [
                price.text.replace("$", "") for price in seller_soup.select('#aod-offer-price .a-offscreen')]

            print(self.buy_box_seller, __, sep='--')
            print(self.buy_box_seller_price)
            print(self.buy_box_seller_rating_count)
            if self.buy_box_seller_price or self.buy_box_seller or self.buy_box_seller_rating_count:
                break

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
        extra_details = {}
        key = [i.text.replace("\t", "") for i in soup.select('.prodDetSectionEntry')]
        values = [i.text for i in soup.select('#productDetails_detailBullets_sections1 span , .prodDetAttrValue')]

        for i, j in zip(key, values):
            extra_details[i.replace("\n", '')] = j.replace("\n", '')
        bsr = extra_details

        if not bsr:
            extra_details = {}
            bsr = [i.strip() for i in soup.select_one('#detailBulletsWrapper_feature_div').text.splitlines() if i]
            if 'Product details' in bsr[0]:
                bsr = bsr[1:]
            print(bsr)
            for i, j in zip(range(0, len(bsr) - 3, 3), range(2, len(bsr) - 1, 3)):
                extra_details[bsr[i]] = bsr[j]
            bsr = extra_details
        # Reviews Related Details
        reviews_url = "https://www.amazon.com/product-reviews/" + self.asin
        try:
            reviews_data, session = Scraper(reviews_url).get_url_data()
        except TypeError:
            return {
                "status": "error",
                "data": [],
                "message": "Page %s was blocked by Amazon. Please try using better proxies\n" % self.url
            }
        reviews_soup = BeautifulSoup(reviews_data, 'html.parser')
        rating = reviews_soup.select_one('#cm_cr-product_info .a-color-secondary').text.split()[0]
        brand = reviews_soup.select_one('#cr-arp-byline .a-link-normal').text
        reviews_heading = [r.text for r in reviews_soup.select('.a-text-bold span')]
        reviews_text = [t.text.replace('\n', '') for t in reviews_soup.select('.review-text-content span')]
        reviews_date = [CommonUtility.date_parse(d.text, 'US') for d in
                        reviews_soup.select('#cm_cr-review_list .review-date')]
        reviews_count = len(reviews_text)
        reviews_len = [len(t) for t in reviews_text]
        reviews_rating = [float(r.text.split()[0]) for r in reviews_soup.select('#cm_cr-review_list .review-rating')]

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
        json_data['user_id'] = self.user_id
        json_data['title'] = title
        json_data['mrp'] = mrp.replace("$", "").replace(",", "")
        json_data['price'] = price.replace("$", "").replace(",", "")
        json_data['status'] = status
        json_data['seller'] = seller
        json_data['category'] = category[0]
        json_data['sub_category'] = category[1]
        json_data['bullets_point'] = features
        json_data['brand'] = brand
        json_data['reviews_heading'] = reviews_heading
        json_data['rating'] = rating
        json_data['reviews_date'] = reviews_date
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
        # iso_dict = {self.iso_code: {self.channel: {self.asin: json_response}}}
        iso_dict = {"iso_code": self.iso_code, "channel": self.channel, "asin": self.asin, "data": json_response}
        print(iso_dict)
        file = './json_response_data/' + self.asin + '_US.json'
        with open(file, 'w', encoding='utf-8') as json_file:
            json_file.write(json.dumps(iso_dict, indent=4))
        CommonUtility.json_file_upload(file)
        os.remove(file)
        print("Json File remove!!")
        # print(CommonUtility.insert_record(iso_dict))
        return {
            "status": "True",
            "data": json_response,
            "message": "Success"
        }


def main(url, user_id, iso_code):
    return DetailsAsin(url, user_id, iso_code).get_data()


def main1(url, user_id, iso_code):
    return DetailsAsin(url, user_id, iso_code).get_data()


if __name__ == '__main__':
    # main1('B081H3Y5NW', 120, 'US')
    # quit()
    for i in Extractor().find_all_asin():
        for j in i[0]:
            print(j)
            main(j, i[1], i[2])
