import datetime
import requests
import re
from datetime import date
from dateutil.relativedelta import relativedelta
from wordcloud import WordCloud, STOPWORDS
import matplotlib.pyplot as plt
import boto3
from botocore.exceptions import NoCredentialsError
from pymongo import MongoClient

ACCESS_KEY = ""
SECRET_KEY = ""

client = MongoClient("mongodb://localhost:27017/")

my_database = client["app_etraky"]

my_collection = my_database["channel_sku"]


class CommonUtility:
    BUCKET_NAME = 'etraky'

    @staticmethod
    def insert_record(rec):
        try:
            my_collection.insert(rec, check_keys=False)
        except TypeError:
            pass
        return "write data successfully!!!!!!!"

    @staticmethod
    def upload_to_aws(local_file, bucket, s3_file):
        s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY,
                          aws_secret_access_key=SECRET_KEY)
        try:
            s3.upload_file(local_file, bucket, s3_file)
            print("Upload Successful")
            return True
        except FileNotFoundError:
            print("The file was not found")
            return False
        except NoCredentialsError:
            print("Credentials not available")
            return False

    @staticmethod
    def date_parse(review_date, iso):
        """
        Convert amazon reviews date into the date time format
        :param review_date: Amazon review's date
        :param iso: Amazon review's date
        :return: datetime format date
        """
        if iso == 'IN':
            r_date = "".join(re.findall(r'[0-9]+\s+\w+\s+[0-9]+', review_date))
            r_date = datetime.datetime.strptime(r_date, '%d %B %Y').strftime('%d-%m-%Y')
            return r_date
        if iso == 'US':
            r_date = " ".join(review_date.split()[-3:])
            r_date = datetime.datetime.strptime(r_date, '%B %d, %Y').strftime('%d-%m-%Y')
            return r_date

    @staticmethod
    def another_parse_date(data):

        if 'ago' in data:
            d = int(re.findall(r'\d+', data)[0])
            six_months = date.today() + relativedelta(months=-d)
            return six_months.strftime('%d-%m-%Y')
        elif 'Today' in data:
            return date.today().strftime('%d-%m-%Y')
        else:
            return datetime.datetime.strptime(data, '%b, %Y').strftime('%d-%m-%Y')

    @staticmethod
    def word_cloud_upload(image_name):
        file_upload_url = "https://app.etraky.com/image_upload.php"
        with open(image_name, "rb") as a_file:
            file_dict = {"image": a_file}
            r = requests.post(url=file_upload_url, files=file_dict)
        return r.status_code

    @staticmethod
    def clean_reviews(reviews):
        """
        remove special characters and stopwords from the reviews
        :param reviews: amazon reviews
        :return: cleaned reviews
        """
        reviews = re.sub('[^a-zA-Z]', ' ', reviews)
        reviews = reviews.lower()
        reviews = reviews.split()
        reviews = [word for word in reviews if word not in STOPWORDS]
        reviews = " ".join(reviews)
        return reviews

    @staticmethod
    def generate_word_cloud(reviews, review_type, asin):
        """
        This function will generate the word cloud based on the reviews and type (pos, neg, neu)
        :param reviews:
        :param review_type:
        :param asin:
        :return: word cloud name
        """
        word_cloud = WordCloud(stopwords=STOPWORDS,
                               background_color='White',
                               width=2500,
                               height=2500
                               ).generate(reviews)
        plt.figure(1, figsize=(13, 13))
        plt.imshow(word_cloud)
        plt.axis('off')
        plt.tight_layout()
        image_path = asin + "_" + review_type + '.png'
        # plt.savefig('/home/ubuntu/flaskproject/wordcloud/' + image_path)
        plt.savefig(image_path)
        server_file_path = f"https://{CommonUtility.BUCKET_NAME}.s3.ap-south-1.amazonaws.com/{image_path}"
        # return '/home/ubuntu/flaskproject/wordcloud/' + image_path, server_file_path
        return image_path, server_file_path

    @staticmethod
    def separate_reviews(rating, reviews):
        """
        separate the reviews and based on the rating
        :param rating: reviews rating
        :param reviews: reviews
        :return: positive, negative, neutral
        """
        negative = ''
        neutral = ''
        positive = ''
        for r, j in zip(rating, reviews):
            if int(float(r)) == 1 or int(float(r)) == 2:
                negative += CommonUtility.clean_reviews(j)
            if int(float(r)) == 3:
                neutral += CommonUtility.clean_reviews(j)
            if int(float(r)) == 4 or int(float(r)) == 5:
                positive += CommonUtility.clean_reviews(j)
        if not positive:
            positive += 'NoSentiments'
        if not negative:
            negative += 'NoSentiments'
        if not neutral:
            neutral += 'NoSentiments'
        return positive, negative, neutral

    @staticmethod
    def json_file_upload(file_name):
        """
        This function will upload a file to etraky server
        :param file_name:
        :return:
        """
        file_upload_url = "https://app.etraky.com/json_upload.php"
        with open(file_name, "rb") as a_file:
            file_dict = {"image[]": a_file}
            r = requests.post(url=file_upload_url, files=file_dict)
        return r.status_code
