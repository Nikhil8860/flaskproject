from time import sleep
from selenium import webdriver
import json
from bs4 import BeautifulSoup
import os
import pymysql.cursors
from webdriver_manager.chrome import ChromeDriverManager
from webdrivermanager import GeckoDriverManager
global d


def connection():
    print("CONNECTION")
    conn = pymysql.connect(host="172.31.0.81", user="root",
                           password='evanik@2019', database="evanik_erp_cronjobs")

    cursor = conn.cursor()
    query = """SELECT UserId,UserName,PASSWORD,channel_id,TYPE,sellerId,updatetime FROM inv_userlist WHERE TYPE IN ('snapdeal') AND exp_date  > NOW() 
                AND active ='1' ORDER BY priority DESC,UserId DESC"""
    cursor.execute(query)
    info = cursor.fetchall()
    cursor.close()
    conn.close()
    user_id = []
    username = []
    password = []
    channel_id = []
    seller_id = []
    for i in info:
        user_id.append(i[0])
        username.append(i[1])
        password.append(i[2])
        channel_id.append(i[3])
        seller_id.append(i[5])
    return user_id, username, password, channel_id, seller_id


def enable_download_in_headless_chrome(browser, download_dir):
    browser.command_executor._commands["send_command"] = ("POST", '/session/$sessionId/chromium/send_command')

    params = {'cmd': 'Page.setDownloadBehavior', 'params': {'behavior': 'allow', 'downloadPath': download_dir}}
    browser.execute("send_command", params)


def process():
    global d
    user_id, username, password, channel_id, seller_id = connection()
    print(user_id, username, password, channel_id, seller_id)
    for usr_id, username, password, chanel_id, sellerId in zip(user_id, username, password, channel_id, seller_id):
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--headless")
        gdd = GeckoDriverManager()
        gdd.download_and_install()
        driver = webdriver.Chrome(gdd.download_and_install(), options=chrome_options)
        # driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)
        # driver = webdriver.Chrome(chrome_options=chrome_options, executable_path="/usr/bin./chromedriver")

        _dir = os.path.join(f"/var/www/html/flaskproject/snapdeal/{usr_id}/{chanel_id}")
        enable_download_in_headless_chrome(driver, _dir)
        driver.get('https://sellers.snapdeal.com/NewSnapdealLogin')
        sleep(2)
        try:
            driver.find_element_by_id('j_id0:navbar:txtUserName').send_keys(username)
            sleep(2)
        except Exception as e:
            print("FAIL: " + str(e))
        try:
            driver.find_element_by_id('j_id0:navbar:txtPassword').send_keys(password)
            sleep(2)
        except Exception as e:
            print("wrong: " + str(e))
        try:
            driver.find_element_by_class_name('sf-button-secondary').click()
            sleep(3)
        except Exception as e:
            print("Password wrong: " + str(e))
        driver.get('https://seller.snapdeal.com/report/get?category=Orders&pageSize=5&start=0')
        sleep(3)
        info = driver.page_source
        soup = BeautifulSoup(info, 'html.parser')
        data = soup.find('pre')
        sleep(5)
        with open('info.txt', 'w') as f:
            sleep(2)
            try:
                f.write(str(data.get_text()))
            except Exception as e:
                print("Wrong Password: " + str(e))
        f1 = open('info.txt')
        sleep(3)
        try:
            d = json.load(f1)
        except Exception as e:
            print(e)
        try:
            for info in d['reports']:
                try:
                    url = 'https://seller.snapdeal.com/report/download/FullOrder/' + str(info['code'])
                    driver.get(url)
                    sleep(2)
                except Exception as e:
                    print("Error: ", e)
        except Exception as e:
            print(e)
        sleep(10)
        print("Success")
        driver.close()
    return "Done"

process()