"""
Module used in the code
"""
from amazon_data_sync import amazon_data_sync
from flipkart_data_sync import flipkart_data_sync
from meesho_data_sync import meesho_finals
from flipkart_charges_variance import flipkart_charges_final, amazon_charges_final
from flask import Flask, render_template, request, redirect, url_for, flash, current_app, jsonify
from werkzeug.utils import secure_filename
from flipkart_shipping_variance import FlipkartReco
from datetime import date
from sqlalchemy import create_engine
import logging as logger
import flipkart_matching
import amazon_matching
import paytm_matching
import pandas as pd
from db import DB
from amazon_variance import AmazonReco
from amazon_asin_scrape import get_data_asin
from amazon_asin_scrape import amazon_us_scraper
from amazon_asin_scrape import flipkart_product_search
import pymysql.cursors
import os
import shutil
import json

logger.basicConfig(level="DEBUG")

app = Flask(__name__)
app.config['SECRET_KEY'] = 'the random string'
app.config['UPLOAD_EXTENSIONS'] = ['.xls', '.xlsx', '.csv']
app.config['UPLOAD_FOLDER'] = '/home/ubuntu/flaskproject/uploaded_files'
app.config['DOWNLOAD_FOLDER'] = '/home/ubuntu/flaskproject/downloaded_files'


#  code added By Nikhil

@app.route("/shipping_variance", methods=['GET', "POST"])
def shipping_variance():
    if request.method == 'POST':
        user_id = request.form.get('user_id')
        response = redirect('/flipkart/api/v1/insert-data/' + str(user_id) + '/')
        return response

    return render_template('shipping-variance.html')


@app.route("/get-args", methods=['GET', 'POST'])
def get_args():
    asin = request.arg.get('asin')
    return {"ASN": asin}


@app.route("/amazon/api/in/asin-data/<string:asin>")
def amazon_india(asin):
    json_data = get_data_asin.main(asin)
    return current_app.response_class(json.dumps(json_data), mimetype="application/json")


@app.route("/amazon/api/us/asin-data/<string:asin>")
def amazon_us(asin):
    json_data = amazon_us_scraper.main(asin)
    return current_app.response_class(json.dumps(json_data), mimetype="application/json")


@app.route("/flipkart/api/in/data/<string:pid>")
def flipkart_in(pid):
    json_data = flipkart_product_search.main(pid)
    return current_app.response_class(json.dumps(json_data), mimetype="application/json")


@app.route('/flipkart/api/v1/insert-data/<int:user_id>/<string:start_date>/<string:end_date>', methods=['GET'])
def insert_data(user_id, start_date, end_date):
    logger.debug(user_id)
    obj = FlipkartReco(user_id)
    obj.get_state(start_date, end_date)
    flash(f'Shipping Variance done for - {user_id}')
    return redirect(url_for('charges_variance1'))


@app.route('/amazon/api/v1/insert-data/<int:user_id>/<string:start_date>/<string:end_date>', methods=['GET'])
def amazon_variance_data(user_id, start_date, end_date):
    print(user_id, start_date, end_date)
    print("___________________________________________-")
    amazon = AmazonReco(user_id, start_date, end_date)
    amazon.get_state()
    flash(f'Shipping Variance done for Amazon - {user_id}')
    return redirect(url_for('charges_variance1'))


@app.route("/snapdeal_download")
def snapdeal_report_download():
    return render_template('snapdeal-download.html')


@app.route("/")
def home():
    return render_template('home.html')


min_date = date(2018, 1, 1)
max_date = date.today()


@app.route("/index", methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        sellerid = request.form['sellerid']
        channel = request.form['channel']
        start_date = request.form['start']
        end_date = request.form['end']

        sellerid = str(sellerid).strip()

        if channel == 'amazon':
            return redirect(
                url_for('amazon', sellerid=sellerid, channel=channel, start_date=start_date, end_date=end_date))
        elif (channel == 'flipkart') or (channel == '2gud'):
            return redirect(
                url_for('flipkart', sellerid=sellerid, channel=channel, start_date=start_date, end_date=end_date))
        elif channel == 'paytm':
            return redirect(
                url_for('paytm', sellerid=sellerid, channel=channel, start_date=start_date, end_date=end_date))

    return render_template("index.html", min_date=min_date, max_date=max_date)


@app.route('/amazon/<string:sellerid>/<string:channel>/<string:start_date>/<string:end_date>', methods=['GET', 'POST'])
def amazon(sellerid, channel, start_date, end_date):
    engine = create_engine(f'mysql://root:evanik@2019@172.31.0.81:3306/invento_{sellerid}')
    df_channels = pd.read_sql_table('channels', engine)
    df_channels = df_channels[['type', 'displayName', 'sellerId']]
    df_channels = df_channels[df_channels['type'] == 'amazon']

    if df_channels['sellerId'].unique()[0] != None:
        stores = df_channels['sellerId'].unique()
    else:
        stores = df_channels['sellerId'].unique()

    if request.method == 'POST':
        mtr_b2c = request.files['mtrb2c']
        mtr_b2b = request.files['mtrb2b']
        store = request.form.getlist('storesss')

        filename3 = secure_filename(mtr_b2c.filename)
        mtr_b2c.save(os.path.join(app.config['UPLOAD_FOLDER'], filename3))
        mtr_b2c_retrieve = os.path.join(app.config['UPLOAD_FOLDER'], filename3)

        if mtr_b2b:
            filename4 = secure_filename(mtr_b2b.filename)
            mtr_b2b.save(os.path.join(app.config['UPLOAD_FOLDER'], filename4))
            mtr_b2b_retrieve = os.path.join(app.config['UPLOAD_FOLDER'], filename4)
        else:
            filename4 = None
            mtr_b2b_retrieve = None

        print(store)
        m, mm = amazon_matching.amazon_match(mtr_b2c_retrieve, mtr_b2b_retrieve, store, engine, sellerid, channel,
                                             start_date,
                                             end_date)

        fieldnames = [i for i in m.keys()]
        # program ends....

        if os.path.exists(os.path.join(app.config['UPLOAD_FOLDER'], filename3)):
            os.remove(os.path.join(app.config['UPLOAD_FOLDER'], filename3))
        if filename4:
            if os.path.exists(os.path.join(app.config['UPLOAD_FOLDER'], filename4)):
                os.remove(os.path.join(app.config['UPLOAD_FOLDER'], filename4))
        return render_template('result.html', m=m, fieldnames=fieldnames, ch=channel, mm=mm)

    return render_template('amazon.html', id=sellerid, ch=channel, sd=start_date, ed=end_date, stores=stores)


@app.route('/flipkart/<string:sellerid>/<string:channel>/<string:start_date>/<string:end_date>',
           methods=['GET', 'POST'])
def flipkart(sellerid, channel, start_date, end_date):
    engine = create_engine(f'mysql://root:evanik@2019@172.31.0.81:3306/invento_{sellerid}')
    df_channels = pd.read_sql_table('channels', engine)
    df_channels = df_channels[['type', 'displayName', 'sellerId']]
    if channel == 'flipkart':
        df_channels = df_channels[df_channels['type'] == 'flipkart']
    if channel == '2gud':
        df_channels = df_channels[df_channels['type'] == '2gud']

    if df_channels['sellerId'].unique()[0] != None:
        stores = df_channels['sellerId'].unique()
    else:
        stores = None

    if request.method == 'POST':
        flipkart_file = request.files['filpkartfile']
        store = request.form.getlist('storesss')

        filename2 = secure_filename(flipkart_file.filename)

        flipkart_file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename2))
        flipkart_file_retrieve = os.path.join(app.config['UPLOAD_FOLDER'], filename2)

        m, mm = flipkart_matching.flipkart_match(flipkart_file_retrieve, store, engine, sellerid, channel,
                                                 start_date, end_date)
        fieldnames = [i for i in m.keys()]
        # program ends....
        if os.path.exists(os.path.join(app.config['UPLOAD_FOLDER'], filename2)):
            os.remove(os.path.join(app.config['UPLOAD_FOLDER'], filename2))
        return render_template('result.html', m=m, fieldnames=fieldnames, ch=channel, mm=mm)
    return render_template('flipkart.html', id=sellerid, ch=channel, sd=start_date, ed=end_date, stores=stores)


@app.route('/paytm/<string:sellerid>/<string:channel>/<string:start_date>/<string:end_date>', methods=['GET', 'POST'])
def paytm(sellerid, channel, start_date, end_date):
    engine = create_engine(f'mysql://root:evanik@2019@172.31.0.81:3306/invento_{sellerid}')
    df_channels = pd.read_sql_table('channels', engine)
    df_channels = df_channels[['type', 'displayName', 'sellerId']]
    df_channels = df_channels[df_channels['type'] == 'paytm']

    if df_channels['sellerId'].unique()[0] != None:
        stores = df_channels['sellerId'].unique()
    else:
        stores = None

    if request.method == 'POST':
        paytmfile = request.files['paytmfile']
        store = request.form.getlist('storesss')

        filename1 = secure_filename(paytmfile.filename)

        paytmfile.save(os.path.join(app.config['UPLOAD_FOLDER'], filename1))
        paytm_file_retrieve = os.path.join(app.config['UPLOAD_FOLDER'], filename1)

        m, mm = paytm_matching.paytm_match(paytm_file_retrieve, store, engine, sellerid, channel, start_date,
                                           end_date)
        fieldnames = [i for i in m.keys()]
        if os.path.exists(os.path.join(app.config['UPLOAD_FOLDER'], filename1)):
            os.remove(os.path.join(app.config['UPLOAD_FOLDER'], filename1))
        return render_template('result.html', m=m, fieldnames=fieldnames, ch=channel, mm=mm)
    return render_template('paytm.html', id=sellerid, ch=channel, sd=start_date, ed=end_date, stores=stores)


@app.route('/file_uploader', methods=['GET', 'POST'])
def file_uploader():
    if request.method == 'POST':
        SELLER_ID = request.form.get('sellerId')
        SELLER_ID = str(SELLER_ID).strip()
        print(SELLER_ID)
        return redirect(url_for('file_uploader_channel', sellerid=SELLER_ID))

    return render_template('file_uploader.html')


@app.route('/file_uploader_channel/<string:sellerid>', methods=['GET', 'POST'])
def file_uploader_channel(sellerid):
    engine = create_engine(f'mysql://root:evanik@2019@172.31.0.81:3306/invento_{sellerid}')
    df_channels = pd.read_sql_table('channels', engine)
    channels = df_channels['type'].unique()
    print(channels)
    print(sellerid)
    if request.method == 'POST':
        channel = request.form.get('channell')
        print(channel)
        return redirect(url_for('flipkart_upload1', sellerid=sellerid, channel=channel))

    return render_template('file_uploader_channel.html', sellerid=sellerid, channels=channels)


@app.route('/flipkart_upload1/<string:sellerid>/<string:channel>', methods=['GET', 'POST'])
def flipkart_upload1(sellerid, channel):
    engine = create_engine(f'mysql://root:evanik@2019@172.31.0.81:3306/invento_{sellerid}')
    df_channels = pd.read_sql_table('channels', engine)
    df_channels = df_channels[['type', 'sellerId']]
    df_channels = df_channels[df_channels['type'] == channel]

    stores = df_channels['sellerId'].unique()
    if request.method == 'POST':
        store = request.form.get('channel')
        file_upload = request.files['filpkartuploadfile']

        filename = secure_filename(file_upload.filename)
        file_upload.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))

        print("FILE NAME: ", file_upload.filename)
        # filenam = secure_filename(file_upload.filename)
        # file_upload.save(os.path.join(app.config['UPLOAD_FOLDER'], filenam))
        file_retrieve = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        print(file_retrieve)
        df_flip_upload = pd.read_excel(file_retrieve, sheet_name='Sales Report')
        # df_flip_upload = pd.read_excel(file_retrieve, sheet_name=1, engine='openpyxl')
        print(df_flip_upload.head(10))
        df_flip_upload["Event Sub Type"] = df_flip_upload["Event Sub Type"].replace('Sale', "Delivered")
        df_flip_upload.pop('Order Approval Date ')
        df_flip_upload.pop("Customer's Delivery Pincode")
        df_flip_upload.pop("Customer's Delivery State")
        df_flip_upload.pop('Order Shipped From (State)')
        df_flip_upload.pop('CST Rate')
        df_flip_upload.pop('CST Amount')
        df_flip_upload.pop('Price before discount')
        df_flip_upload.pop('Buyer Invoice Amount ')

        df_flip_upload.replace(regex=['SKU:'], value='', inplace=True)
        df_flip_upload.replace(regex=['"'], value='', inplace=True)
        df_flip_upload.replace(regex=["'"], value="", inplace=True)
        df_flip_upload.rename(columns={
            'Seller Share ': 'SellerShare',
            'Order ID': 'OrderId',
            'Order Item ID': 'OrderItemID',
            'Fulfilment Type': 'OrderType',
            'Product Title/Description': 'Product',
            'FSN': 'FSN',
            'SKU': 'SKUCode',
            'HSN Code': 'hsn_sac_code',
            'Event Sub Type': 'sale_status',
            'Order Date': 'date',
            'Item Quantity': 'total_items',
            'Shipping Charges': 'shippingFee',
            'Final Invoice Amount (Price after discount+Shipping Charges)': 'grand_total',
            'Taxable Value (Final Invoice Amount -Taxes)': 'total',
            'IGST Rate': 'igst_rate',
            'IGST Amount': 'igst_amount',
            'CGST Rate': 'cgst_rate',
            'CGST Amount': 'cgst_amount',
            'SGST Rate (or UTGST as applicable)': 'sgst_rate',
            'SGST Amount (Or UTGST as applicable)': 'sgst_amount',
            'Buyer Invoice ID': 'invoice_number',
            'Buyer Invoice Date': 'invoice_date',
            "Customer's Billing Pincode": 'PinCode',
            "Customer's Billing State": 'State'
        }, inplace=True)

        df_flip_upload.insert(1, 'sellerId', store)
        df_flip_upload.insert(1, 'created_by', sellerid)
        df_flip_upload.insert(1, 'staff_note', 'Manual')
        df_flip_upload.insert(1, 'reference_no', df_flip_upload['OrderId'])

        df_flip_upload['date'] = pd.to_datetime(df_flip_upload['date'])
        df_flip_upload['invoice_date'] = pd.to_datetime(df_flip_upload['invoice_date'])
        df_flip_upload['invoice_date'] = pd.to_datetime(df_flip_upload['invoice_date'])

        df_flip_upload['grand_total'] = df_flip_upload['grand_total'].abs()
        df_flip_upload['total'] = df_flip_upload['total'].abs()
        df_flip_upload['total_items'] = df_flip_upload['total_items'].abs()
        # df_flip_upload.to_csv('df_file_upload.csv', index=False)

        DB.sellerid = sellerid
        db = DB()
        db.connect()

        engine = create_engine(f'mysql://root:evanik@2019@172.31.0.81:3306/invento_{sellerid}')

        dfSales1 = df_flip_upload[df_flip_upload['sale_status'] == 'Delivered']
        dfSales1.to_sql(con=engine, name='sales_temp', if_exists='replace', index=False)

        query = "INSERT INTO sales (OrderId,reference_no,staff_note,created_by,sellerId,OrderItemID,Product,FSN,hsn_sac_code,SKUCode,sale_status,OrderType,date,total_items,shippingFee,grand_total,total,igst_rate,igst_amount,cgst_rate,cgst_amount,sgst_rate,sgst_amount,PinCode,State,invoice_number,invoice_date) SELECT OrderId,reference_no,staff_note,created_by,sellerId,OrderItemID,Product,FSN,hsn_sac_code,SKUCode,sale_status,OrderType,date,total_items,shippingFee,grand_total,total,igst_rate,igst_amount,cgst_rate,cgst_amount,sgst_rate,sgst_amount,PinCode,State,invoice_number,invoice_date FROM sales_temp ON DUPLICATE KEY UPDATE sales.OrderItemID=sales_temp.OrderItemID,sales.OrderId=sales_temp.OrderId,sales.reference_no=sales_temp.reference_no,sales.created_by=sales_temp.created_by,sales.sellerId=sales_temp.sellerId,sales.sale_status=sales_temp.sale_status,sales.invoice_date=sales_temp.invoice_date,sales.invoice_number=sales_temp.invoice_number,sales.total_items=sales_temp.total_items,sales.grand_total=sales_temp.grand_total,sales.total=sales_temp.total,sales.PinCode=sales_temp.PinCode,sales.State=sales_temp.State"
        db.query(query)

        query1 = 'DROP TABLE sales_temp'
        db.query(query1)

        engine = create_engine(f'mysql://root:evanik@2019@172.31.0.81:3306/invento_{sellerid}')
        dfSales2 = df_flip_upload[df_flip_upload['sale_status'] == 'Cancellation']
        dfSales2.to_sql(con=engine, name='sales_temp', if_exists='replace', index=False)
        query = "INSERT INTO sales (OrderId,reference_no,staff_note,created_by,sellerId,OrderItemID,Product,FSN,hsn_sac_code,SKUCode,sale_status,OrderType,date,total_items,shippingFee,grand_total,total,igst_rate,igst_amount,cgst_rate,cgst_amount,sgst_rate,sgst_amount,PinCode,State) SELECT OrderId,reference_no,staff_note,created_by,sellerId,OrderItemID,Product,FSN,hsn_sac_code,SKUCode,sale_status,OrderType,date,total_items,shippingFee,grand_total,total,igst_rate,igst_amount,cgst_rate,cgst_amount,sgst_rate,sgst_amount,PinCode,State FROM sales_temp ON DUPLICATE KEY UPDATE sales.OrderItemID=sales_temp.OrderItemID,sales.OrderId=sales_temp.OrderId,sales.reference_no=sales_temp.reference_no,sales.created_by=sales_temp.created_by,sales.sellerId=sales_temp.sellerId,sales.sale_status=sales_temp.sale_status"
        db.query(query)

        query1 = 'DROP TABLE sales_temp'
        db.query(query1)

        engine = create_engine(f'mysql://root:evanik@2019@172.31.0.81:3306/invento_{sellerid}')
        dfSales3 = df_flip_upload[df_flip_upload['sale_status'] == 'Return']
        dfSales3.to_sql(con=engine, name='sales_temp', if_exists='replace', index=False)
        query = "INSERT INTO sales (OrderId,reference_no,staff_note,created_by,sellerId,OrderItemID,Product,FSN,hsn_sac_code,SKUCode,sale_status,OrderType,date,total_items,shippingFee,grand_total,total,igst_rate,igst_amount,cgst_rate,cgst_amount,sgst_rate,sgst_amount,PinCode,State) SELECT OrderId,reference_no,staff_note,created_by,sellerId,OrderItemID,Product,FSN,hsn_sac_code,SKUCode,sale_status,OrderType,date,total_items,shippingFee,grand_total,total,igst_rate,igst_amount,cgst_rate,cgst_amount,sgst_rate,sgst_amount,PinCode,State FROM sales_temp ON DUPLICATE KEY UPDATE sales.OrderItemID=sales_temp.OrderItemID,sales.OrderId=sales_temp.OrderId,sales.reference_no=sales_temp.reference_no,sales.created_by=sales_temp.created_by,sales.sellerId=sales_temp.sellerId,sales.sale_status=sales_temp.sale_status"
        db.query(query)

        query1 = 'DROP TABLE sales_temp'
        db.query(query1)

        engine = create_engine(f'mysql://root:evanik@2019@172.31.0.81:3306/invento_{sellerid}')
        dfSales4 = df_flip_upload[df_flip_upload['sale_status'] == 'Return Cancellation']
        dfSales4.to_sql(con=engine, name='sales_temp', if_exists='replace', index=False)
        query = "INSERT INTO sales (OrderId,reference_no,staff_note,created_by,sellerId,OrderItemID,Product,FSN,hsn_sac_code,SKUCode,sale_status,OrderType,date,total_items,shippingFee,grand_total,total,igst_rate,igst_amount,cgst_rate,cgst_amount,sgst_rate,sgst_amount,PinCode,State) SELECT OrderId,reference_no,staff_note,created_by,sellerId,OrderItemID,Product,FSN,hsn_sac_code,SKUCode,sale_status,OrderType,date,total_items,shippingFee,grand_total,total,igst_rate,igst_amount,cgst_rate,cgst_amount,sgst_rate,sgst_amount,PinCode,State FROM sales_temp ON DUPLICATE KEY UPDATE sales.OrderItemID=sales_temp.OrderItemID,sales.OrderId=sales_temp.OrderId,sales.reference_no=sales_temp.reference_no,sales.created_by=sales_temp.created_by,sales.sellerId=sales_temp.sellerId,sales.sale_status=sales_temp.sale_status"
        db.query(query)

        query1 = 'DROP TABLE sales_temp'
        db.query(query1)

        flash(f'File successfully Uploaded - {sellerid}')
        # os.path.join(app.config['UPLOAD_FOLDER'], filename)
        if os.path.exists(os.path.join(app.config['UPLOAD_FOLDER'], filename)):
            os.remove(os.path.join(app.config['UPLOAD_FOLDER'], filename))
        return redirect(url_for('file_uploader'))

    return render_template('flipkart_upload1.html', stores=stores, channel=channel, sellerid=sellerid)


@app.route('/charges_variance', methods=['GET', 'POST'])
def charges_variance1():
    if request.method == 'POST':
        user_id = request.form.get('user_id')
        user_id = str(user_id).strip()
        channel = request.form.get('channel')
        fees = request.form.get('fees')
        start_date = request.form.get('start')
        end_date = request.form.get('end')
        print(user_id, channel, start_date, end_date)

        if fees == 'charges':
            if channel == 'flipkart':
                response = flipkart_charges_final(user_id, start_date, end_date)
                if response:
                    flash(f'flipkart Charges done for (COLLECTION FEE, FIXED FEE) - {user_id}')
                    return redirect(url_for('charges_variance1'))
                else:
                    flash(f'flipkart Charges **Not done for (COLLECTION FEE, FIXED FEE) - {user_id} due to some error')
                    return redirect(url_for('charges_variance1'))
            if channel == 'amazon':
                response = amazon_charges_final(user_id, start_date, end_date)
                if response:
                    flash(f'amazon Charges done for (Closing FEE) - {user_id}')
                    return redirect(url_for('charges_variance1'))
                else:
                    flash(f'amazon Charges **Not done for (Closing FEE) - {user_id} due to some error')
                    return redirect(url_for('charges_variance1'))
        if fees == 'shipping':
            if channel == 'flipkart':
                return redirect(url_for('insert_data', user_id=user_id, start_date=start_date, end_date=end_date))
            if channel == 'amazon':
                return redirect(
                    url_for('amazon_variance_data', user_id=user_id, start_date=start_date, end_date=end_date))
    return render_template('charges_variance1.html', min_date=min_date, max_date=max_date)


@app.route('/data_sync', methods=['GET', 'POST'])
def data_sync():
    if request.method == 'POST':
        user_id = request.form.get('user_id')
        user_id = str(user_id).strip()
        channel = request.form.get('channel')
        start_date = request.form.get('start')
        end_date = request.form.get('end')
        print(user_id, channel, start_date, end_date)

        if channel == 'amazon':

            response = amazon_data_sync(user_id)
            if response:
                flash(f'amazon data has been successfully synced Userid: {user_id}!!!')
                return redirect(url_for('data_sync'))

        if channel == 'flipkart':
            y = start_date.split("-")[0]
            m = start_date.split("-")[1]
            dates = f'{y}_{m}'
            response = flipkart_data_sync(user_id, dates)
            if response:
                flash(f'flipkart data has been successfully synced Userid: {user_id} for {dates}!!!')
                return redirect(url_for('data_sync'))

        if channel == 'meesho':
            folder_name = str(user_id + '_' + channel)
            if os.path.exists(os.path.join(app.config['DOWNLOAD_FOLDER'], folder_name)):
                shutil.rmtree(os.path.join(app.config['DOWNLOAD_FOLDER'], folder_name))

            os.makedirs(os.path.join(app.config['DOWNLOAD_FOLDER'], folder_name))
            response = meesho_finals(user_id, start_date, end_date)
            if response:
                flash(f'Meesho data has been successfully synced Userid: {user_id}, from {start_date} to {end_date}!!!')
                return redirect(url_for('data_sync'))
    return render_template('data_sync_main.html', min_date=min_date, max_date=max_date)


@app.route('/anuj/api/get_channel_list/userid/<string:user_id>', methods=['GET', 'POST'])
def get_user_channels(user_id):
    if user_id.__contains__(','):
        user_list = user_id.split(',')
    else:
        user_list = [user_id]
    li = {}
    for ii in range(len(user_list)):
        con = pymysql.connect(host='172.31.0.81', user='root',
                              password='evanik@2019',
                              database='invento_{}'.format(user_list[ii]))
        cur = con.cursor()
        query = "select id, user_name, user_password, type, sellerId, active from channels where active=0 and type not in ('pos');"
        cur.execute(query)
        data = cur.fetchall()
        cur.close()
        con.close()
        jj = []

        for i in data:
            jj.append({
                'channel_id': i[0],
                'user_name': i[1],
                'user_password': i[2],
                'type': i[3],
                'sellerId': i[4]
            })
        li.update({user_list[ii]: jj})

    return jsonify({f'userlist': li})


# select count(distinct(UserId)) from evanik_erp_cronjobs.inv_userlist where exp_date > NOW() and ;

@app.route('/anuj/api/get_channel_list/allusers', methods=['GET', 'POST'])
def get_allusers():
    con = pymysql.connect(host='172.31.0.81', user='root',
                          password='evanik@2019',
                          database='evanik_erp_cronjobs')
    cur = con.cursor()
    query = "select distinct(UserId) from inv_userlist where exp_date > NOW() order by UserId desc;"
    cur.execute(query)
    data = cur.fetchall()
    cur.close()
    con.close()
    li = {}
    for user in data:
        con = pymysql.connect(host='172.31.0.81', user='root',
                              password='evanik@2019',
                              database='invento_{}'.format(user[0]))
        cur = con.cursor()
        query = "select id, user_name, user_password, type, sellerId, active from channels where active=0 and type not in ('pos');"
        cur.execute(query)
        data1 = cur.fetchall()
        cur.close()
        con.close()
        jj = []

        for i in data1:
            jj.append({
                'channel_id': i[0],
                'user_name': i[1],
                'user_password': i[2],
                'type': i[3],
                'sellerId': i[4]
            })
        li.update({user[0]: jj})
    return jsonify({'all_users': li})


@app.route('/anuj/api/get_channel_list/product/<prod_name>', methods=['GET', 'POST'])
def get_prod(prod_name):
    con = pymysql.connect(host='172.31.0.81', user='root',
                          password='evanik@2019',
                          database='evanik_erp_cronjobs')
    cur = con.cursor()
    query = "select distinct(UserId) from inv_userlist where exp_date > NOW() order by UserId desc;"
    cur.execute(query)
    data = cur.fetchall()
    cur.close()
    con.close()
    li = {}
    for user in data:
        con = pymysql.connect(host='172.31.0.81', user='root',
                              password='evanik@2019',
                              database='invento_{}'.format(user[0]))
        cur = con.cursor()
        query = f"SELECT `name`, price, mrp, channel_type, serial_number, your_selling_price FROM products WHERE LOWER(`name`) LIKE '%{prod_name}%';"
        cur.execute(query)
        data1 = cur.fetchall()
        cur.close()
        con.close()
        jj = []

        for i in data1:
            if str(i[3]).lower() == 'amazon':
                url = "https://www.amazon.in/dp/" + str(i[4])
            else:
                url = str(i[4])
            jj.append({
                'Prod_name': str(i[0]),
                'prod_price': str(i[1]),
                'prod_mrp': str(i[2]),
                'channel_type': str(i[3]),
                'serial_number': url,
                'your_selling_price': str(i[5])
            })
        if len(jj) > 0:
            li.update({user[0]: jj})
    return jsonify({'all_users': li})




if __name__ == "__main__":
    app.run(debug=True)
