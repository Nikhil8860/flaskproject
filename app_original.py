from flask import Flask, render_template, request, redirect, url_for, flash, send_from_directory, abort, send_file
from datetime import date
import pandas as pd
from sqlalchemy import create_engine
from time import sleep
import requests
from bs4 import BeautifulSoup
import re
import pymysql.cursors
from werkzeug.utils import secure_filename
import os

# UPLOAD_FOLDER = '/path/to/the/uploads'
# ALLOWED_EXTENSIONS = {'.xls', 'xlsx', 'csv'}

app = Flask(__name__)
app.config['SECRET_KEY'] = 'the random string'
app.config['UPLOAD_EXTENSIONS'] = ['.xls', 'xlsx', 'csv']
app.config['UPLOAD_FOLDER'] = '/home/ubuntu/flaskproject/uploaded_files'


class DB:
    conn = None
    sellerid = None

    def connect(self):
        self.conn = pymysql.connect('172.31.0.81', 'root', 'evanik@2019', f'invento_{DB.sellerid}')

    def query(self, sql):
        try:
            cursor = self.conn.cursor()
            print("CURSON")
            print(cursor)
            try:
                cursor.execute(sql)
            except:
                pass
            self.conn.commit()
            print(cursor.rowcount, "record(s) affected")
            print('success')
        except (AttributeError, pymysql.err.InterfaceError, pymysql.OperationalError):
            self.connect()
            cursor = self.conn.cursor()
            try:
                cursor.execute(sql)
            except:
                pass
            self.conn.commit()
            print("success11")
        return cursor


#  code added
@app.route("/shipping_variance", methods=['GET', "POST"])
def shipping_variance():
    if request.method == 'POST':
        user_id = request.form.get('user_id')
        return {"user_id": user_id}

    return render_template('shipping-variance.html')


# end code

@app.route("/")
def home():
    return render_template('home.html')
    # return 'hello world'


@app.route("/index", methods=['GET', 'POST'])
def index():
    min_date = date(2018, 1, 1)
    max_date = date.today()
    # max_date = date.today().replace(day=1) - timedelta(days=1)
    if request.method == 'POST':
        sellerid = request.form['sellerid']
        channel = request.form['channel']
        start_date = request.form['start']
        end_date = request.form['end']

        if channel == 'amazon':
            return redirect(
                url_for('amazon', sellerid=sellerid, channel=channel, start_date=start_date, end_date=end_date))
        elif channel == 'flipkart':
            return redirect(
                url_for('flipkart', sellerid=sellerid, channel=channel, start_date=start_date, end_date=end_date))
        elif channel == 'paytm':
            return redirect(
                url_for('paytm', sellerid=sellerid, channel=channel, start_date=start_date, end_date=end_date))

    return render_template("index.html", min_date=min_date, max_date=max_date)


def amazon_match(mtr_b2c_retrieve, mtr_b2b_retrieve, store, engine, sellerid, channel, start_date, end_date):
    if mtr_b2c_retrieve.filename != '' and mtr_b2c_retrieve.filename != '':
        # b2c_file
        df_b2c = pd.read_csv(mtr_b2c_retrieve)
        df_b2c = df_b2c[df_b2c["Transaction Type"] != 'Refund']
        df_b2c = df_b2c[df_b2c["Transaction Type"] != 'Cancel']
        b2c = pd.DataFrame(df_b2c, columns=['Invoice Number', 'Invoice Amount', 'Invoice Date'])
        b2c['Invoice Date'] = pd.to_datetime(b2c['Invoice Date'])

        b2c_duplicate = b2c[b2c.duplicated(['Invoice Number'], keep=False)]
        a = b2c_duplicate.groupby(['Invoice Number'], as_index=False).agg(
            {'Invoice Amount': "sum", 'Invoice Date': 'first'})

        b2c.drop_duplicates(subset='Invoice Number', keep=False, inplace=True)
        b2c = b2c.append(a)

        # mtr b2b_file
        df_b2b = pd.read_csv(mtr_b2b_retrieve)
        df_b2b = df_b2b[df_b2b["Transaction Type"] != 'Refund']
        df_b2b = df_b2b[df_b2b["Transaction Type"] != 'Cancel']
        b2b = pd.DataFrame(df_b2b, columns=['Invoice Number', 'Invoice Amount', 'Invoice Date'])
        b2b['Invoice Date'] = pd.to_datetime(b2b['Invoice Date'])

        b2b_duplicate = b2b[b2b.duplicated(['Invoice Number'], keep=False)]
        b = b2b_duplicate.groupby(['Invoice Number'], as_index=False).agg(
            {'Invoice Amount': "sum", 'Invoice Date': 'first'})
        b2b.drop_duplicates(subset='Invoice Number', keep=False, inplace=True)
        b2b = b2b.append(b)

        # mtr = b2c+b2b
        mtr = b2c.append(b2b)


    elif mtr_b2c_retrieve.filename != '' and mtr_b2b_retrieve.filename == '':
        df_b2c = pd.read_csv(mtr_b2c_retrieve)
        df_b2c = df_b2c[df_b2c["Transaction Type"] != 'Refund']
        df_b2c = df_b2c[df_b2c["Transaction Type"] != 'Cancel']
        b2c = pd.DataFrame(df_b2c, columns=['Invoice Number', 'Invoice Amount', 'Invoice Date'])
        b2c['Invoice Date'] = pd.to_datetime(b2c['Invoice Date'])

        b2c_duplicate = b2c[b2c.duplicated(['Invoice Number'], keep=False)]
        a = b2c_duplicate.groupby(['Invoice Number'], as_index=False).agg(
            {'Invoice Amount': "sum", 'Invoice Date': 'first'})

        b2c.drop_duplicates(subset='Invoice Number', keep=False, inplace=True)
        b2c = b2c.append(a)

        mtr = b2c

    # evanik file - allsalesExport
    engine = create_engine(f'mysql://root:evanik@2019@172.31.0.81:3306/invento_{sellerid}')
    evanik = pd.read_sql_query(
        f"SELECT `s`.`sellerId` as `sellerId`, `s`.`invoice_number` as `Invoice Number`, `s`.`sale_status` as `Sale status`, `s`.`grand_total` as `Grand total`, `s`.`invoice_date` as `Invoice date `, `ch`.`type` as `channel`, `ch`.`displayName` as`storeName` FROM `sales` `s` LEFT JOIN `users` `b` ON `s`.`biller_id`=`b`.`id` LEFT JOIN `users` `c` ON `s`.`customer_id`=`c`.`id` LEFT JOIN `invoice` `i` ON `i`.`sales_id` = `s`.`sales_id` LEFT JOIN `sales_items` `si` ON `si`.`sales_id` = `s`.`sales_id` LEFT JOIN `warehouse` `wh` ON `wh`.`warehouse_id` = `s`.`warehouse_id` LEFT JOIN `products` `p` ON `p`.`product_id` = `s`.`product_id` LEFT JOIN `channels` `ch` ON `ch`.`sellerId` = `s`.`sellerId` WHERE `s`.`created_by`= {sellerid} AND `ch`.`type`='{channel}' AND ((s.invoice_date between '{start_date}' AND '{end_date}')) AND (s.created_by={sellerid} OR s.created_by= '') order by s.date DESC",
        engine)
    # evanik = evanik[evanik['storeName'].isin(store)]
    if evanik['sellerId'].unique()[0] == None:
        evanik = evanik
    else:
        evanik = evanik[evanik['sellerId'].isin(store)]
    evanik = evanik[evanik['Sale status'] != 'Cancelled']
    evanik = evanik[evanik['Sale status'] != 'Cancel']
    evanik = evanik[evanik['Sale status'] != 'Unshipped']

    evanik['Invoice date '] = pd.to_datetime(evanik['Invoice date '])
    allsales_duplicate = evanik[evanik.duplicated(['Invoice Number'], keep=False)]
    all = pd.DataFrame(allsales_duplicate.groupby(['Invoice Number'], as_index=False).agg(
        {'Grand total': "sum", 'Invoice date ': 'first', 'channel': 'first', 'sellerId': 'first'}))
    evanik.drop_duplicates(subset='Invoice Number', keep=False, inplace=True)
    allsales = evanik.append(all)

    # outer merge = mtr + allsales
    merged = pd.merge(mtr, allsales, on='Invoice Number', how='outer', indicator=True)
    merged.rename(columns={'Invoice Amount': 'MTR_Amount', 'Grand total': 'evanik_Amount'}, inplace=True)

    d = {"left_only": "Only in MTR", "right_only": "Only in eVanik", "both": "both"}
    merged["_merge"] = merged["_merge"].map(d)

    merged['difference'] = merged.MTR_Amount - merged.evanik_Amount
    merged.drop(['channel', 'Sale status'], axis=1, inplace=True)

    m1 = merged[(merged['_merge'] == 'both') & (merged['difference'] != 0)]
    m1.reset_index(inplace=True, drop=True)
    m2 = merged[merged['_merge'] == "Only in MTR"]
    m2.reset_index(inplace=True, drop=True)
    m3 = merged[merged['_merge'] == "Only in eVanik"]
    m3.reset_index(inplace=True, drop=True)

    return m1, m2, m3


@app.route('/amazon/<string:sellerid>/<string:channel>/<string:start_date>/<string:end_date>', methods=['GET', 'POST'])
def amazon(sellerid, channel, start_date, end_date):
    engine = create_engine(f'mysql://root:evanik@2019@172.31.0.81:3306/invento_{sellerid}')
    # engine = sqlalchemy.create_engine('mysql+pymysql://root:evanik@2019@172.31.0.81:3306/invento_78385', pool_recycle=1800)
    df_channels = pd.read_sql_table('channels', engine)
    df_channels = df_channels[['type', 'displayName', 'sellerId']]
    df_channels = df_channels[df_channels['type'] == 'amazon']

    if df_channels['sellerId'].unique()[0] != None:
        stores = df_channels['sellerId'].unique()
    else:
        stores = df_channels['sellerId'].unique()
    # stores = df_channels['sellerId'].unique()

    if request.method == 'POST':
        mtr_b2c = request.files['mtrb2c']
        mtr_b2b = request.files['mtrb2b']
        # month = request.form['month']
        store = request.form.getlist('storesss')

        filename3 = secure_filename(mtr_b2c.filename)
        if filename3 != '':
            file_ext = os.path.splitext(filename3)[1]
            if file_ext not in app.config['UPLOAD_EXTENSIONS']:
                abort(400)

        mtr_b2c.save(os.path.join(app.config['UPLOAD_FOLDER'], filename3))
        mtr_b2c_retrieve = os.path.join(app.config['UPLOAD_FOLDER'], filename3)

        filename4 = secure_filename(mtr_b2b.filename)
        # if filename4 != '':
        #     file_ext = os.path.splitext(filename4)[1]
        #     if file_ext not in app.config['UPLOAD_EXTENSIONS']:
        #         abort(400)

        mtr_b2b.save(os.path.join(app.config['UPLOAD_FOLDER'], filename4))
        mtr_b2b_retrieve = os.path.join(app.config['UPLOAD_FOLDER'], filename4)

        print(store)
        m1, m2, m3 = amazon_match(mtr_b2c_retrieve, mtr_b2b_retrieve, store, engine, sellerid, channel, start_date,
                                  end_date)
        # program ends....
        if os.path.exists(os.path.join(app.config['UPLOAD_FOLDER'], filename3)):
            os.remove(os.path.join(app.config['UPLOAD_FOLDER'], filename3))
        if os.path.exists(os.path.join(app.config['UPLOAD_FOLDER'], filename4)):
            os.remove(os.path.join(app.config['UPLOAD_FOLDER'], filename4))
        return render_template('result.html', data1=[m1.to_html(classes='data')],
                               titles1=m1.columns.values, data2=[m2.to_html(classes='data')],
                               titles2=m2.columns.values, data3=[m3.to_html(classes='data')],
                               titles3=m3.columns.values, ch=channel)

    return render_template('amazon.html', id=sellerid, ch=channel, sd=start_date, ed=end_date, stores=stores)


def flipkart_match(flipkart_file_retrieve, store, engine, sellerid, channel, start_date, end_date):
    flip = pd.read_excel(flipkart_file_retrieve, sheet_name="Sales Report")

    # sale
    sale = flip[flip['Event Sub Type'].isin(['Sale'])]
    sale_df = pd.DataFrame(sale,
                           columns=['Order Item ID', 'Final Invoice Amount (Price after discount+Shipping Charges)',
                                    'Order Date', 'Buyer Invoice Date'])
    sale_df['Order Date'] = pd.to_datetime(sale_df['Order Date'])
    sale_df['Buyer Invoice Date'] = pd.to_datetime(sale_df['Buyer Invoice Date'])
    sale_df.rename(
        columns={"Final Invoice Amount (Price after discount+Shipping Charges)": "Final Invoice Amount - Sale"},
        inplace=True)

    sale_df_duplicated = sale_df[sale_df.duplicated(['Order Item ID'], keep=False)]
    a = sale_df_duplicated.groupby(['Order Item ID'], as_index=False).agg(
        {"Final Invoice Amount - Sale": "sum", 'Order Date': 'first', 'Buyer Invoice Date': 'first'})
    sale_df.drop_duplicates(subset='Order Item ID', keep=False, inplace=True)
    sale_df = sale_df.append(a)

    # cancel
    cancel = flip[flip['Event Sub Type'].isin(["Cancellation"])]
    cancel_df = pd.DataFrame(cancel, columns=['Order Item ID',
                                              'Final Invoice Amount (Price after discount+Shipping Charges)',
                                              'Order Date', 'Buyer Invoice Date'])
    cancel_df['Order Date'] = pd.to_datetime(cancel_df['Order Date'])
    cancel_df['Buyer Invoice Date'] = pd.to_datetime(cancel_df['Buyer Invoice Date'])

    cancel_df.rename(
        columns={"Final Invoice Amount (Price after discount+Shipping Charges)": "Final Invoice Amount - Cancel"},
        inplace=True)

    cancel_df_duplicated = cancel_df[cancel_df.duplicated(['Order Item ID'], keep=False)]
    b = cancel_df_duplicated.groupby(['Order Item ID'], as_index=False).agg(
        {"Final Invoice Amount - Cancel": "sum", 'Order Date': 'first', 'Buyer Invoice Date': 'first'})
    cancel_df.drop_duplicates(subset='Order Item ID', keep=False, inplace=True)
    cancel_df = cancel_df.append(b)

    # apply left merge(sale,cancel)
    left_sale = pd.merge(sale_df, cancel_df, on='Order Item ID', how='left', indicator=True)

    left_sale[["Final Invoice Amount - Sale", "Final Invoice Amount - Cancel"]] = left_sale[
        ["Final Invoice Amount - Sale", "Final Invoice Amount - Cancel"]].fillna(value=0)

    left_sale['difference'] = left_sale["Final Invoice Amount - Sale"] - left_sale["Final Invoice Amount - Cancel"]
    left_sale = left_sale[left_sale['difference'] != 0]

    flipkart = pd.DataFrame(left_sale, columns=['Order Item ID', 'difference', 'Buyer Invoice Date_x'])
    flipkart["Order Item ID"] = flipkart["Order Item ID"].astype(str)
    flipkart["Order Item ID"] = flipkart['Order Item ID'].str.replace("'", '')

    # evanik file
    engine = create_engine(f'mysql://root:evanik@2019@172.31.0.81:3306/invento_{sellerid}')
    df_evanik = pd.read_sql_query(
        f"SELECT `s`.`sellerId` as `sellerId`, `s`.`OrderItemID` as `Order Item ID`, `s`.`sale_status` as `Sale status`, `s`.`grand_total` as `Grand total`, `s`.`invoice_date` as `Invoice date `, `ch`.`type` as `channel`,`ch`.`displayName` as`storeName` FROM `sales` `s` LEFT JOIN `users` `b` ON `s`.`biller_id`=`b`.`id` LEFT JOIN `users` `c` ON `s`.`customer_id`=`c`.`id` LEFT JOIN `invoice` `i` ON `i`.`sales_id` = `s`.`sales_id` LEFT JOIN `sales_items` `si` ON `si`.`sales_id` = `s`.`sales_id` LEFT JOIN `warehouse` `wh` ON `wh`.`warehouse_id` = `s`.`warehouse_id` LEFT JOIN `products` `p` ON `p`.`product_id` = `s`.`product_id` LEFT JOIN `channels` `ch` ON `ch`.`sellerId` = `s`.`sellerId` WHERE `s`.`created_by`= {sellerid} AND `ch`.`type`='{channel}' AND ((s.invoice_date between '{start_date}' AND '{end_date}')) AND (s.created_by={sellerid} OR s.created_by= '') order by s.date DESC",
        engine)
    if df_evanik['sellerId'].unique()[0] == None:
        df_evanik = df_evanik
    else:
        df_evanik = df_evanik[df_evanik['sellerId'].isin(store)]
    print(df_evanik)
    df_evanik = df_evanik[df_evanik['Sale status'] != 'Cancelled']
    df_evanik = df_evanik[df_evanik['Sale status'] != 'Cancellation']
    df_evanik = df_evanik[df_evanik['Sale status'] != 'Hold']
    df_evanik = df_evanik[df_evanik['Sale status'] != 'Cancel']

    df_evanik['Invoice date '] = pd.to_datetime(df_evanik['Invoice date '])
    evanik_df = pd.DataFrame(df_evanik,
                             columns=['Order Item ID', 'Grand total', "Invoice date ", 'storeName', 'sellerId'])
    evanik_df["Order Item ID"] = evanik_df['Order Item ID'].str.replace("'", '')

    evanik_df_duplicated = evanik_df[evanik_df.duplicated(["Order Item ID"], keep=False)]
    ev = evanik_df_duplicated.groupby(['Order Item ID'], as_index=False).agg(
        {'Grand total': "sum", "Invoice date ": 'first', 'storeName': 'first', 'sellerId': 'first'})
    evanik_df.drop_duplicates(subset='Order Item ID', keep=False, inplace=True)
    evanik_df = evanik_df.append(ev)

    # final outer merge (flipkart, evanik)
    merged = pd.merge(flipkart, evanik_df, on='Order Item ID', how='outer', indicator=True)
    d = {"left_only": "Only in flipkart", "right_only": "Only in eVanik", "both": "both"}
    merged["_merge"] = merged["_merge"].map(d)

    merged.rename(columns={'difference': "flipkart amount", 'Buyer Invoice Date_x': 'flipkart Invoice Date',
                           'Grand total': "Evanik amount", "Invoice date ": "Evanik Invoice Date"}, inplace=True)
    merged['difference'] = merged["flipkart amount"] - merged["Evanik amount"]

    m1 = merged[(merged['_merge'] == 'both') & (merged['difference'] != 0)]
    m1.reset_index(inplace=True, drop=True)
    m2 = merged[merged['_merge'] == "Only in flipkart"]
    m2.reset_index(inplace=True, drop=True)
    m3 = merged[merged['_merge'] == "Only in eVanik"]
    m3.reset_index(inplace=True, drop=True)

    return m1, m2, m3


@app.route('/flipkart/<string:sellerid>/<string:channel>/<string:start_date>/<string:end_date>',
           methods=['GET', 'POST'])
def flipkart(sellerid, channel, start_date, end_date):
    engine = create_engine(f'mysql://root:evanik@2019@172.31.0.81:3306/invento_{sellerid}')
    df_channels = pd.read_sql_table('channels', engine)
    df_channels = df_channels[['type', 'displayName', 'sellerId']]
    df_channels = df_channels[df_channels['type'] == 'flipkart']

    if df_channels['sellerId'].unique()[0] != None:
        stores = df_channels['sellerId'].unique()
    else:
        stores = None

    if request.method == 'POST':
        flipkart_file = request.files['filpkartfile']
        store = request.form.getlist('storesss')

        filename2 = secure_filename(flipkart_file.filename)
        if filename2 != '':
            file_ext = os.path.splitext(filename2)[1]
            if file_ext not in app.config['UPLOAD_EXTENSIONS']:
                abort(400)

        flipkart_file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename2))
        flipkart_file_retrieve = os.path.join(app.config['UPLOAD_FOLDER'], filename2)

        m1, m2, m3 = flipkart_match(flipkart_file_retrieve, store, engine, sellerid, channel, start_date, end_date)
        # program ends....
        if os.path.exists(os.path.join(app.config['UPLOAD_FOLDER'], filename2)):
            os.remove(os.path.join(app.config['UPLOAD_FOLDER'], filename2))
        return render_template('result.html', data1=[m1.to_html(classes='data')],
                               titles1=m1.columns.values, data2=[m2.to_html(classes='data')],
                               titles2=m2.columns.values, data3=[m3.to_html(classes='data')],
                               titles3=m3.columns.values, ch=channel)
    return render_template('flipkart.html', id=sellerid, ch=channel, sd=start_date, ed=end_date, stores=stores)


def paytm_match(paytm_file_retrieve, store, engine, sellerid, channel, start_date, end_date):
    # paytm file
    paytm_df = pd.read_csv(paytm_file_retrieve,
                           usecols=['item_taxable_value', 'Order ID', 'Order Item Status', 'Total Price',
                                    'Invoice Generation Date'])

    paytm_df = paytm_df[paytm_df.item_taxable_value > 0]
    paytm_df = paytm_df[paytm_df['Order Item Status'] != 'Merchant Cancelled']
    paytm_df = paytm_df[paytm_df['Order Item Status'] != 'User Cancelled']

    paytm_df['Invoice Generation Date'] = pd.to_datetime(paytm_df['Invoice Generation Date'])
    paytm_df.drop(['Order Item Status', 'item_taxable_value'], axis=1, inplace=True)

    pay_df_duplicated = paytm_df[paytm_df.duplicated(['Order ID'], keep=False)]
    a = pay_df_duplicated.groupby(['Order ID'], as_index=False).agg(
        {'Invoice Generation Date': 'first', "Total Price": "sum"})
    paytm_df.drop_duplicates(subset='Order ID', keep=False, inplace=True)
    paytm_df = paytm_df.append(a)
    paytm_df['Order ID'] = paytm_df['Order ID'].astype('str')
    # paytm file end

    # eVanik file
    engine = create_engine(f'mysql://root:evanik@2019@172.31.0.81:3306/invento_{sellerid}')
    evanik_df = pd.read_sql_query(
        f"SELECT `s`.`sellerId` as `sellerId`, `s`.`OrderId` as `Order id`,`s`.`sale_status` as `Sale status`, `s`.`grand_total` as `Grand total`, `s`.`invoice_date` as `Invoice date `, `ch`.`displayName` as`storeName` FROM `sales` `s` LEFT JOIN `users` `b` ON `s`.`biller_id`=`b`.`id` LEFT JOIN `users` `c` ON `s`.`customer_id`=`c`.`id` LEFT JOIN `invoice` `i` ON `i`.`sales_id` = `s`.`sales_id` LEFT JOIN `sales_items` `si` ON `si`.`sales_id` = `s`.`sales_id` LEFT JOIN `warehouse` `wh` ON `wh`.`warehouse_id` = `s`.`warehouse_id` LEFT JOIN `products` `p` ON `p`.`product_id` = `s`.`product_id` LEFT JOIN `channels` `ch` ON `ch`.`sellerId` = `s`.`sellerId` WHERE `s`.`created_by`= {sellerid} AND `ch`.`type`='{channel}' AND ((s.invoice_date between '{start_date}' AND '{end_date}')) AND (s.created_by={sellerid} OR s.created_by= '') order by s.date DESC",
        engine)
    evanik_df["Invoice date "] = pd.to_datetime(evanik_df["Invoice date "])
    if evanik_df['sellerId'].unique()[0] == None:
        evanik_df = evanik_df
    else:
        evanik_df = evanik_df[evanik_df['sellerId'].isin(store)]
    evanik_df = evanik_df[evanik_df['Sale status'] != 'Merchant Cancelled']
    evanik_df = evanik_df[evanik_df['Sale status'] != 'User Cancelled']

    evanik_df_duplicated = evanik_df[evanik_df.duplicated(['Order id'], keep=False)]
    b = evanik_df_duplicated.groupby(['Order id'], as_index=False).agg(
        {'Invoice date ': 'first', "Grand total": "sum", 'storeName': 'first', 'sellerId': 'first'})
    evanik_df.drop_duplicates(subset='Order id', keep=False, inplace=True)
    evanik_df = evanik_df.append(b)
    evanik_df.drop(['Sale status'], axis=1, inplace=True)
    evanik_df['Order id'] = evanik_df['Order id'].str.replace('OI:', '')
    evanik_df.rename(columns={'Order id': 'Order ID'}, inplace=True)
    # eVanik file end

    # merged file
    merged = pd.merge(paytm_df, evanik_df, on='Order ID', how='outer', indicator=True)
    d = {"left_only": "Only in PayTm", "right_only": "Only in eVanik", "both": "both"}
    merged["_merge"] = merged["_merge"].map(d)

    merged.rename(columns={'Grand total': "eVanik amount", 'Invoice Generation Date': 'PayTm Invoice Date',
                           'Total Price': "PayTm amount", "Invoice date ": "Evanik Invoice Date", }, inplace=True)
    merged['difference'] = merged["PayTm amount"] - merged["eVanik amount"]

    m1 = merged[(merged['_merge'] == 'both') & (merged['difference'] != 0)]
    m1.reset_index(inplace=True, drop=True)
    m2 = merged[merged['_merge'] == "Only in PayTm"]
    m2.reset_index(inplace=True, drop=True)
    m3 = merged[merged['_merge'] == "Only in eVanik"]
    m3.reset_index(inplace=True, drop=True)

    return m1, m2, m3


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
        if filename1 != '':
            file_ext = os.path.splitext(filename1)[1]
            if file_ext not in app.config['UPLOAD_EXTENSIONS']:
                abort(400)

        paytmfile.save(os.path.join(app.config['UPLOAD_FOLDER'], filename1))
        paytm_file_retrieve = os.path.join(app.config['UPLOAD_FOLDER'], filename1)

        m1, m2, m3 = paytm_match(paytm_file_retrieve, store, engine, sellerid, channel, start_date, end_date)
        if os.path.exists(os.path.join(app.config['UPLOAD_FOLDER'], filename1)):
            os.remove(os.path.join(app.config['UPLOAD_FOLDER'], filename1))
        return render_template('result.html', data1=[m1.to_html(classes='data')],
                               titles1=m1.columns.values, data2=[m2.to_html(classes='data')],
                               titles2=m2.columns.values, data3=[m3.to_html(classes='data')],
                               titles3=m3.columns.values, ch=channel)
    return render_template('paytm.html', id=sellerid, ch=channel, sd=start_date, ed=end_date, stores=stores)


@app.route('/file_uploader', methods=['GET', 'POST'])
def file_uploader():
    if request.method == 'POST':
        SELLER_ID = request.form.get('sellerId')
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
        if filename != '':
            file_ext = os.path.splitext(filename)[1]
            if file_ext not in app.config['UPLOAD_EXTENSIONS']:
                abort(400)

        file_upload.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
        file_retrieve = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        df_flip_upload = pd.read_excel(file_retrieve, sheet_name='Sales Report', engine='openpyxl')
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


if __name__ == "__main__":
    app.run(debug=True)
