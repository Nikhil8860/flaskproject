import pandas as pd
from sqlalchemy import create_engine


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
