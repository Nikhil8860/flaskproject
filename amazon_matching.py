from sqlalchemy import create_engine
import pandas as pd


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
