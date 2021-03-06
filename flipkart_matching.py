import pandas as pd
from sqlalchemy import create_engine


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
        f"""SELECT `s`.`sellerId` as `sellerId`, `s`.`OrderItemID` as `Order Item ID`, 
            `s`.`sale_status` as `Sale status`, `s`.`grand_total` as `Grand total`, 
            `s`.`invoice_date` as `Invoice date `, `ch`.`type` as `channel`,
            `ch`.`displayName` as`storeName` FROM `sales` `s` LEFT JOIN `users` `b` ON 
            `s`.`biller_id`=`b`.`id` 
            LEFT JOIN `users` `c` ON `s`.`customer_id`=`c`.`id` 
            LEFT JOIN `invoice` `i` ON `i`.`sales_id` = `s`.`sales_id` 
            LEFT JOIN `sales_items` `si` ON `si`.`sales_id` = `s`.`sales_id` 
            LEFT JOIN `warehouse` `wh` ON `wh`.`warehouse_id` = `s`.`warehouse_id` 
            LEFT JOIN `products` `p` ON `p`.`product_id` = `s`.`product_id` 
            LEFT JOIN `channels` `ch` ON `ch`.`sellerId` = `s`.`sellerId` 
            WHERE `s`.`created_by`= {sellerid} AND `ch`.`type`='{channel}' 
            AND ((s.invoice_date between '{start_date}' AND '{end_date}')) 
            AND (s.created_by={sellerid} OR s.created_by= '') order by s.date DESC""",
        engine)
    if not df_evanik['sellerId'].unique()[0] == None:
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
