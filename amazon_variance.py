import pymysql.cursors
import math
import amazon_calculator
from phpserialize import unserialize


class AmazonReco:

    def __init__(self, user_id, start_date, end_date):
        self.user_id = user_id
        self.start_date = start_date
        self.end_date = end_date
        self.applicable_zone = None
        self.conn = None
        self.data = None

    def connect(self):
        self.conn = pymysql.connect('172.31.0.81', 'root', 'evanik@2019', f'invento_{self.user_id}')

    def query(self, sql, val):
        self.connect()
        try:
            cursor = self.conn.cursor()
            try:
                cursor.execute(sql, val)
            except Exception as e:
                print(e)
            self.conn.commit()
            print(cursor.rowcount, "record(s) affected")
        except (AttributeError, pymysql.err.InterfaceError, pymysql.OperationalError, pymysql.err.OperationalError):
            self.connect()
            cursor = self.conn.cursor()
            try:
                cursor.execute(sql, val)
            except Exception as e:
                print(e)
            self.conn.commit()
            print(cursor.rowcount, "record(s) affected Success")
        finally:
            cursor.close()
            self.conn.close()
        return cursor

    def get_tiers(self, order_date, seller_id):
        """
        This function will take the order_id and seller_id

        :param order_date:
        :param seller_id:
        :return: tier
        """

        connection = pymysql.connect('172.31.0.81', 'root', 'evanik@2019', f'invento_{self.user_id}')
        cursor = connection.cursor()
        query = "SELECT tier FROM channel_tiers WHERE " + "'" + str(
            order_date) + "'" + " BETWEEN start_date AND end_date and sellerId=" + "'" + seller_id + "'"
        cursor.execute(query)
        data = cursor.fetchone()
        cursor.close()
        connection.close()
        if data:
            tiers = data[0]
        else:
            tiers = 'bronze'

        return tiers

    def get_state(self):
        marketplace = 'amazon'
        reco_head = 'Shipping Fee'
        query = f"""
                SELECT s.PinCode AS To_pincode,c.pincode AS From_pincode,ch.whid,s.OrderId,s.warehouse_id,
                s.OrderItemID, s.shippingZone,s.sale_status, s.date, c.type, s.weight, s.shipmentLength,
                s.shipmentBreadth, s.shipmentHeight,p.extra_details,s.sellerId,pr.length,pr.breadth,
                pr.height,pr.weight,c.type,c.pincode, s.total_items FROM sales AS s
                LEFT JOIN channels AS c ON c.sellerId=s.sellerId
                LEFT JOIN channel_warehouse AS ch ON
                ((s.sellerId=ch.sellerId  AND s.whid=ch.whid) OR (s.sellerId=ch.sellerId))
                LEFT JOIN payments_process AS p ON p.OrderId=s.OrderId
                LEFT JOIN products AS pr ON pr.code = s.SKUcode
                WHERE (s.PinCode IS NOT NULL AND s.PinCode!=0
                AND c.type='amazon' AND p.extra_details IS NOT NULL AND
                (s.date BETWEEN '{self.start_date}' and '{self.end_date}')) GROUP BY p.OrderId
            """
        connection = pymysql.connect('172.31.0.81', 'root', 'evanik@2019', f'invento_{self.user_id}')
        cursor = connection.cursor()
        cursor.execute(query)
        data = cursor.fetchall()
        cursor.close()
        connection.close()
        for i in data:
            to_pin = i[0]
            from_pin = i[1]
            whid = i[2]
            order_id = i[3]
            warehouse_id = i[4]
            order_item_id = i[5]
            shipping_zone = i[6]
            sale_status = i[7]
            order_date = i[8]
            channel = i[9]
            qty = i[22]
            if i[10]:
                weight = float(i[10]) / 1000.0
                shipment_length = float(i[11])
                shipment_breath = float(i[12])
                shipment_height = float(i[13])

                calculated_weight = (shipment_length * shipment_breath * shipment_height) / 5000
                applied_weight = max(weight, calculated_weight)
                # convert weight to ceil 0.5
                applied_weight = 0.5 * math.ceil(2.0 * float(applied_weight))

            else:
                applied_weight = None

            if i[14]:
                applied_shipping_fee = bytes(i[14], 'utf-8')
                output = unserialize(applied_shipping_fee)
                try:
                    applied_shipping_fee = dict(output)[b'Commsion'][b'FBA Weight Handling Fee']
                except (KeyError, TypeError):
                    applied_shipping_fee = 0.0
                applied_shipping_fee = abs(float(applied_shipping_fee))
            else:
                applied_shipping_fee = 0.0

            seller_id = i[15]
            length = i[16]
            breadth = i[17]
            height = i[18]
            weight = i[19]
            if not weight:
                weight = 0

            #  To get the tiers

            tiers = self.get_tiers(order_date, seller_id)

            query_to = "Select flipkart_region,District, statename from pincodes where pincode=" + str(to_pin) + ""
            query_from = "Select flipkart_region, District, statename from pincodes where pincode=" + str(from_pin) + ""

            connection_evanik_main = pymysql.connect('172.31.0.81', 'root', 'evanik@2019', 'evanik_main')
            cursor_evanik_main = connection_evanik_main.cursor()

            cursor_evanik_main.execute(query_from)
            data_from = cursor_evanik_main.fetchone()

            from_fk_region = data_from[0]
            from_district = data_from[1]
            from_state = data_from[2]

            cursor_evanik_main.execute(query_to)
            data_to = cursor_evanik_main.fetchone()

            cursor_evanik_main.close()
            connection_evanik_main.close()

            try:
                to_fk_region = data_to[0]
                to_district = data_to[1]
                to_state = data_to[2]
            except:
                to_fk_region = ''
                to_district = ''
                to_state = ''

            #  To get the Zone
            if to_district == from_district:
                self.applicable_zone = 'Local'
            elif (to_district != from_district) and (to_fk_region == from_fk_region):
                self.applicable_zone = 'Zonal'
            else:
                self.applicable_zone = 'National'

            if length and breadth and height:
                calculated_weight_applicable = (float(length) * float(breadth) * float(height)) / 5000
                applicable_weight = max(calculated_weight_applicable, float(weight))
                applicable_weight = (0.5 * math.ceil(2.0 * float(applicable_weight))) * qty
            else:
                applicable_weight = None

            if not applicable_weight:
                try:
                    # getting price from applied_weight and applicable_zone
                    applicable_shipping_fee = amazon_calculator.cal_payment(float(applied_weight),
                                                                            self.applicable_zone, tiers)
                except Exception as e:
                    applicable_shipping_fee = 0
            else:
                try:
                    # getting price from applicable_weight and applicable_zone
                    applicable_shipping_fee = amazon_calculator.cal_payment(float(applicable_weight),
                                                                            self.applicable_zone, tiers)
                except Exception as e:
                    applicable_shipping_fee = 0
            gap = applied_shipping_fee - applicable_shipping_fee

            if (
                    shipping_zone != self.applicable_zone) and sale_status != 'Return':
                print(shipping_zone, order_id, self.applicable_zone, to_district, to_fk_region, to_state, from_district,
                      from_fk_region, from_state, sep='--')

            if shipping_zone or not shipping_zone:
                sql = "INSERT INTO charges_shipping_variance (marketplace, channel, warehouse, order_id, item_id, applied_zone, " \
                      "applicable_zone, applied_fee, applicable_fee, gap, sale_status, payment_status, " \
                      "order_date, applied_weight, applicable_weight, reco_head) " \
                      "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)" \
                      "ON DUPLICATE KEY UPDATE marketplace = %s, channel = %s, warehouse = %s, order_id = %s, item_id = %s, " \
                      "applied_zone = %s, applicable_zone = %s, applied_fee = %s, applicable_fee = %s, " \
                      "gap = %s, sale_status = %s, payment_status = %s, order_date = %s, applied_weight = %s, " \
                      "applicable_weight = %s, reco_head = %s"
                val = (
                    marketplace, channel, whid, order_id, order_item_id, shipping_zone, self.applicable_zone,
                    applied_shipping_fee,
                    applicable_shipping_fee, gap,
                    sale_status, '', order_date, applied_weight, applicable_weight, reco_head, marketplace, channel,
                    whid, order_id,
                    order_item_id, shipping_zone, self.applicable_zone, applied_shipping_fee,
                    applicable_shipping_fee, gap,
                    sale_status, '', order_date, applied_weight, applicable_weight, reco_head)
                self.query(sql, val)
