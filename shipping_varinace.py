import pymysql.cursors


class FlipkartReco:

    def __init__(self, user_id):
        self.user_id = user_id
        self.connection = pymysql.connect('172.31.0.81', 'root', 'evanik@2019', f'invento_{user_id}')
        self.connection_evanik_main = pymysql.connect('172.31.0.81', 'root', 'evanik@2019', 'evanik_main')
        self.cursor = self.connection.cursor()
        self.cursor_evanik_main = self.connection_evanik_main.cursor()
        self.applicable_zone = None
        self.conn = None
        self.data = None
        self.cursor1 = None

    def connect(self):
        self.conn = pymysql.connect('172.31.0.81', 'root', 'evanik@2019', f'invento_{self.user_id}')

    def query(self, sql, val):
        self.connect()
        try:
            self.cursor1 = self.conn.cursor()
            try:
                self.cursor1.execute(sql, val)
            except Exception as e:
                print(e)
            self.conn.commit()
            print(self.cursor1.rowcount, "record(s) affected")
        except (AttributeError, pymysql.err.InterfaceError, pymysql.OperationalError, pymysql.err.OperationalError):
            self.connect()
            self.cursor1 = self.conn.cursor()
            try:
                self.cursor1.execute(sql, val)
            except Exception as e:
                print(e)
            self.conn.commit()
            print(self.cursor1.rowcount, "record(s) affected Success")

        return self.cursor1

    def get_tiers(self, order_date, seller_id):
        """
        This function will take the order_id and seller_id

        :param order_date:
        :param seller_id:
        :return: tier
        """
        try:
            query = "SELECT tier FROM channel_tiers WHERE " + "'" + str(
                order_date) + "'" + " BETWEEN start_date AND end_date and sellerId=" + "'" + seller_id + "'"
            print(query)
            self.cursor.execute(query)
            data = self.cursor.fetchone()
            if data:
                tiers = data[0]
            else:
                tiers = 'bronze'

            return tiers

        except Exception as e:
            print(e)
