import pymysql.cursors


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
