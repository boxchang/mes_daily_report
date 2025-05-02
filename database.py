import os
import pyodbc
from sqlite3 import Error

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

class mes_olap_database:
    server = '10.13.102.22'
    database = 'MES_OLAP'
    username = 'sa'
    password = '!QAw3ed'
    driver = 'ODBC Driver 17 for SQL Server'  # 根據你的環境選擇合適的 ODBC driver

    def select_sql(self, sql):
        self.conn = self.create_mes_olap_connection()
        self.cur = self.conn.cursor()
        self.cur.execute(sql)
        return self.cur.fetchall()

    def select_sql_dict(self, sql):
        self.conn = self.create_mes_olap_connection()
        self.cur = self.conn.cursor()
        self.cur.execute(sql)

        desc = self.cur.description
        column_names = [col[0] for col in desc]
        data = [dict(zip(column_names, row))
                for row in self.cur.fetchall()]
        return data

    def execute_sql(self, sql):
        self.conn = self.create_mes_olap_connection()
        self.cur = self.conn.cursor()
        self.cur.execute(sql)
        self.conn.commit()

    def execute_sql_values(self, sql, values):
        self.conn = self.create_mes_olap_connection()
        self.cur = self.conn.cursor()
        self.cur.execute(sql, values)
        self.conn.commit()

    def create_mes_olap_connection(self):
        try:
            conn = pyodbc.connect("DRIVER={{SQL Server}};SERVER={server}; database={database}; \
                                   trusted_connection=no;UID={uid};PWD={pwd}".format(server=self.server,
                                                                                     database=self.database,
                                                                                     uid=self.username,
                                                                                     pwd=self.password))
            return conn
        except Error as e:
            print(e)

        return None

class vnedc_database:
    def select_sql(self, sql):
        self.conn = self.create_vnedc_connection()
        self.cur = self.conn.cursor()
        self.cur.execute(sql)
        return self.cur.fetchall()

    def select_sql_dict(self, sql):
        self.conn = self.create_vnedc_connection()
        self.cur = self.conn.cursor()
        self.cur.execute(sql)

        desc = self.cur.description
        column_names = [col[0] for col in desc]
        data = [dict(zip(column_names, row))
                for row in self.cur.fetchall()]
        return data

    def execute_sql(self, sql):
        self.conn = self.create_vnedc_connection()
        self.cur = self.conn.cursor()
        self.cur.execute(sql)
        self.conn.commit()

    def create_vnedc_connection(self):
        try:
            conn = pyodbc.connect("DRIVER={{SQL Server}};SERVER={server}; database={database}; \
                                   trusted_connection=no;UID={uid};PWD={pwd}".format(server="10.13.104.181",
                                                                                     database="VNEDC",
                                                                                     uid="vnedc",
                                                                                     pwd="vnedc#2024"))
            return conn
        except Error as e:
            print(e)

        return None

class scada_database:
    def select_sql(self, sql):
        self.conn = self.create_sgada_connection()
        self.cur = self.conn.cursor()
        self.cur.execute(sql)
        return self.cur.fetchall()

    def select_sql_dict(self, sql):
        self.conn = self.create_sgada_connection()
        self.cur = self.conn.cursor()
        self.cur.execute(sql)

        desc = self.cur.description
        column_names = [col[0] for col in desc]
        data = [dict(zip(column_names, row))
                for row in self.cur.fetchall()]
        return data

    def execute_sql(self, sql):
        self.conn = self.create_sgada_connection()
        self.cur = self.conn.cursor()
        self.cur.execute(sql)
        self.conn.commit()

    def create_sgada_connection(self):
        try:
            conn = pyodbc.connect("DRIVER={{SQL Server}};SERVER={server}; database={database}; \
                                   trusted_connection=no;UID={uid};PWD={pwd}".format(server="10.13.102.22",
                                                                                     database="PMG_DEVICE",
                                                                                     uid="scadauser",
                                                                                     pwd="pmgscada+123"))
            return conn
        except Error as e:
            print(e)

        return None

class tgm_database:
    def select_sql(self, sql):
        self.conn = self.create_sgada_connection()
        self.cur = self.conn.cursor()
        self.cur.execute(sql)
        return self.cur.fetchall()

    def select_sql_dict(self, sql):
        self.conn = self.create_sgada_connection()
        self.cur = self.conn.cursor()
        self.cur.execute(sql)

        desc = self.cur.description
        column_names = [col[0] for col in desc]
        data = [dict(zip(column_names, row))
                for row in self.cur.fetchall()]
        return data

    def execute_sql(self, sql):
        self.conn = self.create_sgada_connection()
        self.cur = self.conn.cursor()
        self.cur.execute(sql)
        self.conn.commit()

    def create_sgada_connection(self):
        try:
            conn = pyodbc.connect("DRIVER={{SQL Server}};SERVER={server}; database={database}; \
                                   trusted_connection=no;UID={uid};PWD={pwd}".format(server="10.13.102.22",
                                                                                     database="TGM",
                                                                                     uid="scadauser",
                                                                                     pwd="pmgscada+123"))
            return conn
        except Error as e:
            print(e)

        return None

class tgm_gdnbr_database:
    def select_sql(self, sql):
        self.conn = self.create_sgada_connection()
        self.cur = self.conn.cursor()
        self.cur.execute(sql)
        return self.cur.fetchall()

    def select_sql_dict(self, sql):
        self.conn = self.create_sgada_connection()
        self.cur = self.conn.cursor()
        self.cur.execute(sql)

        desc = self.cur.description
        column_names = [col[0] for col in desc]
        data = [dict(zip(column_names, row))
                for row in self.cur.fetchall()]
        return data

    def execute_sql(self, sql):
        self.conn = self.create_sgada_connection()
        self.cur = self.conn.cursor()
        self.cur.execute(sql)
        self.conn.commit()

    def create_sgada_connection(self):
        try:
            conn = pyodbc.connect("DRIVER={{SQL Server}};SERVER={server}; database={database}; \
                                   trusted_connection=no;UID={uid};PWD={pwd}".format(server="10.13.102.22",
                                                                                     database="TGM_GDNBR",
                                                                                     uid="scadauser",
                                                                                     pwd="pmgscada+123"))
            return conn
        except Error as e:
            print(e)

        return None

class tgm_gdpvc_database:
    def select_sql(self, sql):
        self.conn = self.create_sgada_connection()
        self.cur = self.conn.cursor()
        self.cur.execute(sql)
        return self.cur.fetchall()

    def select_sql_dict(self, sql):
        self.conn = self.create_sgada_connection()
        self.cur = self.conn.cursor()
        self.cur.execute(sql)

        desc = self.cur.description
        column_names = [col[0] for col in desc]
        data = [dict(zip(column_names, row))
                for row in self.cur.fetchall()]
        return data

    def execute_sql(self, sql):
        self.conn = self.create_sgada_connection()
        self.cur = self.conn.cursor()
        self.cur.execute(sql)
        self.conn.commit()

    def create_sgada_connection(self):
        try:
            conn = pyodbc.connect("DRIVER={{SQL Server}};SERVER={server}; database={database}; \
                                   trusted_connection=no;UID={uid};PWD={pwd}".format(server="10.13.102.22",
                                                                                     database="TGM_GDPVC",
                                                                                     uid="scadauser",
                                                                                     pwd="pmgscada+123"))
            return conn
        except Error as e:
            print(e)

        return None

class mes_database:
    def select_sql(self, sql):
        self.conn = self.create_mes_connection()
        self.cur = self.conn.cursor()
        self.cur.execute(sql)
        return self.cur.fetchall()

    def select_sql_dict(self, sql):
        self.conn = self.create_mes_connection()
        self.cur = self.conn.cursor()
        self.cur.execute(sql)

        desc = self.cur.description
        column_names = [col[0] for col in desc]
        data = [dict(zip(column_names, row))
                for row in self.cur.fetchall()]
        return data

    def execute_sql(self, sql):
        self.conn = self.create_mes_connection()
        self.cur = self.conn.cursor()
        self.cur.execute(sql)
        self.conn.commit()

    def create_mes_connection(self):
        try:
            conn = pyodbc.connect("DRIVER={{SQL Server}};SERVER={server}; database={database}; \
                                   trusted_connection=no;UID={uid};PWD={pwd}".format(server="10.13.102.22",
                                                                                     database="PMGMES",
                                                                                     uid="scadauser",
                                                                                     pwd="pmgscada+123"))
            return conn
        except Error as e:
            print(e)

        return None

class lkmes_database:
    def select_sql(self, sql):
        self.conn = self.create_mes_connection()
        self.cur = self.conn.cursor()
        self.cur.execute(sql)
        return self.cur.fetchall()

    def select_sql_dict(self, sql):
        self.conn = self.create_mes_connection()
        self.cur = self.conn.cursor()
        self.cur.execute(sql)

        desc = self.cur.description
        column_names = [col[0] for col in desc]
        data = [dict(zip(column_names, row))
                for row in self.cur.fetchall()]
        return data

    def execute_sql(self, sql):
        self.conn = self.create_mes_connection()
        self.cur = self.conn.cursor()
        self.cur.execute(sql)
        self.conn.commit()

    def create_mes_connection(self):
        try:
            conn = pyodbc.connect("DRIVER={{SQL Server}};SERVER={server}; database={database}; \
                                   trusted_connection=no;UID={uid};PWD={pwd}".format(server="10.14.102.11",
                                                                                     database="PMGMES",
                                                                                     uid="scadauser",
                                                                                     pwd="pmgscada+123"))
            return conn
        except Error as e:
            print(e)

        return None

class lkmes_olap_database:
    server = '10.14.102.11'
    database = 'MES_OLAP'
    username = 'vnedc'
    password = 'vnedc#2024'
    driver = 'ODBC Driver 17 for SQL Server'  # 根據你的環境選擇合適的 ODBC driver

    def select_sql(self, sql):
        self.conn = self.create_mes_olap_connection()
        self.cur = self.conn.cursor()
        self.cur.execute(sql)
        return self.cur.fetchall()

    def select_sql_dict(self, sql):
        self.conn = self.create_mes_olap_connection()
        self.cur = self.conn.cursor()
        self.cur.execute(sql)

        desc = self.cur.description
        column_names = [col[0] for col in desc]
        data = [dict(zip(column_names, row))
                for row in self.cur.fetchall()]
        return data

    def execute_sql(self, sql):
        self.conn = self.create_mes_olap_connection()
        self.cur = self.conn.cursor()
        self.cur.execute(sql)
        self.conn.commit()

    def execute_sql_values(self, sql, values):
        self.conn = self.create_mes_olap_connection()
        self.cur = self.conn.cursor()
        self.cur.execute(sql, values)
        self.conn.commit()

    def create_mes_olap_connection(self):
        try:
            conn = pyodbc.connect("DRIVER={{SQL Server}};SERVER={server}; database={database}; \
                                   trusted_connection=no;UID={uid};PWD={pwd}".format(server=self.server,
                                                                                     database=self.database,
                                                                                     uid=self.username,
                                                                                     pwd=self.password))
            return conn
        except Error as e:
            print(e)

        return None

class lkedc_database:
    def select_sql(self, sql):
        self.conn = self.create_vnedc_connection()
        self.cur = self.conn.cursor()
        self.cur.execute(sql)
        return self.cur.fetchall()

    def select_sql_dict(self, sql):
        self.conn = self.create_vnedc_connection()
        self.cur = self.conn.cursor()
        self.cur.execute(sql)

        desc = self.cur.description
        column_names = [col[0] for col in desc]
        data = [dict(zip(column_names, row))
                for row in self.cur.fetchall()]
        return data

    def execute_sql(self, sql):
        self.conn = self.create_vnedc_connection()
        self.cur = self.conn.cursor()
        self.cur.execute(sql)
        self.conn.commit()

    def create_vnedc_connection(self):
        try:
            conn = pyodbc.connect("DRIVER={{SQL Server}};SERVER={server}; database={database}; \
                                   trusted_connection=no;UID={uid};PWD={pwd}".format(server="10.14.102.11",
                                                                                     database="LKEDC",
                                                                                     uid="vnedc",
                                                                                     pwd="vnedc#2024"))
            return conn
        except Error as e:
            print(e)

        return None