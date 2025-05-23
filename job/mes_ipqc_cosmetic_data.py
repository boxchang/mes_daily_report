import configparser
import sys
import os
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
import json
from datetime import datetime, timedelta, date
from database import vnedc_database, mes_database, mes_olap_database, lkmes_database, lkmes_olap_database


class MES_IPQC_COSMETIC(object):
    date1 = ""
    date2 = ""

    def __init__(self, date1, date2):
        self.date1 = date1
        self.date2 = date2

        config_file = "..\mes_daily_report.config"
        config = configparser.ConfigParser()
        config.read(config_file, encoding="utf-8")
        self.location = config.get("Settings", "location", fallback=None)

        if self.location in "GD":
            self.mes_db = mes_database()
            self.mes_olap_db = mes_olap_database()
        elif self.location in "LK":
            self.mes_db = lkmes_database()
            self.mes_olap_db = lkmes_olap_database()
        else:
            self.mes_db = None
            self.mes_olap_db = None

    def delete_data(self):
        tmp = ""
        sql1 = f"""
        SELECT id FROM PMG_MES_RunCard r where (((r.InspectionDate = '{self.date1}' AND r.Period BETWEEN 6 AND 23)
                    OR (r.InspectionDate = DATEADD(DAY, 1, '{self.date2}') AND r.Period BETWEEN 0 AND 5))
                    OR (r.InspectionDate between DATEADD(DAY, 1, '{self.date1}') AND '{self.date2}'))
        """
        raws = self.mes_db.select_sql_dict(sql1)
        for raw in raws:
            tmp += f"'{raw['id']}',"

        if len(tmp) > 0:
            tmp = tmp[:-1]

        sql = f"""
            delete from [MES_OLAP].[dbo].[mes_ipqc_cosmetic_data] where 
            runcard in (
                {tmp}
            )
        """

        self.mes_olap_db.execute_sql(sql)

    def insert_data(self):

        sql = f"""
                SELECT PartNo, ProductItem, CustomerCode, CustomerName, CustomerPartNo, 
                  RunCardId, JsonData, ipqc.CreationTime, r.CosmeticInspectionQty
                  FROM [PMGMES].[dbo].[PMG_MES_IPQCInspectingRecord] ipqc
				  JOIN [PMGMES].[dbo].[PMG_MES_RunCard] r on r.Id = ipqc.RunCardId
				  JOIN [PMGMES].[dbo].[PMG_MES_WorkOrder] w on r.WorkOrderId = w.Id
                  WHERE OptionName = 'Cosmetic'
                  AND (((r.InspectionDate = '{self.date1}' AND r.Period BETWEEN 6 AND 23)
                    OR (r.InspectionDate = DATEADD(DAY, 1, '{self.date2}') AND r.Period BETWEEN 0 AND 5))
                    OR (r.InspectionDate between DATEADD(DAY, 1, '{self.date1}') AND '{self.date2}'))
                """
        raws = self.mes_db.select_sql_dict(sql)

        for raw in raws:
            runcard = raw['RunCardId']
            partno = raw['PartNo']
            product_item = raw['ProductItem']
            customer_code = raw['CustomerCode']
            customer_name = raw['CustomerName']
            customer_partno = raw['CustomerPartNo']
            create_time = raw['CreationTime'][:19]
            inspect_qty = raw['CosmeticInspectionQty']
            odata = json.loads(raw['JsonData'])

            if odata['CosmeticDefectQty'] > 0:
                for o in odata['CosmeticDefectCodes']:
                    defect_code = o['DefectCode']
                    qty = o['Qty']

                    sql = """
                    INSERT INTO [MES_OLAP].[dbo].[mes_ipqc_cosmetic_data] 
                        (PartNo, ProductItem, CustomerCode, CustomerName, CustomerPartNo, runcard, defect_code, qty, create_at, insert_at, cosmetic_inspect_qty)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE(), ?)
                    """

                    values = (partno, product_item, customer_code, customer_name, customer_partno, runcard, defect_code, qty, create_time, inspect_qty)

                    self.mes_olap_db.execute_sql_values(sql, values)
            else:
                defect_code = ''
                qty = 0
                sql = """
                                    INSERT INTO [MES_OLAP].[dbo].[mes_ipqc_cosmetic_data] 
                                        (PartNo, ProductItem, CustomerCode, CustomerName, CustomerPartNo, runcard, defect_code, qty, create_at, insert_at, cosmetic_inspect_qty)
                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE(), ?)
                                    """

                values = (
                partno, product_item, customer_code, customer_name, customer_partno, runcard, defect_code, qty,
                create_time, inspect_qty)

                self.mes_olap_db.execute_sql_values(sql, values)


    def convert(self):

        self.delete_data()
        self.insert_data()


report_date1 = datetime.today() - timedelta(days=1)
report_date1 = report_date1.strftime('%Y%m%d')
report_date2 = report_date1
# report_date1 = '20250418'
# report_date2 = '20250419'

obj = MES_IPQC_COSMETIC(report_date1, report_date2)
obj.convert()