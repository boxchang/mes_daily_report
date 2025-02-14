import sys
import os
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
import json
from datetime import datetime, timedelta, date
from database import vnedc_database, mes_database, mes_olap_database


class ConvertMESJSON(object):
    date1 = ""
    date2 = ""

    def __init__(self, date1, date2):
        self.date1 = date1
        self.date2 = date2

    def delete_data(self):
        mes_olap = mes_olap_database()

        sql = f"""
            delete from [MES_OLAP].[dbo].[mes_ipqc_cosmetic_data] where 
            create_at between CONVERT(DATETIME, '{self.date1} 00:00:00', 120) and CONVERT(DATETIME, '{self.date2} 23:59:59', 120)
        """

        mes_olap.execute_sql(sql)

    def insert_data(self):
        vnedc_db = vnedc_database()
        mes_olap = mes_olap_database()
        mes_db = mes_database()
        sql = f"""
                SELECT PartNo, ProductItem, CustomerCode, CustomerName, CustomerPartNo, 
                  RunCardId, JsonData, ipqc.CreationTime, r.CosmeticInspectionQty
                  FROM [PMGMES].[dbo].[PMG_MES_IPQCInspectingRecord] ipqc
				  JOIN [PMGMES].[dbo].[PMG_MES_RunCard] r on r.Id = ipqc.RunCardId
				  JOIN [PMGMES].[dbo].[PMG_MES_WorkOrder] w on r.WorkOrderId = w.Id
                  WHERE OptionName = 'Cosmetic'
                  AND ipqc.CreationTime between CONVERT(DATETIME, '{self.date1} 00:00:00', 120) and CONVERT(DATETIME, '{self.date2} 23:59:59', 120)
                  AND SUBSTRING(JsonData,CHARINDEX(',',JsonData)-1,1) > 0
                """
        raws = mes_db.select_sql_dict(sql)

        for raw in raws:
            runcard = raw['RunCardId']
            odata = json.loads(raw['JsonData'])
            create_time = raw['CreationTime'][:19]
            inspect_qty = raw['CosmeticInspectionQty']
            partno = raw['PartNo']
            product_item = raw['ProductItem']
            customer_code = raw['CustomerCode']
            customer_name = raw['CustomerName']
            customer_partno = raw['CustomerPartNo']

            for o in odata['CosmeticDefectCodes']:
                defect_code = o['DefectCode']
                qty = o['Qty']

                sql2 = f"""
                        Insert Into [MES_OLAP].[dbo].[mes_ipqc_cosmetic_data] (PartNo, ProductItem, CustomerCode, 
                        CustomerName, CustomerPartNo, runcard, defect_code, qty, create_at, insert_at, cosmetic_inspect_qty)
                        Values('{partno}','{product_item}','{customer_code}','{customer_name}','{customer_partno}','{runcard}', '{defect_code}', {qty}, '{create_time}', GETDATE(), {inspect_qty})
                        """
                print(sql2)
                mes_olap.execute_sql(sql2)


    def execute(self):

        self.delete_data()
        self.insert_data()


report_date1 = datetime.today() - timedelta(days=1)
report_date1 = report_date1.strftime('%Y%m%d')

obj = ConvertMESJSON('20241001', '20250206')
# obj = ConvertMESJSON(report_date1, report_date1)
obj.execute()