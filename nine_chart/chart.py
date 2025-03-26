from database import vnedc_database, mes_database
import pandas as pd
from datetime import datetime, timedelta

class Chart(object):
    week_range = 15

    def execute(self):
        pass

    def get_week_date_df(self):
        vnedc_db = vnedc_database()
        today = datetime.now()
        seven_days_ago = (today - timedelta(days=7)).strftime('%Y-%m-%d')

        sql = f"""
            SELECT TOP ({self.week_range}) *
              FROM [MES_OLAP].[dbo].[week_date]
              WHERE CONVERT(DATETIME, '{seven_days_ago}', 120) >= start_date and CONVERT(DATETIME, '{seven_days_ago}', 120) < end_date
              AND enable = 1
              ORDER BY year desc, month desc, month_week
        """

        print(sql)
        raws = vnedc_db.select_sql_dict(sql)
        df = pd.DataFrame(raws)

        return df

    def get_week_date_df_fix(self):
        vnedc_db = vnedc_database()

        sql = f"""
            SELECT *
            FROM [MES_OLAP].[dbo].[week_date] 
            where  [year] = 2025 and [month] =3 and month_week = 'W1'
             and enable = 1
        """

        print(sql)
        raws = vnedc_db.select_sql_dict(sql)
        df = pd.DataFrame(raws)

        return df

    def get_mach_list(self, plant):
        if plant == 'NBR':
            plant_ = 'NBR'
        else:
            plant_ = 'PVC1'

        sql = f"""
                        select name FROM [PMGMES].[dbo].[PMG_DML_DataModelList] 
                        where DataModelTypeId = 'DMT000003' and name like '%{plant_}%' 
                        and name not in ('VN_GD_PVC1_L03','VN_GD_PVC1_L04')
                        order by name
                    """
        data = mes_database().select_sql_dict(sql)

        return data
