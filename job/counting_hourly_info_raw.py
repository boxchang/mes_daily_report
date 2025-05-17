import configparser
import sys
import os

from factory import ConfigObject

curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
from lib.utils import Utils
from database import mes_database, vnedc_database, mes_olap_database, lkmes_database, lkmes_olap_database, \
    lkedc_database
import pandas as pd
from datetime import datetime, timedelta, date
import numpy as np

class Output(object):
    week_range = 15
    stopLimit = 15
    sLimit = 10
    plant = ""

    def __init__(self, location, plant, report_date1, report_date2):
        self.plant = plant
        self.report_date1 = report_date1
        self.report_date2 = report_date2

        self.location = location

        if self.location in "GD":
            self.mes_db = mes_database()
            self.mes_olap_db = mes_olap_database()
            self.vnedc_db = vnedc_database()
        elif self.location in "LK":
            self.mes_db = lkmes_database()
            self.mes_olap_db = lkmes_olap_database()
            self.vnedc_db = lkedc_database()
        else:
            self.mes_db = None
            self.mes_olap_db = None
            self.vnedc_db = None


    def execute(self):
        year, week_no = Utils().get_week_data_df(self.mes_olap_db, self.report_date1)
        plant = self.plant
        start_date = self.report_date1
        end_date = self.report_date2

        # 以點數機資料為主串工單資訊
        self.delete_data(plant, start_date)
        self.sorting_data(year, week_no, plant, start_date, end_date)

    def delete_data(self, plant, report_date):

        sql = f"""
        DELETE FROM counting_hourly_info_raw WHERE belong_to = '{report_date}' AND branch = '{plant}'
        """
        print(sql)
        self.mes_olap_db.execute_sql(sql)

    def shift(self, period):
        try:
            if 6 <= int(period) <= 17:
                return '早班'
            else:
                return '晚班'
        except Exception as ex:
            return ''

    def sorting_data(self, year, week_no, plant, start_date, end_date):

        sPlant = location
        sPlant2 = ""
        sPlant3 = location + plant

        if 'PVC' in plant:
            sPlant2 = 'PVC'
            up_limit = 'UpperSpeed'
            low_limit = 'LowerSpeed'
        elif 'NBR' in plant:
            sPlant2 = 'NBR'
            up_limit = 'UpperLineSpeed_Min'
            low_limit = 'LowerLineSpeed_Min'

        sql = f"""
                    SELECT distinct dml.name
                    FROM [PMGMES].[dbo].[PMG_DML_DataModelList] dml 
                    join [PMG_DEVICE].[dbo].[COUNTING_DATA_MACHINE] cdm
                    on cdm.MES_MACHINE = dml.name
                    where dml.DataModelTypeId= 'DMT000003' 
                    and dml.name like '%{self.plant}%'
               """
        mach_list = self.mes_db.select_sql_dict(sql)

        for item in mach_list:
            mach = item['name']

            # Counting Machine
            sql1 = f"""WITH ConsecutiveStops AS (
                            SELECT MES_MACHINE, MachineName, LINE, CAST(DATEPART(hour, CreationTime) AS INT) AS Period, Qty2 AS Qty, CreationTime AS Cdt,
                                Speed,
                                CASE WHEN Qty2 IS NULL OR Qty2 <= {self.sLimit} THEN 1 ELSE 0 END AS IsStop, --StopLimitHere!
                                ROW_NUMBER() OVER (PARTITION BY MES_MACHINE, LINE ORDER BY CreationTime)
                                - ROW_NUMBER() OVER (PARTITION BY MES_MACHINE, LINE, CASE WHEN Qty2 IS NULL OR Qty2 <= {self.sLimit} THEN 1 ELSE 0 END ORDER BY CreationTime) AS StopGroup  --StopLimitHere!
                            FROM [PMG_DEVICE].[dbo].[COUNTING_DATA] d
                            JOIN [PMG_DEVICE].[dbo].[COUNTING_DATA_MACHINE] m
                                ON d.MachineName = m.COUNTING_MACHINE
                            WHERE m.MES_MACHINE = '{mach}' AND CreationTime BETWEEN CONVERT(DATETIME, '{start_date} 06:00:00', 120) AND CONVERT(DATETIME, '{end_date} 05:59:59', 120)),
                        GroupStats AS (
                            SELECT MES_MACHINE, LINE, StopGroup, COUNT(*) AS StopRowCount, MIN(Cdt) AS StartCdt, MAX(Cdt) AS EndCdt
                            FROM ConsecutiveStops
                            WHERE IsStop = 1
                            GROUP BY MES_MACHINE, LINE, StopGroup)
                        
                        SELECT MES_MACHINE as Machine, LINE as Line, Period, CreationTime as WorkDate, 
                        max(Speed) MaxSpeed, min(Speed) MinSpeed, round(avg(Speed),0) AvgSpeed,
                        sum(Quantity) Quantity, sum(Stop_time) Stop_time FROM ( 
                        SELECT cs.MES_MACHINE, cs.MachineName, cs.LINE, cast(cs.Period as int) as Period, cast(cs.Qty as int) as Quantity, Speed,
                        CONVERT(VARCHAR(10), cs.Cdt, 120) as CreationTime, CASE WHEN gs.StopRowCount > {int(self.stopLimit/5)} THEN 5 ELSE 0 END AS Stop_time
                        FROM ConsecutiveStops cs
                        LEFT JOIN GroupStats gs
                        ON cs.MES_MACHINE = gs.MES_MACHINE AND cs.LINE = gs.LINE AND cs.Cdt BETWEEN gs.StartCdt AND gs.EndCdt) A
                        GROUP BY CreationTime, MES_MACHINE, MachineName, LINE, Period
                        ORDER BY CreationTime, MES_MACHINE, LINE, Period;"""
            print(sql1)
            counting_raws = self.mes_db.select_sql_dict(sql1)
            counting_df = pd.DataFrame(counting_raws)
            counting_df = counting_df.fillna('')
            counting_df['Period'] = counting_df['Period'].astype(str)

            # 抓取工單派工的Runcard
            sql = f"""
                WITH IsolationTable AS (
                    SELECT RunCardId, sum(ActualQty) isolation_qty
                      FROM [PMGMES].[dbo].[PMG_MES_Isolation] i
                      JOIN [PMGMES].[dbo].[PMG_MES_RunCard] r on r.Id = i.RunCardId
                      WHERE  ((r.InspectionDate = '{start_date}' AND r.Period BETWEEN 6 AND 23)
                               OR (r.InspectionDate = '{end_date}' AND r.Period BETWEEN 0 AND 5))
                    group by RunCardId
                )
                
                SELECT
                    wo.Id AS WorkOrder, wo.PartNo, wo.ProductItem, wo.CustomerCode, wo.CustomerName,rc.InspectionDate AS WorkDate, rc.MachineName AS Machine, rc.LineName AS Line,
                    rc.Id as Runcard, rc.period AS Period,  std.{low_limit} AS LowSpeed, std.{up_limit} AS UpSpeed,
                    std.{up_limit} AS StdSpeed,
					SUM(CASE WHEN op.ActualQty IS NOT NULL THEN op.ActualQty ELSE 0 END) AS OnlinePacking,
					SUM(CASE WHEN wp.ActualQty IS NOT NULL THEN wp.ActualQty ELSE 0 END) AS WIPPacking,
                    SUM(CASE WHEN ft.ActualQty IS NOT NULL THEN ft.ActualQty ELSE 0 END) AS FaultyQuantity,
                    SUM(CASE WHEN sp.ActualQty IS NOT NULL THEN sp.ActualQty ELSE 0 END) AS ScrapQuantity,
					MIN(wo.StartDate) AS WoStartDate, 
					MAX(wo.EndDate) AS WoEndDate,
					MAX(ISNULL(op.StandardAQL, wp.StandardAQL)) StandardAQL,
					MAX(ISNULL(op.InspectedAQL, wp.InspectedAQL)) InspectedAQL,
					SUM(iso.isolation_qty) IsolationQty
                FROM [PMGMES].[dbo].[PMG_MES_RunCard] rc
                JOIN [PMGMES].[dbo].[PMG_MES_WorkOrder] wo
                    ON wo.id = rc.WorkOrderId AND wo.StartDate IS NOT NULL
                LEFT JOIN [PMGMES].[dbo].[PMG_MES_{sPlant2}_SCADA_Std] std
                    ON std.PartNo = wo.PartNo
                LEFT JOIN [PMGMES].[dbo].[PMG_MES_Faulty] ft
                    ON ft.RunCardId = rc.id AND ft.WorkOrderId = wo.id
                LEFT JOIN [PMGMES].[dbo].[PMG_MES_Scrap] sp
                    ON sp.RunCardId = rc.id AND sp.WorkOrderId = wo.id
				LEFT JOIN [dbo].[PMG_MES_WorkInProcess] op ON rc.Id = op.RunCardId and op.PackingType = 'OnlinePacking'
				LEFT JOIN [dbo].[PMG_MES_WorkInProcess] wp ON rc.Id = wp.RunCardId and wp.PackingType = 'WIPPacking'
				LEFT JOIN IsolationTable iso on iso.RunCardId = rc.Id
                WHERE rc.MachineName = '{mach}'
                    AND ((rc.InspectionDate = '{start_date}' AND rc.Period BETWEEN 6 AND 23)
                    OR (rc.InspectionDate = '{end_date}' AND rc.Period BETWEEN 0 AND 5))
                GROUP BY wo.Id, wo.PartNo,
                    wo.ProductItem, wo.CustomerCode,wo.CustomerName,rc.InspectionDate, rc.MachineName, rc.LineName, rc.Id, rc.period,
                    std.{low_limit}, std.{up_limit}
                ORDER BY rc.LineName, rc.InspectionDate, CAST(rc.Period AS INT)
                
                """
            print(sql)
            wo_info_raws = self.mes_db.select_sql_dict(sql)
            wo_info_df = pd.DataFrame(wo_info_raws)

            fix_sql = f"""
            SELECT WorkDate, Machine, Line, Period, MinSpeed fix_MinSpeed, MaxSpeed fix_MaxSpeed, AvgSpeed fix_AvgSpeed, CountingQty fix_CountingQty, Target fix_Target, StopTime fix_StopTime, RunTime fix_RunTime
              FROM [MES_OLAP].[dbo].[counting_hourly_info_fix]
              WHERE WorkDate between '{start_date}' and '{end_date}'
            """
            print(fix_sql)
            fix_raws = self.mes_olap_db.select_sql_dict(fix_sql)
            fix_df = pd.DataFrame(fix_raws)

            pitch_sql = f"""
            SELECT Name Machine, 
            CAST(ISNULL(attr1.AttrValue, 1) AS INT) AS std_val, 
            CAST(attr2.AttrValue AS INT) AS act_val, 
            ISNULL(CAST(CAST(attr2.AttrValue AS FLOAT) / CAST(ISNULL(attr1.AttrValue, 1) AS FLOAT) AS FLOAT), 1) AS pitch_rate
			  FROM [PMGMES].[dbo].[PMG_DML_DataModelList] dl
			  LEFT JOIN [PMGMES].[dbo].[PMG_DML_DataModelAttrList] attr1 on dl.Id = attr1.DataModelListId and attr1.AttrName = 'StandardPitch'
			  LEFT JOIN [PMGMES].[dbo].[PMG_DML_DataModelAttrList] attr2 on dl.Id = attr2.DataModelListId and attr2.AttrName = 'ActualPitch'
			  WHERE DataModelTypeId = 'DMT000003' and Name = '{mach}'
            """
            print(pitch_sql)
            pitch_raws = self.mes_db.select_sql_dict(pitch_sql)
            pitch_df = pd.DataFrame(pitch_raws)

            # 離型資料
            if "NBR" in sPlant2:
                dmf_sql = f"""
                SELECT 
                      cd.MES_MACHINE AS Machine
                     ,cd.LINE AS Line
                     ,FORMAT(CreationTime, 'yyyy-MM-dd') AS WorkDate
                     ,CAST(DATEPART(hour, CreationTime) AS INT) AS Period
                     ,Sum(OverShortQty2) OverShortQty
                     ,Sum(OverLongQty2) OverLongQty
                     ,Sum(ModelQty2) ModelQty
                     , 0 AS GRM_Qty
                FROM 
                    [PMG_DEVICE].[dbo].[COUNTING_DATA] c
                JOIN 
                    [PMG_DEVICE].[dbo].[COUNTING_DATA_MACHINE] cd ON c.MachineName = cd.COUNTING_MACHINE
                WHERE CreationTime BETWEEN CONVERT(DATETIME, '{start_date} 06:00:00', 120) 
                  AND CONVERT(DATETIME, '{end_date} 05:59:59', 120)
                  AND MES_MACHINE = '{mach}' and Line in ('A1','A2','B1','B2')
                GROUP BY MES_MACHINE, LINE, FORMAT(CreationTime, 'yyyy-MM-dd'), CAST(DATEPART(hour, CreationTime) AS INT)
                ORDER BY MES_MACHINE, LINE, FORMAT(CreationTime, 'yyyy-MM-dd'), CAST(DATEPART(hour, CreationTime) AS INT)
                """
            elif "PVC" in sPlant2:
                dmf_sql = f"""
                SELECT cd.MES_MACHINE AS Machine
                       ,cd.LINE AS Line
                       ,FORMAT(CreationTime, 'yyyy-MM-dd') AS WorkDate
                       ,CAST(DATEPART(hour, CreationTime) as INT) Period
                       ,0 AS OverShortQty
                       ,0 AS OverLongQty
                       ,Sum(ModelQty2) AS ModelQty
                       ,SUM(Qty2) AS GRM_Qty
                  FROM [PMG_DEVICE].[dbo].[PVC_GRM_DATA] g
                  JOIN [PMG_DEVICE].[dbo].[COUNTING_DATA_MACHINE] cd on g.MachineName = cd.COUNTING_MACHINE
                 WHERE CreationTime BETWEEN CONVERT(DATETIME, '{start_date} 06:00:00', 120) AND CONVERT(DATETIME, '{end_date} 05:59:59', 120)
                   AND cd.MES_MACHINE = '{mach}'
                 GROUP BY cd.MES_MACHINE, cd.LINE, FORMAT(CreationTime, 'yyyy-MM-dd'), CAST(DATEPART(hour, CreationTime) AS INT) 
                    """
            print(dmf_sql)
            dmf_raws = self.mes_db.select_sql_dict(dmf_sql)
            dmf_df = pd.DataFrame(dmf_raws)
            dmf_df['Period'] = dmf_df['Period'].astype(str)

            if not counting_df.empty and not wo_info_df.empty:
                data_df = pd.merge(counting_df, wo_info_df, on=['WorkDate', 'Machine', 'Line', 'Period'], how='left')
                data_df = pd.merge(data_df, pitch_df, on=['Machine'], how='left')
                data_df = pd.merge(data_df, dmf_df, on=['Machine', 'Line', 'WorkDate', 'Period'], how='left')

                # 點數機會有模擬測試的情況，有RunCard才算點數機數量
                data_df["MaxSpeed"] = pd.to_numeric(data_df["MaxSpeed"], errors="coerce")
                data_df.loc[data_df["WorkOrder"].isna() | (data_df["WorkOrder"] == "") | (data_df["MaxSpeed"] < 100), "Quantity"] = 0
                data_df.loc[data_df["WorkOrder"].isna() | (data_df["WorkOrder"] == "") | (data_df["MaxSpeed"] < 100), "Stop_time"] = 60

                data_df["Run_time"] = 60 - data_df["Stop_time"]
                data_df["Run_time"] = data_df["Run_time"].astype(float)
                data_df["StdSpeed"] = data_df["StdSpeed"].astype(float)
                data_df["pitch_rate"] = data_df["pitch_rate"].astype(float)

                # 建立條件 mask：OnlinePacking 或 WIPPacking 有值（非 NaN 且 > 0）
                mask = (data_df["OnlinePacking"].fillna(0) > 0) | (data_df["WIPPacking"].fillna(0) > 0)

                # 預設 Target 為 0
                data_df["Target"] = 0

                # 當條件成立時才計算 Target
                data_df.loc[mask, "Target"] = (
                        60 * data_df["StdSpeed"].fillna(0) / data_df["pitch_rate"].replace(0, np.nan).fillna(1)
                ).replace([np.inf, -np.inf], np.nan).fillna(0).astype(int)

                data_df['WorkOrder'] = data_df['WorkOrder'].fillna('').astype(str)
                data_df['WoStartDate'] = data_df['WoStartDate'].fillna('').astype(str)
                data_df['WoEndDate'] = data_df['WoEndDate'].fillna('').astype(str)
                data_df['PartNo'] = data_df['PartNo'].fillna('').astype(str)
                data_df['ProductItem'] = data_df['ProductItem'].fillna('').astype(str)
                data_df['CustomerCode'] = data_df['CustomerCode'].fillna('').astype(str)
                data_df['CustomerName'] = data_df['CustomerName'].fillna('').astype(str)
                data_df['Runcard'] = data_df['Runcard'].fillna('').astype(str)
                data_df['LowSpeed'] = data_df['LowSpeed'].fillna('').astype(str)
                data_df['UpSpeed'] = data_df['UpSpeed'].fillna('').astype(str)
                data_df['StdSpeed'] = data_df['StdSpeed'].fillna('').astype(str)
                data_df['MinSpeed'] = data_df['MinSpeed'].fillna('').astype(str)
                data_df['MaxSpeed'] = data_df['MaxSpeed'].fillna('').astype(str)
                data_df['AvgSpeed'] = data_df['AvgSpeed'].fillna('').astype(str)
                data_df['OnlinePacking'] = data_df['OnlinePacking'].fillna('').astype(str)
                data_df['WIPPacking'] = data_df['WIPPacking'].fillna('').astype(str)
                data_df['Target'] = data_df['Target'].fillna('').astype(str)
                data_df['FaultyQuantity'] = data_df['FaultyQuantity'].fillna('').astype(str)
                data_df['ScrapQuantity'] = data_df['ScrapQuantity'].fillna('').astype(str)
                data_df['StandardAQL'] = data_df['StandardAQL'].fillna('').astype(str)
                data_df['InspectedAQL'] = data_df['InspectedAQL'].fillna('').astype(str)
                data_df['Shift'] = data_df['Period'].apply(self.shift)
                data_df['IsolationQty'] = data_df['IsolationQty'].fillna('null').astype(str)
                data_df['OverShortQty'] = data_df['OverShortQty'].fillna('null').astype(str)
                data_df['OverLongQty'] = data_df['OverLongQty'].fillna('null').astype(str)
                data_df['ModelQty'] = data_df['ModelQty'].fillna('null').astype(str)
                data_df['GRM_Qty'] = data_df['GRM_Qty'].fillna('null').astype(str)

                # 點數機資料修正
                if not fix_df.empty:
                    fix_df['Period'] = fix_df['Period'].astype(str)
                    data_df = pd.merge(data_df, fix_df, on=['WorkDate', 'Machine', 'Line', 'Period'], how='left')
                    data_df.loc[
                        data_df["fix_CountingQty"].notna(), ["MinSpeed", "MaxSpeed", "AvgSpeed", "Quantity", "Target", "Stop_time", "Run_time"]] = \
                        data_df.loc[data_df["fix_CountingQty"].notna(), ["fix_MinSpeed", "fix_MaxSpeed", "fix_AvgSpeed",
                                                                         "fix_CountingQty", "fix_Target", "fix_StopTime", "fix_RunTime"]].values

                for _, row in data_df.iterrows():
                    try:
                        work_order = row['WorkOrder']
                        wo_start_date = row['WoStartDate'][:19]
                        wo_end_date = row['WoEndDate'][:19]
                        part_no = row['PartNo']
                        product_item = row['ProductItem']
                        customer_code = row['CustomerCode']
                        customer_name = row['CustomerName']
                        work_date = row['WorkDate']
                        machine = row['Machine']
                        line = row['Line']
                        shift = row['Shift']
                        runcard = row['Runcard']
                        period = row['Period']
                        low_speed = row['LowSpeed'] if row['LowSpeed'] != '' else 'null'
                        up_speed = row['UpSpeed'] if row['UpSpeed'] != '' else 'null'
                        std_speed = row['StdSpeed'] if row['StdSpeed'] != '' else 'null'
                        min_speed = row['MinSpeed'] if row['MinSpeed'] != '' else 'null'
                        max_speed = row['MaxSpeed'] if row['MaxSpeed'] != '' else 'null'
                        avg_speed = row['AvgSpeed'] if row['AvgSpeed'] != '' else 'null'
                        run_time = row['Run_time']
                        stop_time = row['Stop_time']
                        counting_qty = row['Quantity'] if row['Quantity'] != '' else 'null'
                        online_packing_qty = row['OnlinePacking'] if row['OnlinePacking'] != '' else 'null'
                        wip_packing_qty = row['WIPPacking'] if row['WIPPacking'] != '' else 'null'
                        target = row['Target'] if row['Target'] != '' else 'null'
                        faulty_qty = row['FaultyQuantity'] if row['FaultyQuantity'] != '' else 'null'
                        scrap_qty = row['ScrapQuantity'] if row['ScrapQuantity'] != '' else 'null'
                        standard_aql = row['StandardAQL'] if row['StandardAQL'] != '' else 'null'
                        inspected_aql = row['InspectedAQL'] if row['InspectedAQL'] != '' else 'null'
                        isolation_qty = row['IsolationQty'] if row['IsolationQty'] != 'null' else 0
                        overshort_qty = row['OverShortQty'] if row['OverShortQty'] != 'null' else 0
                        overlong_qty = row['OverLongQty'] if row['OverLongQty'] != 'null' else 0
                        model_qty = row['ModelQty'] if row['ModelQty'] != 'null' else 0
                        GRM_Qty = row['GRM_Qty'] if row['GRM_Qty'] != 'null' else 0

                        if work_date != '':
                            if int(period) >= 0 and int(period) <= 5:
                                belong_to = (datetime.strptime(work_date, "%Y-%m-%d") - timedelta(days=1)).strftime(
                                    "%Y-%m-%d")
                            else:
                                belong_to = work_date

                        insert_sql = f"""
                        Insert into counting_hourly_info_raw ([Year], Week_No, WorkOrder, WoStartDate, WoEndDate, PartNo, ProductItem, WorkDate, 
                        Machine, Line, Shift, Runcard, Period, LowSpeed, UpSpeed, StdSpeed, MinSpeed, MaxSpeed, AvgSpeed,RunTime, StopTime, CountingQty, OnlinePacking, WIPPacking, Target, FaultyQuantity, ScrapQuantity, 
                        StandardAQL, InspectedAQL, create_at, plant, branch, belong_to, CustomerCode, CustomerName, IsolationQty, ModelQty, OverShortQty, OverLongQty, GRM_Qty)
                        Values({year}, '{week_no}', '{work_order}','{wo_start_date}','{wo_end_date}','{part_no}','{product_item}','{work_date}',
                        '{machine}','{line}', N'{shift}','{runcard}',{period},{low_speed},{up_speed},{std_speed}, 
                        {min_speed},{max_speed},{avg_speed},{run_time}, {stop_time},
                        {counting_qty},{online_packing_qty},{wip_packing_qty},{target},{faulty_qty},{scrap_qty},
                        {standard_aql}, {inspected_aql},
                        GETDATE(), '{sPlant}', '{plant}', '{belong_to}', '{customer_code}', '{customer_name}', {isolation_qty}, {model_qty}, {overshort_qty}, {overlong_qty}, {GRM_Qty})
                        """
                        print(insert_sql)
                        self.mes_olap_db.execute_sql(insert_sql)
                    except Exception as e:
                        print(e)


report_date1 = datetime.today() - timedelta(days=1)
report_date1 = report_date1.strftime('%Y%m%d')

report_date2 = datetime.today()
report_date2 = report_date2.strftime('%Y%m%d')

#report_date1 = '20250511'
#report_date2 = '20250512'

config_file = "..\mes_daily_report.config"
config = configparser.ConfigParser()
config.read(config_file, encoding='utf-8')

location = config['Settings'].get('location')
plants = config['Settings'].get('plants', '').split(',')
fix_start_date = config['Settings'].get('fix_start_date')
fix_end_date = config['Settings'].get('fix_end_date')

if len(fix_start_date) > 0 and len(fix_end_date) > 0:
    report_date1 = fix_start_date
    report_date2 = fix_end_date

for plant in plants:
    output = Output(location, plant, report_date1, report_date2)
    output.execute()