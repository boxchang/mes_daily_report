import sys
import os
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
from lib.utils import Utils
from database import mes_database, vnedc_database, mes_olap_database
import pandas as pd
from datetime import datetime, timedelta, date
import numpy as np

class Output(object):
    week_range = 15
    stopLimit = 15
    sLimit = 10
    plant = ""

    def __init__(self, plant):
        self.plant = plant

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


    def execute(self):
        week_date = Utils().get_week_date_df()
        #week_date = self.get_week_date_df_fix()
        for index, row in week_date.iterrows():
            year = row['year']
            month_week = str(row['month']) + row['month_week']
            start_date = row['start_date']
            end_date = row['end_date']

            # 以點數機資料為主串工單資訊
            self.delete_data(year, month_week)
            self.sorting_data(year, month_week, start_date, end_date)

            # 以Runcard儲存IPQC結果
            self.delete_ipqc_data(year, month_week)
            self.ipqc_data(year, month_week, start_date, end_date)


    def delete_data(self, year, month_week):
        mes_olap_db = mes_olap_database()
        sql = f"""
        DELETE FROM counting_daily_info_raw WHERE [Year] = {year}
        AND MonthWeek = '{month_week}' AND branch = '{self.plant}'
        """
        mes_olap_db.execute_sql(sql)

    def shift(self, period):
        try:
            if 6 <= int(period) <= 17:
                return '早班'
            else:
                return '晚班'
        except Exception as ex:
            return ''

    def sorting_data(self, year, month_week, start_date, end_date):
        vnedc_db = vnedc_database()
        mes_db = mes_database()
        mes_olap_db = mes_olap_database()
        sPlant = ""
        sPlant2 = ""
        sPlant3 = ""

        if self.plant == 'LK':
            sPlant = 'LK'
            sPlant2 = 'NBR'
            sPlant3 = 'LKNBR'
            up_limit = 'UpperLineSpeed_Min'
            low_limit = 'LowerLineSpeed_Min'
        elif self.plant == 'GDPVC':
            sPlant = 'GD'
            sPlant2 = 'PVC'
            sPlant3 = 'GDPVC'
            up_limit = 'UpperSpeed'
            low_limit = 'LowerSpeed'
        elif self.plant == 'GDNBR':
            sPlant = 'GD'
            sPlant2 = 'NBR'
            sPlant3 = 'GDNBR'
            up_limit = 'UpperLineSpeed_Min'
            low_limit = 'LowerLineSpeed_Min'

        sql = f"""
                    SELECT distinct dml.name
                    FROM [PMGMES].[dbo].[PMG_DML_DataModelList] dml 
                    join [PMG_DEVICE].[dbo].[COUNTING_DATA_MACHINE] cdm
                    on cdm.MES_MACHINE = dml.name
                    where dml.DataModelTypeId= 'DMT000003' 
                    and dml.name like '%{sPlant2}%'
               """
        mach_list = mes_db.select_sql_dict(sql)

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
                                        WHERE m.MES_MACHINE = '{mach}' AND CreationTime BETWEEN CONVERT(DATETIME, '{start_date} 06:00:00', 120) AND DATEADD(DAY, 1, CONVERT(DATETIME, '{end_date} 05:59:59', 120))),
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
            counting_raws = mes_db.select_sql_dict(sql1)
            counting_df = pd.DataFrame(counting_raws)
            counting_df = counting_df.fillna('')
            counting_df['Period'] = counting_df['Period'].astype(str)

            # 抓取工單派工的Runcard
            sql = f"""
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
					MAX(ISNULL(op.InspectedAQL, wp.InspectedAQL)) InspectedAQL
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
                WHERE rc.MachineName = '{mach}'
                    AND (((rc.InspectionDate = '{start_date}' AND rc.Period BETWEEN 6 AND 23)
                    OR (rc.InspectionDate = DATEADD(DAY, 1, '{end_date}') AND rc.Period BETWEEN 0 AND 5))
                    OR (rc.InspectionDate between DATEADD(DAY, 1, '{start_date}') AND '{end_date}'))
                GROUP BY wo.Id, wo.PartNo,
                    wo.ProductItem, wo.CustomerCode,wo.CustomerName,rc.InspectionDate, rc.MachineName, rc.LineName, rc.Id, rc.period,
                    std.{low_limit}, std.{up_limit}
                ORDER BY rc.LineName, rc.InspectionDate, CAST(rc.Period AS INT)
                
                """
            print(sql)
            wo_info_raws = mes_db.select_sql_dict(sql)
            wo_info_df = pd.DataFrame(wo_info_raws)

            fix_sql = f"""
            SELECT WorkDate, Machine, Line, Period, MinSpeed fix_MinSpeed, MaxSpeed fix_MaxSpeed, AvgSpeed fix_AvgSpeed, CountingQty fix_CountingQty, Target fix_Target, StopTime fix_StopTime, RunTime fix_RunTime
              FROM [MES_OLAP].[dbo].[counting_daily_info_fix]
              WHERE WorkDate between '{start_date}' and '{end_date}'
            """
            print(fix_sql)
            fix_raws = mes_olap_db.select_sql_dict(fix_sql)
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
            pitch_raws = mes_db.select_sql_dict(pitch_sql)
            pitch_df = pd.DataFrame(pitch_raws)


            if not counting_df.empty and not wo_info_df.empty:
                data_df = pd.merge(counting_df, wo_info_df, on=['WorkDate', 'Machine', 'Line', 'Period'], how='left')
                data_df = pd.merge(data_df, pitch_df, on=['Machine'], how='left')

                # 點數機會有模擬測試的情況，有RunCard才算點數機數量
                data_df["MaxSpeed"] = pd.to_numeric(data_df["MaxSpeed"], errors="coerce")
                data_df.loc[data_df["WorkOrder"].isna() | (data_df["WorkOrder"] == "") | (data_df["MaxSpeed"] < 100), "Quantity"] = 0
                data_df.loc[data_df["WorkOrder"].isna() | (data_df["WorkOrder"] == "") | (data_df["MaxSpeed"] < 100), "Stop_time"] = 60

                data_df["Run_time"] = 60 - data_df["Stop_time"]
                data_df["Run_time"] = data_df["Run_time"].astype(float)
                data_df["StdSpeed"] = data_df["StdSpeed"].astype(float)
                data_df["pitch_rate"] = data_df["pitch_rate"].astype(float)

                data_df["Target"] = (
                        data_df["Run_time"].fillna(0) *
                        data_df["StdSpeed"].fillna(0) /
                        data_df["pitch_rate"].replace(0, np.nan).fillna(1)  # 避免除 0
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

                        if work_date != '':
                            if int(period) >= 0 and int(period) <= 5:
                                belong_to = (datetime.strptime(work_date, "%Y-%m-%d") - timedelta(days=1)).strftime(
                                    "%Y-%m-%d")
                            else:
                                belong_to = work_date

                        insert_sql = f"""
                        Insert into counting_daily_info_raw ([Year], MonthWeek, WorkOrder, WoStartDate, WoEndDate, PartNo, ProductItem, WorkDate, 
                        Machine, Line, Shift, Runcard, Period, LowSpeed, UpSpeed, StdSpeed, MinSpeed, MaxSpeed, AvgSpeed,RunTime, StopTime, CountingQty, OnlinePacking, WIPPacking, Target, FaultyQuantity, ScrapQuantity, 
                        StandardAQL, InspectedAQL,
                        create_at, plant, branch, belong_to, CustomerCode, CustomerName)
                        Values({year}, '{month_week}', '{work_order}','{wo_start_date}','{wo_end_date}','{part_no}','{product_item}','{work_date}',
                        '{machine}','{line}', N'{shift}','{runcard}',{period},{low_speed},{up_speed},{std_speed}, 
                        {min_speed},{max_speed},{avg_speed},{run_time}, {stop_time},
                        {counting_qty},{online_packing_qty},{wip_packing_qty},{target},{faulty_qty},{scrap_qty},
                        {standard_aql}, {inspected_aql},
                        GETDATE(), '{sPlant}', '{sPlant3}', '{belong_to}', '{customer_code}', '{customer_name}')
                        """
                        print(insert_sql)
                        mes_olap_db.execute_sql(insert_sql)
                    except Exception as e:
                        print(e)

    def delete_ipqc_data(self, year, month_week):
        mes_olap_db = mes_olap_database()
        sql = f"""
                DELETE FROM [MES_OLAP].[dbo].[mes_ipqc_data] WHERE [Year] = {year}
                AND MonthWeek = '{month_week}'
                """
        mes_olap_db.execute_sql(sql)

    def ipqc_data(self, year, month_week, start_date, end_date):
        mes_db = mes_database()
        mes_olap_db = mes_olap_database()

        end_date = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)
        end_date = end_date.strftime("%Y-%m-%d")

        sql = f"""
            SELECT ipqc.RunCardId Runcard, ipqc.OptionName, ipqc.InspectionStatus, ipqc.InspectionValue, ipqc.Lower_InspectionValue, ipqc.Upper_InspectionValue, ipqc.DefectCode
              FROM [PMGMES].[dbo].[PMG_MES_IPQCInspectingRecord] ipqc
              JOIN [PMGMES].[dbo].[PMG_MES_RunCard] r on ipqc.RunCardId = r.Id
              WHERE r.InspectionDate between '{start_date}' and '{end_date}'
        """
        ipqc_rows = mes_db.select_sql_dict(sql)
        ipqc_df = pd.DataFrame(ipqc_rows)

        sql = f"""
            SELECT Runcard FROM [MES_OLAP].[dbo].[counting_daily_info_raw]
            WHERE [Year] = {year} AND MonthWeek = '{month_week}' AND Runcard <> ''           
        """
        counting_rows = mes_olap_db.select_sql_dict(sql)
        counting_df = pd.DataFrame(counting_rows)

        data_df = pd.merge(counting_df, ipqc_df, on=['Runcard',], how='left')

        OptionName = ['Weight', 'Width', 'Length', 'Tensile', 'Elongation', 'Roll', 'Cuff', 'Palm', 'Finger', 'FingerTip', 'Pinhole']

        results = []

        for index, row in counting_df.iterrows():
            result = {}
            runcard = row["Runcard"]

            ipqc = {}
            for option in OptionName:

                value = None
                status = None
                lower = None
                upper = None
                defect = None
                ipqc_option_df = data_df[data_df["OptionName"] == option]
                runcard_ipqc_value_df = ipqc_option_df[ipqc_option_df["Runcard"] == runcard]

                if not runcard_ipqc_value_df.empty:
                    value = float(runcard_ipqc_value_df.iloc[0]["InspectionValue"]) if runcard_ipqc_value_df.iloc[0]["InspectionValue"] != None else None
                    status = runcard_ipqc_value_df.iloc[0]["InspectionStatus"]
                    lower = float(runcard_ipqc_value_df.iloc[0]["Lower_InspectionValue"]) if runcard_ipqc_value_df.iloc[0]["Lower_InspectionValue"] else None
                    upper = float(runcard_ipqc_value_df.iloc[0]["Upper_InspectionValue"]) if runcard_ipqc_value_df.iloc[0]["Upper_InspectionValue"] else None
                    defect = runcard_ipqc_value_df.iloc[0]["DefectCode"]
                ipqc[option] = {'value': value, 'status': status, 'lower': lower, 'upper': upper, 'defect': defect}

            tensile_status = '' if ipqc['Tensile']['status'] == None else ipqc['Tensile']['status']
            tensile_value = 0 if ipqc['Tensile']['status'] == None else ipqc['Tensile']['value']
            tensile_limit = '' if ipqc['Tensile']['status'] == None else str(ipqc['Tensile']['lower']) + " ~ " + str(ipqc['Tensile']['upper'])
            tensile_defect = '' if ipqc['Tensile']['defect'] == None else ipqc['Tensile']['defect']

            elongation_value = 0 if ipqc['Elongation']['status'] == None else ipqc['Elongation']['value']
            elongation_limit = '' if ipqc['Elongation']['status'] == None else str(ipqc['Elongation']['lower']) + " ~ " + str(ipqc['Tensile']['upper'])
            elongation_status = '' if ipqc['Elongation']['status'] == None else ipqc['Elongation']['status']
            elongation_defect = '' if ipqc['Elongation']['defect'] == None else ipqc['Elongation']['defect']

            roll_value = 0 if ipqc['Roll']['status'] == None else ipqc['Roll']['value']
            roll_limit = '' if ipqc['Roll']['status'] == None else str(ipqc['Roll']['lower']) + " ~ " + str(ipqc['Roll']['upper'])
            roll_status = '' if ipqc['Roll']['status'] == None else ipqc['Roll']['status']
            roll_defect = '' if ipqc['Roll']['defect'] == None else ipqc['Roll']['defect']

            cuff_value = 0 if ipqc['Cuff']['status'] == None else ipqc['Cuff']['value']
            cuff_limit = '' if ipqc['Cuff']['status'] == None else str(ipqc['Cuff']['lower']) + " ~ " + str(ipqc['Cuff']['upper'])
            cuff_status = '' if ipqc['Cuff']['status'] == None else ipqc['Cuff']['status']
            cuff_defect = '' if ipqc['Cuff']['defect'] == None else ipqc['Cuff']['defect']

            palm_value = 0 if ipqc['Palm']['status'] == None else ipqc['Palm']['value']
            palm_limit = '' if ipqc['Palm']['status'] == None else str(ipqc['Palm']['lower']) + " ~ " + str(ipqc['Palm']['upper'])
            palm_status = '' if ipqc['Palm']['status'] == None else ipqc['Palm']['status']
            palm_defect = '' if ipqc['Palm']['defect'] == None else ipqc['Palm']['defect']

            finger_value = 0 if ipqc['Finger']['status'] == None else ipqc['Finger']['value']
            finger_limit = '' if ipqc['Finger']['status'] == None else str(ipqc['Finger']['lower']) + " ~ " + str(ipqc['Finger']['upper'])
            finger_status = '' if ipqc['Finger']['status'] == None else ipqc['Finger']['status']
            finger_defect = '' if ipqc['Finger']['defect'] == None else ipqc['Finger']['defect']

            fingerTip_value = 0 if ipqc['FingerTip']['status'] == None else ipqc['FingerTip']['value']
            fingerTip_limit = '' if ipqc['FingerTip']['status'] == None else str(ipqc['FingerTip']['lower']) + " ~ " + str(ipqc['FingerTip']['upper'])
            fingerTip_status = '' if ipqc['FingerTip']['status'] == None else ipqc['FingerTip']['status']
            fingerTip_defect = '' if ipqc['FingerTip']['defect'] == None else ipqc['FingerTip']['defect']

            weight_value = 0 if ipqc['Weight']['status'] == None else ipqc['Weight']['value']
            weight_limit = '' if ipqc['Weight']['status'] == None else str(ipqc['Weight']['lower']) + " ~ " + str(ipqc['Weight']['upper'])
            weight_status = '' if ipqc['Weight']['status'] == None else ipqc['Weight']['status']
            weight_defect = '' if ipqc['Weight']['defect'] == None else ipqc['Weight']['defect']

            length_value = 0 if ipqc['Length']['status'] == None else ipqc['Length']['value']
            length_limit = '' if ipqc['Length']['status'] == None else str(ipqc['Length']['lower']) + " ~ " + str(ipqc['Length']['upper'])
            length_status = '' if ipqc['Length']['status'] == None else ipqc['Length']['status']
            length_defect = '' if ipqc['Length']['defect'] == None else ipqc['Length']['defect']

            width_value = 0 if ipqc['Width']['status'] == None else ipqc['Width']['value']
            width_limit = '' if ipqc['Width']['status'] == None else str(ipqc['Width']['lower']) + " ~ " + str(ipqc['Width']['upper'])
            width_status = '' if ipqc['Width']['status'] == None else ipqc['Width']['status']
            width_defect = '' if ipqc['Width']['defect'] == None else ipqc['Width']['defect']

            pinhole_value = 0 if ipqc['Pinhole']['status'] == None else ipqc['Pinhole']['value']
            pinhole_limit = '' if ipqc['Pinhole']['status'] == None else str(ipqc['Pinhole']['lower']) + " ~ " + str(ipqc['Pinhole']['upper'])
            pinhole_status = '' if ipqc['Pinhole']['status'] == None else ipqc['Pinhole']['status']
            pinhole_defect = '' if ipqc['Pinhole']['defect'] == None else ipqc['Pinhole']['defect']
            cosmetic_value = ''
            cosmetic_status = ''

            sql = f"""
                        Insert into [MES_OLAP].[dbo].[mes_ipqc_data]([Year],MonthWeek,Runcard,
                        Tensile_Value,Tensile_Limit,Tensile_Status,Tensile_Defect,
                        Elongation_Value,Elongation_Limit,Elongation_Status,Elongation_Defect,
                        Roll_Value,Roll_Limit,Roll_Status,Roll_Defect,
                        Cuff_Value,Cuff_Limit,Cuff_Status,Cuff_Defect,
                        Palm_Value,Palm_Limit,Palm_Status,Palm_Defect,
                        Finger_Value,Finger_Limit,Finger_Status,Finger_Defect,
                        FingerTip_Value,FingerTip_Limit,FingerTip_Status,FingerTip_Defect,
                        Weight_Value,Weight_Limit,Weight_Status,Weight_Defect,
                        Length_Value,Length_Limit,Length_Status,Length_Defect,
                        Width_Value,Width_Limit,Width_Status,Width_Defect,
                        Pinhole_Value,Pinhole_Limit,Pinhole_Status,Pinhole_Defect, 
                        create_at)
                        Values({year},'{month_week}','{runcard}',
                        {tensile_value},'{tensile_limit}','{tensile_status}','{tensile_defect}',
                        {elongation_value},'{elongation_limit}','{elongation_status}','{elongation_defect}',
                        {roll_value},'{roll_limit}','{roll_status}','{roll_defect}',
                        {cuff_value},'{cuff_limit}','{cuff_status}','{cuff_defect}',
                        {palm_value},'{palm_limit}','{palm_status}','{palm_defect}',
                        {finger_value},'{finger_limit}','{finger_status}','{finger_defect}',
                        {fingerTip_value},'{fingerTip_limit}','{fingerTip_status}','{fingerTip_defect}',
                        {weight_value},'{weight_limit}','{weight_status}','{weight_defect}',
                        {length_value},'{length_limit}','{length_status}','{length_defect}',
                        {width_value},'{width_limit}','{width_status}','{width_defect}',
                        {pinhole_value},'{pinhole_limit}','{pinhole_status}', '{pinhole_defect}', 
                        GETDATE()
                        )
                    """
            print(sql)
            mes_olap_db.execute_sql(sql)



    def chart1(self):
        pass

    def chart2(self):
        pass

plants = ['GDNBR', 'GDPVC']

for plant in plants:
    output = Output(plant)
    output.execute()