import sys
import os

from openpyxl.formatting.rule import CellIsRule

curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
import time
from matplotlib.ticker import FuncFormatter
import configparser
import ast
from factory import Factory
import numpy as np
from openpyxl.utils import get_column_letter
from database import mes_database, mes_olap_database, lkmes_database, lkmes_olap_database
import pandas as pd
from openpyxl.styles import Alignment, NamedStyle, Font, Border, Side, PatternFill
import logging
from openpyxl.comments import Comment
import matplotlib.pyplot as plt
from io import BytesIO
from datetime import datetime, timedelta, date


class ConfigObject:
    def read_config(self, config_file):
        config = configparser.ConfigParser()
        config.read(config_file, encoding='utf-8')

        self.location = config['Settings'].get('location')
        self.plants = config['Settings'].get('plants', '').split(',')
        self.hour_output_limit = int(config['Settings'].get('hour_output_limit', 0))
        self.fix_mode = config['Settings'].getboolean('fix_mode', False)
        self.report_font = config['Settings'].get('report_font', 'Arial')

    def read_mail_config(self, config_file):
        self.smtp_config = {}
        self.to_emails = []
        self.admin_emails = []

        current_section = None

        with open(config_file, 'r') as file:
            for line in file:
                line = line.strip()

                # Skip empty lines and comments
                if not line or line.startswith('#'):
                    continue

                # Detect section headers
                if line.startswith('[') and line.endswith(']'):
                    current_section = line[1:-1].lower()
                    continue

                # Read lines based on the current section
                if current_section == 'smtp':
                    if '=' in line:
                        key, value = line.split('=', 1)
                        self.smtp_config[key.strip()] = value.strip()
                elif current_section == 'recipients':
                    self.to_emails.append(line)
                elif current_section == 'admin_email':
                    self.admin_emails.append(line)

    def __init__(self, config_file, mail_config_file):
        self.read_config(config_file)
        self.read_mail_config(mail_config_file)


class ColumnControl:
    def __init__(self, name, align, format_style, header_name, font=None, hidden=False, width=None, data_type=None,
                 comment=None, comment_width=None):
        """
        :param name: 欄位名稱 (str)
        :param align: 對齊方式 ('left', 'center', 'right')
        :param format_style: 格式設定，例如 '0.0%'、'#,##0' 等
        :param header_name: 欄位的顯示名稱 (str)
        :param font: openpyxl.styles.Font 物件 (可選)
        :param hidden: 是否隱藏欄位 (bool)
        :param width: 欄位寬度 (float)，如果為 None，則根據內容調整
        :param data_type: 指定資料型別 (`str`, `int`, `float`...)，預設為 None
        :param comment: 欄位的 Excel 註解 (str)，可選
        :param comment_width: 註解的寬度 (int)，預設為 20
        """
        self.name = name
        self.align = align.lower()
        self.format_style = format_style
        self.header_name = header_name
        self.font = font if font else Font(name="Calibri", size=11)
        self.hidden = hidden
        self.width = width
        self.data_type = data_type
        self.comment = comment
        self.comment_width = comment_width

    def get_alignment(self):
        """ 依據 align 設定對齊方式 """
        alignments = {"center": "center", "right": "right", "left": "left"}
        return Alignment(horizontal=alignments.get(self.align, "left"))

    def apply_data_format(self, cell):
        """ 套用字型、對齊、格式與註解 """
        cell.font = self.font
        cell.alignment = self.get_alignment()
        cell.number_format = self.format_style  # 這行設定格式，例如 '@' 為文字
        cell.value = self.convert_value(cell.value)

    def convert_value(self, value):
        """
        依據指定的 data_type 轉換數值
        """
        if self.data_type and value is not None:
            try:
                return self.data_type(value)
            except ValueError:
                return value  # 如果轉換失敗，回傳原始值
        return value

    def __repr__(self):
        return (f"ColumnControl(name='{self.name}', align='{self.align}', format_style='{self.format_style}', "
                f"header_name='{self.header_name}', font={self.font}, hidden={self.hidden}, "
                f"width={self.width}, data_type={self.data_type}, comment={self.comment})")


class DataControl:
    def __init__(self):
        self.columns = []

    def add(self, column):
        if isinstance(column, ColumnControl):
            self.columns.append(column)
            self.header_font = Font(bold=True)
            self.header_alignment = Alignment(horizontal='center')
            self.header_border = Border(bottom=Side(style='thin'))
        else:
            raise TypeError("Only ColumnControl objects can be added.")

    @property
    def column_names(self):
        """ 回傳所有欄位名稱 """
        return [col.name for col in self.columns]

    def apply_header_format(self, cell):
        """對指定的 Excel 單元格應用標題樣式"""
        cell.font = self.header_font
        cell.alignment = self.header_alignment
        cell.border = self.header_border

    @property
    def column_letter(self):
        """
        產生 {欄位名稱: Excel 欄位字母} 對應表
        """
        return {col.name: get_column_letter(i + 1) for i, col in enumerate(self.columns)}

    @property
    def column_index(self):
        """
        產生 {欄位名稱: Excel 欄位順序} 對應表
        """
        return {col.name: i for i, col in enumerate(self.columns)}

    def __repr__(self):
        return f"DataControl(columns={self.columns})"

    @property
    def header_columns(self):
        """
        產生 {欄位名稱: 顯示名稱} 對應表
        """
        return {col.name: col.header_name for col in self.columns}

    def apply_formatting(self, worksheet):
        """
        將字型套用到 Excel Sheet 中的標題列
        """
        for i, col in enumerate(self.columns, start=1):  # Excel 欄位從 1 開始
            col_letter = get_column_letter(i)
            cell = worksheet.cell(row=1, column=i)
            cell.font = col.font  # 套用字型
            self.apply_header_format(cell)

            if col.hidden:  # 隱藏欄位
                worksheet.column_dimensions[col_letter].hidden = True

            if col.width is not None:
                worksheet.column_dimensions[col_letter].width = col.width  # 套用指定欄寬
            else:
                max_length = max(len(str(cell.value)) for cell in worksheet[col_letter][1:])
                worksheet.column_dimensions[col_letter].width = max_length + 5

            if col.comment:  # 如果有提供註解
                comment = Comment(col.comment, "System")
                comment.width = col.comment_width  # 設定註解寬度
                cell.comment = comment

        # 轉換數據型別
        for row in worksheet.iter_rows(min_row=2, max_row=worksheet.max_row):
            for col, cell in zip(self.columns, row):
                col.apply_data_format(cell)

    def __repr__(self):
        return f"DataControl(columns={self.columns})"


class mes_daily_report(object):
    def is_special_date(self, date):
        result = False

        sql = f"""
        SELECT CONTROL_DATE FROM [MES_OLAP].[dbo].[special_date] WHERE job_name = 'mes_daily_report'
        AND CONTROL_DATE = CONVERT(DATE, '{date}', 112)
        """
        raws = self.mes_olap_db.select_sql_dict(sql)

        if len(raws) > 0:
            result = True

        return result

    def __init__(self, report_date1, report_date2):
        config_file = "mes_daily_report.config"
        mail_config_file = 'mes_daily_report_mail.config'

        self.config = ConfigObject(config_file, mail_config_file)
        self.report_date1 = report_date1
        self.report_date2 = report_date2
        if self.config.location in "GD":
            self.mes_db = mes_database()
            self.mes_olap_db = mes_olap_database()
        elif self.config.location in "LK":
            self.mes_db = lkmes_database()
            self.mes_olap_db = lkmes_olap_database()
        else:
            self.mes_db = None
            self.mes_olap_db = None

    def main(self):
        logger = logging.getLogger('UNIQUE_NAME_HERE')
        logging.getLogger()
        logger.setLevel(logging.DEBUG)
        stream_handler = logging.StreamHandler()
        formatter = logging.Formatter('%(message)s')
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

        mes_db = self.mes_db
        mes_olap_db = self.mes_olap_db
        location = self.config.location
        plants = self.config.plants
        hour_output_limit = self.config.hour_output_limit
        fix_mode = self.config.fix_mode
        report_font = self.config.report_font

        print(f"Plant: {plants}, Hour Output Limit: {hour_output_limit}, Fix Mode: {fix_mode}")

        report_date1 = self.report_date1
        report_date2 = self.report_date2

        if self.is_special_date(report_date1):
            sys.exit()

        file_list = []
        image_buffers = []

        # Save Path media/daily_output/
        save_path = os.path.join("daily_output")

        # Check folder to create
        if not os.path.exists(save_path):
            os.makedirs(save_path)

        for plant in plants:
            logger.info(f"{plant} start running......")
            dr = DailyReport(mes_db, mes_olap_db, location, plant, report_date1, report_date2, hour_output_limit,
                             report_font, logger)

            logger.info(f"{plant} precheck......")
            dr.Precheck()

            logger.info(f"{plant} generate_main_df......")
            main_df, cosmetic_df = dr.generate_main_df()

            logger.info(f"{plant} fix_main_df......")
            fixed_main_df = dr.fix_main_df(main_df)

            logger.info(f"{plant} sorting_data......")
            subtotals_df, chart_df, activation_df = dr.sorting_data(fixed_main_df, cosmetic_df)

            logger.info(f"{plant} validate_data......")
            dr.validate_data(fixed_main_df, subtotals_df)

            # Generate Excel file
            logger.info(f"{plant} generate_excel......")
            file_name = f'MES_{location}_{plant}_DAILY_Report_{report_date1}.xlsx'
            excel_file = os.path.join(save_path, file_name)

            dr.generate_excel(fixed_main_df, subtotals_df, activation_df, cosmetic_df, excel_file)
            if os.path.exists(excel_file):
                file_list.append({'file_name': file_name, 'excel_file': excel_file})

            # Generate Chart
            logger.info(f"{plant} generate_chart......")
            image_file = f'MES_{location}_{plant}_bar_chart_{report_date1}.png'
            image_file = os.path.join(save_path, image_file)

            image_buffer = dr.generate_chart(chart_df, image_file)
            image_buffers.append(image_buffer)

        if not fix_mode:
            logger.info(f"{location} send_email......")
            subject = f'[{location} Report] 產量日報表 {self.report_date1}'
            dr.send_email(self.config, subject, file_list, image_buffers, dr.msg_list, dr.error_list)


class DailyReport(Factory):
    file_list = []
    error_list = []
    msg_list = []

    def __init__(self, mes_db, mes_olap_db, location, plant, report_date1, report_date2, hour_output_limit, report_font,
                 logger):
        self.location = location
        self.plant = plant
        self.report_date1 = report_date1
        self.report_date2 = report_date2
        self.hour_output_limit = int(hour_output_limit) if hour_output_limit else 1000
        self.report_font = report_font
        self.mes_db = mes_db
        self.mes_olap_db = mes_olap_db
        self.logger = logger

    def Precheck(self):
        start_time = time.time()

        sql = f"""
            WITH WorkOrder AS (
                select distinct CustomerCode, CustomerName from [dbo].[counting_daily_info_raw]
                where CustomerCode <> '' or CustomerName <> '' and belong_to = '{self.report_date1}'
            )

            SELECT W.CustomerCode, w.CustomerName FROM WorkOrder w
            LEFT JOIN [sap_customer_define] d on w.CustomerCode = d.CustomerCode
            WHERE d.CustomerCode is null
        """
        rows = self.mes_olap_db.select_sql_dict(sql)

        for row in rows:
            customerCode = row['CustomerCode']
            customerName = row['CustomerName']
            msg = f"MES_OLAP mes_defect_define {customerCode} {customerName} 沒有設定銷售地點"
            if msg not in self.error_list:
                self.error_list.append(msg)

        sql = f"""
        SELECT distinct cos.defect_code
          FROM [MES_OLAP].[dbo].[counting_daily_info_raw] r
          LEFT JOIN [MES_OLAP].[dbo].[mes_ipqc_cosmetic_data] cos on r.Runcard = cos.runcard
          LEFT JOIN [MES_OLAP].[dbo].[mes_defect_define] d on d.defect_code = cos.defect_code
          where r.belong_to = '{self.report_date1}' and cos.defect_code <> ''
		  and d.defect_code is null
        """
        rows = self.mes_olap_db.select_sql_dict(sql)

        for row in rows:
            defect_code = row['defect_code']
            msg = f"MES_OLAP mes_defect_define {defect_code} 沒有設定缺點"
            if msg not in self.error_list:
                self.error_list.append(msg)

        end_time = time.time()
        elapsed_time = end_time - start_time
        self.logger.info(f"Time taken: {elapsed_time:.2f} seconds.")

    def get_df_main(self):
        scada_table = ""
        upper_column = ""
        lower_column = ""

        if "NBR" in self.plant:
            upper_column = "UpperLineSpeed_Min"
            lower_column = "LowerLineSpeed_Min"
            scada_table = "[PMG_MES_NBR_SCADA_Std]"
        elif "PVC" in self.plant:
            upper_column = "UpperSpeed"
            lower_column = "LowerSpeed"
            scada_table = "[PMGMES].[dbo].[PMG_MES_PVC_SCADA_Std]"

        report_date1 = self.report_date1
        report_date2 = self.report_date2
        plant = self.plant

        sql = f"""
            WITH WorkOrderInfo AS (
                SELECT 
                    w.MachineId,
                    dl.Name,
                    wi.LineId AS Line,
                    CAST(r.Period AS INT) AS Period,
                    wi.WorkOrderId,
                    wi.StartDate, 
                    wi.EndDate,
                    WorkOrderDate,
                    CustomerName,
                    w.ProductItem,
                    nss.{upper_column} LineSpeedStd,
                    m.COUNTING_MACHINE,
                    w.PartNo,
                    w.AQL,
                    w.PlanQty,
                    wi.Qty,
                    w.Status,
                    ipqc.InspectionStatus weight_result,
                    ipqc.InspectionValue weight_value,
                    ipqc.Lower_InspectionValue weight_lower,
                    ipqc.Upper_InspectionValue weight_upper,
                    hole.InspectionStatus hole_result,
                    r.Id runcard,
                    r.InspectionDate
                FROM 
                    [PMGMES].[dbo].[PMG_MES_WorkOrderInfo] wi
                    JOIN [PMGMES].[dbo].[PMG_MES_WorkOrder] w ON wi.WorkOrderId = w.id
                    JOIN [PMGMES].[dbo].[PMG_DML_DataModelList] dl ON w.MachineId = dl.Id
                    JOIN [PMGMES].[dbo].[PMG_MES_RunCard] r ON r.WorkOrderId = w.Id AND r.LineName = wi.LineId
                    JOIN [PMGMES].[dbo].[PMG_MES_IPQCInspectingRecord] ipqc ON ipqc.RunCardId = r.Id
                    LEFT JOIN [PMGMES].[dbo].[PMG_MES_IPQCInspectingRecord] hole ON hole.RunCardId = r.Id and hole.OptionName = 'Pinhole'
                    LEFT JOIN {scada_table} nss ON nss.PartNo = w.PartNo
                    JOIN [PMG_DEVICE].[dbo].[COUNTING_DATA_MACHINE] m ON dl.Name = m.MES_MACHINE AND wi.LineId = m.LINE
                WHERE 
                    ((r.InspectionDate = '{report_date1}' AND Period between 6 and 23) or (r.InspectionDate = '{report_date2}' AND Period between 0 and 5))
                    AND ipqc.OptionName = 'Weight'
                    AND dl.Name LIKE '%{plant}%' AND COUNTING_MACHINE LIKE '%CountingMachine%'
            ),
            Pitch AS (
                SELECT Name, 
                CAST(ISNULL(attr1.AttrValue, 1) AS FLOAT) AS std_val, 
                CAST(attr2.AttrValue AS FLOAT) AS act_val, 
                ISNULL(CAST(CAST(attr2.AttrValue AS FLOAT) / CAST(ISNULL(attr1.AttrValue, 1) AS FLOAT) AS FLOAT), 1) AS pitch_rate
                  FROM [PMGMES].[dbo].[PMG_DML_DataModelList] dl
                  LEFT JOIN [PMGMES].[dbo].[PMG_DML_DataModelAttrList] attr1 on dl.Id = attr1.DataModelListId and attr1.AttrName = 'StandardPitch'
                  LEFT JOIN [PMGMES].[dbo].[PMG_DML_DataModelAttrList] attr2 on dl.Id = attr2.DataModelListId and attr2.AttrName = 'ActualPitch'
                  WHERE DataModelTypeId = 'DMT000003'
            ),
            Machines as (
                select distinct MES_MACHINE Name from [PMG_DEVICE].[dbo].[COUNTING_DATA] c
                    join [PMG_DEVICE].[dbo].[COUNTING_DATA_MACHINE] m on c.MachineName = m.COUNTING_MACHINE
                    where creationTime between CONVERT(DATETIME, '{report_date1} 06:00:00', 120) AND CONVERT(DATETIME, '{report_date2} 05:59:59', 120)
                    and m.MES_MACHINE like '%{plant}%'
            ),
            Optical as (
                SELECT MES_MACHINE, LINE,CAST(DATEPART(hour, Cdt) AS INT) AS Period, sum(CAST(OKQty AS BIGINT)) OKQty, sum(CAST(NGQty AS BIGINT)) NGQty, 
                CASE 
                         WHEN sum(CAST(NGQty AS BIGINT)) = 0 THEN 0
                         ELSE ROUND(CAST(sum(CAST(NGQty AS BIGINT)) AS FLOAT)/(sum(CAST(OKQty AS BIGINT))+sum(CAST(NGQty AS BIGINT))), 3) 
                       END OpticalNGRate
                  FROM [PMG_DEVICE].[dbo].[OpticalDevice] o
                  JOIN [PMG_DEVICE].[dbo].[COUNTING_DATA_MACHINE] m on o.DeviceId = m.COUNTING_MACHINE
                  where Cdt between CONVERT(DATETIME, '{report_date1} 06:00:00', 120) and CONVERT(DATETIME, '{report_date2} 06:00:00', 120)
                  group by MES_MACHINE, LINE, CAST(DATEPART(hour, Cdt) AS INT)
            ),
            GRM as (
                SELECT  FORMAT(CreationTime, 'yyyy-MM-dd') AS WorkDate,CAST(DATEPART(hour, CreationTime) as INT) Period,
                m.mes_machine Name,m.line Line,sum(Qty2) grm_qty
                  FROM [PMG_DEVICE].[dbo].[PVC_GRM_DATA] d
                  JOIN [PMG_DEVICE].[dbo].[COUNTING_DATA_MACHINE] m on d.MachineName = m.COUNTING_MACHINE 
                  where CreationTime between CONVERT(DATETIME, '{report_date1} 06:00:00', 120) 
                  and CONVERT(DATETIME, '{report_date2} 05:59:59', 120) 
                  group by m.mes_machine,FORMAT(CreationTime, 'yyyy-MM-dd'),DATEPART(hour, CreationTime),m.line
            ),
            Faulty as (
                SELECT r.InspectionDate,r.Period,f.CreationTime,m.Name,f.ActualQty,f.DefectCode1,c.Descr,r.WorkOrderId,r.LineName,r.Id runcardId
                  FROM [PMGMES].[dbo].[PMG_MES_Faulty] f
                  JOIN [PMGMES].[dbo].[PMG_MES_RunCard] r on f.RunCardId = r.Id
                  JOIN [PMGMES].[dbo].[PMG_DML_DataModelList] m on r.MachineId = m.Id
                  LEFT JOIN [PMGMES].[dbo].[PMG_DML_ConstValue] c on c.[Value] = f.DefectCode1
                  where ((r.InspectionDate = '{report_date1}' AND Period between 6 and 23) or (r.InspectionDate = '{report_date2}' AND Period between 0 and 5))
            ),
            Scrap as (
                SELECT r.InspectionDate,r.Period,s.CreationTime,m.Name,s.ActualQty,r.WorkOrderId,r.LineName,r.Id runcardId
                  FROM [PMGMES].[dbo].[PMG_MES_Scrap] s
                  JOIN [PMGMES].[dbo].[PMG_MES_RunCard] r on s.RunCardId = r.Id
                  JOIN [PMGMES].[dbo].[PMG_DML_DataModelList] m on r.MachineId = m.Id
                  where ((r.InspectionDate = '{report_date1}' AND Period between 6 and 23) or (r.InspectionDate = '{report_date2}' AND Period between 0 and 5))
            ),
            WIPPacking as (
                SELECT RunCardId,InspectedAQL,sum(ActualQty) ActualQty from [PMGMES].[dbo].[PMG_MES_WorkInProcess] wip
                JOIN WorkOrderInfo w on w.runcard = wip.RunCardId
                AND PackingType = 'WIPPacking'
                group by RunCardId,InspectedAQL
            ),
            OnlinePacking as (
                SELECT RunCardId,InspectedAQL,sum(ActualQty) ActualQty from [PMGMES].[dbo].[PMG_MES_WorkInProcess] wip
                JOIN WorkOrderInfo w on w.runcard = wip.RunCardId
                AND PackingType = 'OnlinePacking'
                group by RunCardId,InspectedAQL
            ),
            Ticket as (
                SELECT RunCardId,InspectedAQL,sum(ActualQty) ActualQty from [PMGMES].[dbo].[PMG_MES_WorkInProcess] wip
                JOIN WorkOrderInfo w on w.runcard = wip.RunCardId
                group by RunCardId,InspectedAQL
            )

            SELECT 
                wo.MachineId,
                mach.Name,
                wo.Line,
                wo.Period,
                wo.StartDate, 
                wo.EndDate,
                wo.WorkOrderId,
                wo.WorkOrderDate,
                wo.CustomerName,
                wo.PartNo,
                wo.ProductItem,
                wo.AQL,
                t.InspectedAQL,
                wo.PlanQty,
                wo.Qty,
                wo.Status,
                CAST(wo.LineSpeedStd AS FLOAT) AS LineSpeedStd,
                60 AS ProductionTime,
                hole_result Separate,
                ISNULL(s.ActualQty, 0) Scrap,
                ISNULL(f.ActualQty, 0) SecondGrade,
                CAST(60 * wo.LineSpeedStd/pitch_rate AS INT) AS Target,
                weight_result OverControl,
                CAST(round(weight_value,2) AS DECIMAL(10, 2)) WeightValue,
                OpticalNGRate,
                grm_qty,
                CAST(round(weight_lower,2) AS DECIMAL(10, 2)) WeightLower,
                CAST(round(weight_upper,2) AS DECIMAL(10, 2)) WeightUpper,
                runcard,
                wp.ActualQty WIP_Qty,
                op.ActualQty Good_Qty,
                t.ActualQty Ticket_Qty,
                isn.ActualQty Isolation_Qty,
                wo.StartDate WoStartDate, 
                wo.EndDate WoEndDate,
                wo.InspectionDate AS Date
            FROM 
                Machines mach
                LEFT JOIN WorkOrderInfo wo ON mach.Name = wo.Name
                LEFT JOIN Optical o ON wo.Name = o.MES_MACHINE AND wo.Line = o.LINE AND wo.Period = o.Period
                LEFT JOIN GRM grm ON wo.Name = grm.Name AND wo.Line = grm.Line AND wo.Period = grm.Period
                LEFT JOIN Faulty f ON wo.runcard = f.runcardId
                LEFT JOIN Scrap s ON wo.runcard = s.runcardId
                LEFT JOIN WIPPacking wp on wo.runcard = wp.RunCardId
                LEFT JOIN OnlinePacking op on wo.runcard = op.RunCardId
                LEFT JOIN Pitch pc on pc.Name = wo.Name
                LEFT JOIN PMG_MES_Isolation isn on isn.RunCardId = wo.runcard
                LEFT JOIN Ticket t on wo.runcard = t.RunCardId
                WHERE NOT (wo.WorkOrderId IS NOT NULL AND t.ActualQty IS NULL) --有小票才列入計算，主要是User會用錯RunCard，以有小票為主進行統計
            ORDER BY 
                mach.Name, 
                wo.Period, 
                wo.Line;
                """
        raws = self.mes_db.select_sql_dict(sql)

        df_main = pd.DataFrame(raws)

        return df_main

    # Counting Machine Data
    def get_df_detail(self):

        sql = f"""
                        SELECT FORMAT(CreationTime, 'yyyy-MM-dd') AS CountingDate,CAST(DATEPART(hour, CreationTime) as INT) Period ,m.mes_machine Name,m.line Line, max(Speed) max_speed,min(Speed) min_speed,round(avg(Speed),0) avg_speed,sum(Qty2) sum_qty
                          FROM [PMG_DEVICE].[dbo].[COUNTING_DATA] c, [PMG_DEVICE].[dbo].[COUNTING_DATA_MACHINE] m
                          where CreationTime between CONVERT(DATETIME, '{self.report_date1} 06:00:00', 120) and CONVERT(DATETIME, '{self.report_date2} 05:59:59', 120)
                          and c.MachineName = m.counting_machine and m.mes_machine like '%{self.plant}%'
                          group by m.mes_machine,FORMAT(CreationTime, 'yyyy-MM-dd'),DATEPART(hour, CreationTime),m.line
                          order by m.mes_machine,FORMAT(CreationTime, 'yyyy-MM-dd'),DATEPART(hour, CreationTime),m.line
                        """
        detail_raws = self.mes_db.select_sql_dict(sql)
        df_detail = pd.DataFrame(detail_raws)

        return df_detail

    def get_df_ipqc(self):
        sql = f"""
        select c.Runcard runcard,cu.SalePlaceCode
          ,[Tensile_Value]
          ,[Tensile_Limit]
          ,[Tensile_Status]
          ,[Tensile_Defect]
          ,[Elongation_Value]
          ,[Elongation_Limit]
          ,[Elongation_Status]
          ,[Elongation_Defect]
          ,[Roll_Value]
          ,[Roll_Limit]
          ,[Roll_Status]
          ,[Roll_Defect]
          ,[Cuff_Value]
          ,[Cuff_Limit]
          ,[Cuff_Status]
          ,[Cuff_Defect]
          ,[Palm_Value]
          ,[Palm_Limit]
          ,[Palm_Status]
          ,[Palm_Defect]
          ,[Finger_Value]
          ,[Finger_Limit]
          ,[Finger_Status]
          ,[Finger_Defect]
          ,[FingerTip_Value]
          ,[FingerTip_Limit]
          ,[FingerTip_Status]
          ,[FingerTip_Defect]
          ,[Length_Value]
          ,[Length_Limit]
          ,[Length_Status]
          ,[Length_Defect]
          ,[Weight_Value]
          ,[Weight_Limit]
          ,[Weight_Status]
          ,[Weight_Defect]
          ,CASE WHEN Weight_Defect IS NULL THEN NULL WHEN Weight_Defect = 'LL2' THEN 'NG' ELSE 'PASS' END AS Weight_Light
          ,CASE WHEN Weight_Defect IS NULL THEN NULL WHEN Weight_Defect = 'LL1' THEN 'NG' ELSE 'PASS' END AS Weight_Heavy
          ,[Width_Value]
          ,[Width_Limit]
          ,[Width_Status]
          ,[Width_Defect]
          ,[Pinhole_Value]
          ,[Pinhole_Limit]
          ,[Pinhole_Status]
          ,[Pinhole_Defect]
          ,[Cosmetic_Value]
          ,[Cosmetic_Status]
        from counting_daily_info_raw c
        JOIN mes_ipqc_data ipqc on c.Runcard = ipqc.Runcard
        LEFT JOIN sap_customer_define cu on cu.CustomerCode = c.CustomerCode
        where c.belong_to = '{self.report_date1}'
        """
        rows = self.mes_olap_db.select_sql_dict(sql)
        df = pd.DataFrame(rows)

        return df

    def get_df_cosmetic(self):

        sql = f"""    
          SELECT counting.Runcard runcard,counting.belong_to,counting.Machine,counting.Line,counting.Shift,counting.WorkOrder,counting.PartNo,counting.ProductItem,SalePlaceCode,counting.Period
          FROM [MES_OLAP].[dbo].[counting_daily_info_raw] counting
          LEFT JOIN [MES_OLAP].[dbo].[mes_ipqc_data] ipqc on ipqc.Runcard = counting.Runcard
          LEFT JOIN [MES_OLAP].[dbo].[sap_customer_define] cus on cus.CustomerCode = counting.CustomerCode
          where counting.belong_to = '{self.report_date1}'
          and Machine like '%{self.plant}%'
          and (OnlinePacking > 0 or WIPPacking > 0)
          order by Machine, WorkDate, Cast(Period as Int)

        """
        data = self.mes_olap_db.select_sql_dict(sql)
        df = pd.DataFrame(data)

        weight_sql = f"""
        SELECT ipqc.Runcard runcard, Cast(SalePlaceCode as varchar)+Weight_Defect defect_code, 1 qty 
          FROM [MES_OLAP].[dbo].[counting_daily_info_raw] counting
          LEFT JOIN [MES_OLAP].[dbo].[mes_ipqc_data] ipqc on ipqc.Runcard = counting.Runcard
          LEFT JOIN [MES_OLAP].[dbo].[sap_customer_define] cus on cus.CustomerCode = counting.CustomerCode
          where counting.belong_to = '{report_date1}'
          and Weight_Status = 'NG'
          and Machine like '%{self.plant}%'
          and InspectedAQL is not Null
          and cus.CustomerCode is not null

          --6100 美
          --6200 歐
          --6300 日
          --LL1 過重
          --LL2 過輕
        """
        weight_data = self.mes_olap_db.select_sql_dict(weight_sql)
        weight_df = pd.DataFrame(weight_data)

        weight_df = weight_df.pivot(index="runcard", columns="defect_code", values="qty").reset_index()

        cosmetic_sql = f"""
         SELECT r.runcard, d.defect_code, sum(qty) cosmetic_sum_qty, max(cos.cosmetic_inspect_qty) cosmetic_inspect_qty
          FROM [MES_OLAP].[dbo].[counting_daily_info_raw] r
          LEFT JOIN [MES_OLAP].[dbo].[mes_ipqc_cosmetic_data] cos on r.Runcard = cos.runcard
          LEFT JOIN [MES_OLAP].[dbo].[mes_defect_define] d on d.defect_code = cos.defect_code
          where r.belong_to = '{self.report_date1}' and r.runcard<>''
          group by r.runcard, defect_level, d.defect_code, desc2
        """
        cosmetic_data = self.mes_olap_db.select_sql_dict(cosmetic_sql)
        cosmetic_sample_df = pd.DataFrame(cosmetic_data)

        inspect_qty_df = cosmetic_sample_df[['runcard', 'cosmetic_inspect_qty']].drop_duplicates()
        cosmetic_pivot_df = cosmetic_sample_df.pivot(index="runcard", columns="defect_code",
                                                     values="cosmetic_sum_qty").reset_index()

        # 計算針孔Defect Code加總
        pinhole_sql = f"""
        SELECT r.runcard, d.defect_code, sum(qty) pinhole_sum_qty
          FROM [MES_OLAP].[dbo].[counting_daily_info_raw] r
          JOIN [MES_OLAP].[dbo].[mes_ipqc_pinhole_data] cos on r.Runcard = cos.runcard
          JOIN [MES_OLAP].[dbo].[mes_defect_define] d on d.defect_code = cos.defect_code
          where r.belong_to = '{report_date1}'
          group by r.runcard, defect_level, d.defect_code, desc2
        """
        pinhole_data = self.mes_olap_db.select_sql_dict(pinhole_sql)
        pinhole_df = pd.DataFrame(pinhole_data)
        pinhole_pivot_df = pinhole_df.pivot(index="runcard", columns="defect_code",
                                            values="pinhole_sum_qty").reset_index()
        pinhole_pivot_df['Pinhole'] = pinhole_pivot_df.iloc[:, 1:].notna().any(axis=1).astype(int)

        # 計算針孔樣本數
        pinhole_sample_sql = f"""
        SELECT r.Id runcard,WorkCenterTypeName, AQL AS WO_AQL
          FROM [PMGMES].[dbo].[PMG_MES_RunCard] r
          JOIN [PMGMES].[dbo].[PMG_MES_RunCard_IPQCInspectIOptionMapping] m on r.Id = m.RunCardId
          JOIN [PMGMES].[dbo].[PMG_MES_WorkOrder] w on r.WorkOrderId = w.Id
          where GroupType = 'Pinhole' and r.InspectionDate between '{self.report_date1}' and '{self.report_date2}'
        """
        pinhole_sample_data = self.mes_db.select_sql_dict(pinhole_sample_sql)
        pinhole_sample_df = pd.DataFrame(pinhole_sample_data)

        if "PVC" in self.plant:
            pinhole_sample_df['Pinhole_Sample'] = 25
        elif "NBR" in self.plant:
            pinhole_sample_df['Pinhole_Sample'] = np.where(pinhole_sample_df['WO_AQL'] == '1.0', 50, 25)

        df = pd.merge(df, weight_df, on=['runcard'], how='left')
        df = pd.merge(df, cosmetic_sample_df, on=['runcard'], how='left')
        df = pd.merge(df, cosmetic_pivot_df, on=['runcard'], how='left')
        df = pd.merge(df, pinhole_pivot_df, on=['runcard'], how='left')
        df = pd.merge(df, inspect_qty_df, on="runcard", how="left")
        df = pd.merge(df, pinhole_sample_df, on="runcard", how="left")
        df = df.fillna('')

        return df

    def generate_main_df(self):
        start_time = time.time()

        df_main = self.get_df_main()

        df_detail = self.get_df_detail()

        df_ipqc = self.get_df_ipqc()

        final_df = pd.merge(df_main, df_detail, on=['Name', 'Period', 'Line'], how='left')

        final_df = pd.merge(final_df, df_ipqc, on=['runcard'], how='left')

        cosmetic_df = self.get_df_cosmetic()

        end_time = time.time()
        elapsed_time = end_time - start_time
        self.logger.info(f"Time taken: {elapsed_time:.2f} seconds.")

        return final_df, cosmetic_df

    def get_df_fix(self):

        sql = f"""
        SELECT WorkDate CountingDate, Machine Name, Line, Period, MinSpeed, MaxSpeed, AvgSpeed, CountingQty
          FROM [MES_OLAP].[dbo].[counting_daily_info_fix] where 
          WorkDate between '{self.report_date1}' and '{self.report_date2}'
        """

        raws = self.mes_olap_db.select_sql_dict(sql)
        df = pd.DataFrame(raws)

        return df

    def fix_main_df(self, main_df):
        start_time = time.time()

        fix_df = self.get_df_fix()

        if len(fix_df) > 0:
            main_df = pd.merge(main_df, fix_df, on=['CountingDate', 'Name', 'Period', 'Line'], how='left')

            # 點數機資料修正
            main_df.loc[
                main_df["CountingQty"].notna(), ["max_speed", "min_speed", "avg_speed", "sum_qty"]] = \
                main_df.loc[main_df["CountingQty"].notna(), ["MaxSpeed", "MinSpeed", "AvgSpeed", "CountingQty"]].values

        end_time = time.time()
        elapsed_time = end_time - start_time
        self.logger.info(f"Time taken: {elapsed_time:.2f} seconds.")

        return main_df

    def sorting_data(self, df, cosmetic_df):
        start_time = time.time()

        def join_values(col):
            return '/'.join(map(str, sorted(set(col))))

        def min2hour(col):
            sum_val = col.sum()
            data_list = col.tolist()
            hours = sum_val / 60
            return hours

        def counting_ng_ratio(col):
            data_list = col.tolist()
            ng_count = data_list.count('NG')

            # 計算 NG 的比例
            ng_ratio = ng_count / len(data_list)
            return ng_ratio

        def shift(period):
            try:
                if 6 <= int(period) <= 17:
                    return '早班'
                else:
                    return '晚班'
            except Exception as ex:
                return ''

        # Add Column Shift
        df['Shift'] = df['Period'].apply(shift)

        # 設定IPQC欄位判斷條件
        df['IPQC'] = df.apply(lambda row: "" if pd.isna(row['WorkOrderId']) or row['WorkOrderId'] == ""
        else ('NG' if 'NG' in row[['Tensile_Status', 'Elongation_Status', 'Roll_Status', 'Cuff_Status',
                                   'Palm_Status', 'Finger_Status', 'FingerTip_Status', 'Length_Status',
                                   'Weight_Heavy', 'Weight_Light', 'Width_Status', 'Pinhole_Status']].values
              else 'PASS'), axis=1)

        # Drop the 'Period' and 'Date' column from each group
        group_without_period = df.drop(columns=['Period', 'Date'])

        # Data group by 'Name and then calculating
        mach_grouped = group_without_period.groupby(['Name'])

        rows = []
        chart_rows = []
        activation_rows = []

        for mach_name, mach_group in mach_grouped:

            # 處理停機情況
            if not mach_group['ProductItem'].iloc[0]:
                subtotal = {
                    'Name': join_values(mach_group['Name']),
                    'ProductItem': '',
                    'AQL': '',
                    'Shift': '',
                    'Line': '',
                    'max_speed': '',
                    'min_speed': '',
                    'avg_speed': '',
                    'LineSpeedStd': '',
                    'ProductionTime': '',
                    'sum_qty': 0,
                    'Good_Qty': 0,
                    'WIP_Qty': 0,
                    'Isolation_Qty': 0,
                    'grm_qty': 0,
                    'Separate': '',
                    'Scrap': '',
                    'SecondGrade': '',
                    'Target': 0,
                    'OverControl': '',
                }
                subtotal_df = pd.DataFrame([subtotal])
                chart_rows.append(subtotal_df)
                continue

            line_grouped = mach_group.groupby(['Shift', 'Line'])

            tmp_rows = []
            for line_name, line_group in line_grouped:
                line_sum_qty = line_group['sum_qty'].sum()
                line_secondGrade_qty = line_group['SecondGrade'].sum()
                line_second_rate = round(float(line_secondGrade_qty) / line_sum_qty, 3) if line_sum_qty > 0 else 0

                line_scrap_qty = line_group['Scrap'].sum()
                line_scrap_rate = round(float(line_scrap_qty) / line_sum_qty, 3) if line_sum_qty > 0 else 0

                line_sum = {
                    'Name': '',
                    'ProductItem': join_values(line_group['ProductItem']),
                    'AQL': join_values(line_group['AQL']),
                    'Shift': join_values(line_group['Shift']),
                    'Line': join_values(line_group['Line']),
                    'max_speed': line_group['max_speed'].max(),
                    'min_speed': line_group['min_speed'].min(),
                    'avg_speed': line_group['avg_speed'].mean(),
                    'LineSpeedStd': join_values(line_group['LineSpeedStd']),
                    'ProductionTime': min2hour(line_group['ProductionTime']),
                    'sum_qty': line_group['sum_qty'].sum(),
                    'Good_Qty': line_group['Good_Qty'].sum(),
                    'WIP_Qty': line_group['WIP_Qty'].sum(),
                    'Isolation_Qty': line_group['Isolation_Qty'].sum(),
                    'grm_qty': line_group['grm_qty'].sum(),
                    'Separate': counting_ng_ratio(line_group['Separate']),
                    'Scrap': line_scrap_rate,
                    'SecondGrade': line_second_rate,
                    'Target': line_group['Target'].sum(),
                    'OverControl': counting_ng_ratio(line_group['OverControl']),
                }
                line_sum_df = pd.DataFrame([line_sum])

                tmp_rows.append(line_sum_df)

            df_tmp = pd.concat(tmp_rows, ignore_index=True)

            # Sorting Data
            # Day Shift
            day_df = df_tmp[df_tmp['Shift'] == '早班'].copy()
            day_production_time = 0
            night_production_time = 0
            if not day_df.empty:
                subtotal = {
                    'Name': '',
                    'ProductItem': '',
                    'AQL': '',
                    'Shift': join_values(day_df['Shift']),
                    'Line': '',
                    'max_speed': day_df['max_speed'].max(),
                    'min_speed': day_df['min_speed'].min(),
                    'avg_speed': day_df['avg_speed'].mean(),
                    'LineSpeedStd': join_values(day_df['LineSpeedStd']),
                    'ProductionTime': day_df['ProductionTime'].mean(),
                    'sum_qty': day_df['sum_qty'].sum(),
                    'Good_Qty': day_df['Good_Qty'].sum(),
                    'WIP_Qty': day_df['WIP_Qty'].sum(),
                    'Isolation_Qty': day_df['Isolation_Qty'].sum(),
                    'grm_qty': day_df['grm_qty'].sum(),
                    'Separate': day_df['Separate'].mean(),
                    'Scrap': day_df['Scrap'].mean(),
                    'SecondGrade': day_df['SecondGrade'].mean(),
                    'Target': day_df['Target'].sum(),
                    'OverControl': day_df['OverControl'].mean(),
                }
                subtotal_df = pd.DataFrame([subtotal])
                subtotal_df['avg_speed'] = subtotal_df['avg_speed'].round(0)

                day_df[['Name', 'Shift']] = ''
                day_df['avg_speed'] = day_df['avg_speed'].round(0)

                rows.append(day_df)  # Day row data
                rows.append(subtotal_df)  # Day Shift total summary

                if not np.isnan(day_df['ProductionTime'].mean()):
                    day_production_time = day_df['ProductionTime'].mean()

            # Night Shift
            night_df = df_tmp[df_tmp['Shift'] == '晚班'].copy()
            if not night_df.empty:
                subtotal = {
                    'Name': '',
                    'ProductItem': '',
                    'AQL': '',
                    'Shift': join_values(night_df['Shift']),
                    'Line': '',
                    'max_speed': night_df['max_speed'].max(),
                    'min_speed': night_df['min_speed'].min(),
                    'avg_speed': night_df['avg_speed'].mean(),
                    'LineSpeedStd': join_values(night_df['LineSpeedStd']),
                    'ProductionTime': night_df['ProductionTime'].mean(),
                    'sum_qty': night_df['sum_qty'].sum(),
                    'Good_Qty': night_df['Good_Qty'].sum(),
                    'WIP_Qty': night_df['WIP_Qty'].sum(),
                    'Isolation_Qty': night_df['Isolation_Qty'].sum(),
                    'grm_qty': night_df['grm_qty'].sum(),
                    'Separate': night_df['Separate'].mean(),
                    'Scrap': night_df['Scrap'].mean(),
                    'SecondGrade': night_df['SecondGrade'].mean(),
                    'Target': night_df['Target'].sum(),
                    'OverControl': night_df['OverControl'].mean(),
                }
                subtotal_df = pd.DataFrame([subtotal])
                subtotal_df['avg_speed'] = subtotal_df['avg_speed'].round(0)

                night_df[['Name', 'Shift']] = ''
                night_df['avg_speed'] = night_df['avg_speed'].round(0)

                rows.append(night_df)  # Night row data
                rows.append(subtotal_df)  # Night Shift total summary

                if not np.isnan(night_df['ProductionTime'].mean()):
                    night_production_time = night_df['ProductionTime'].mean()

            # Machine total summary
            activation_rate, df_activation_row = self.calculate_activation(mach_name)

            # Second Grade
            sum_qty = mach_group['sum_qty'].sum()
            scrap_qty = mach_group['Scrap'].sum()
            second_qty = mach_group['SecondGrade'].sum()
            target = mach_group['Target'].sum()
            online_qty = mach_group['Good_Qty'].sum()
            isolation_qty = mach_group['Isolation_Qty'].sum()

            activation_rows.append(df_activation_row)

            tmp_scrap = scrap_qty / sum_qty if sum_qty > 0 else 0
            tmp_second = second_qty / sum_qty if sum_qty > 0 else 0

            tmp_qty = sum_qty + scrap_qty + second_qty
            capacity_rate = tmp_qty / target if target > 0 else 0
            yield_rate = (online_qty - isolation_qty) / tmp_qty if tmp_qty > 0 else 0
            isolation_rate = isolation_qty / online_qty if online_qty > 0 else 0
            oee_rate = activation_rate * capacity_rate * yield_rate

            cosmetic_dpm = self.calculate_cosmetic_dpm(mach_group, cosmetic_df)
            pinhole_dpm = self.calculate_pinhole_dpm(mach_group, cosmetic_df)
            mold_lost_rate = self.calculate_mold_lost_rate(mach_name)
            dmf_rate = self.calculate_dmf_rate(mach_name)

            subtotal = {
                'Name': join_values(mach_group['Name']),
                'ProductItem': join_values(mach_group['ProductItem']),
                'AQL': join_values(mach_group['AQL']),
                'Shift': '',
                'Line': '',
                'max_speed': mach_group['max_speed'].max(),
                'min_speed': mach_group['min_speed'].min(),
                'avg_speed': mach_group['avg_speed'].mean(),
                'LineSpeedStd': join_values(mach_group['LineSpeedStd']),
                'ProductionTime': day_production_time + night_production_time,
                'sum_qty': sum_qty,
                'Good_Qty': mach_group['Good_Qty'].sum(),
                'WIP_Qty': mach_group['WIP_Qty'].sum(),
                'Isolation_Qty': mach_group['Isolation_Qty'].sum(),
                'grm_qty': mach_group['grm_qty'].sum(),
                'Separate': counting_ng_ratio(mach_group['Separate']),
                'Scrap': tmp_scrap,
                'SecondGrade': tmp_second,
                'Target': mach_group['Target'].sum(),
                'OverControl': counting_ng_ratio(mach_group['OverControl']),
                'Activation': activation_rate,
                'Capacity': capacity_rate,
                'Yield': yield_rate,
                'OEE': oee_rate,
                'Isolation': isolation_rate,
                'OpticalNGRate': mach_group['OpticalNGRate'].mean(),
                'Cosmetic_DPM': cosmetic_dpm,
                'Pinhole_DPM': pinhole_dpm,
                'Mold_Lost_Rate': mold_lost_rate,  # 缺模率
                'DMF_Rate': dmf_rate,  # 離型不良率
            }
            subtotal_df = pd.DataFrame([subtotal])
            subtotal_df['avg_speed'] = subtotal_df['avg_speed'].round(0)
            rows.append(subtotal_df)  # Machine total summary
            chart_rows.append(subtotal_df)

        # Combine the grouped data into a DataFrame
        with_subtotals_df = pd.concat(rows, ignore_index=True)

        # ProductionTime加上小時的文字
        with_subtotals_df['ProductionTime'] = with_subtotals_df['ProductionTime'].astype(str) + 'H'

        # Group the total quantity of each machine into a DataFrame
        chart_df = pd.concat(chart_rows, ignore_index=True)

        activation_df = pd.concat(activation_rows, ignore_index=True)

        end_time = time.time()
        elapsed_time = end_time - start_time
        self.logger.info(f"Time taken: {elapsed_time:.2f} seconds.")
        return with_subtotals_df, chart_df, activation_df

    def validate_data(self, fixed_main_df, subtotals_df):
        start_time = time.time()
        # 檢查欄位 LineSpeedStd 是否有空值
        df_filtered = fixed_main_df[fixed_main_df['Line'].notnull()]
        isNoStandard = df_filtered['LineSpeedStd'].isnull().any()
        if isNoStandard:
            self.error_list.append(f"有品項尚未維護標準值，無法計算目標產量")

        # 生產時間不可能超過24小時，防呆檢查
        numeric_production_time = subtotals_df['ProductionTime'].str.rstrip('H').astype(float)
        machines_exceeding_24 = subtotals_df.loc[numeric_production_time > 24, 'Name'].unique()
        if machines_exceeding_24.size > 0:
            for machine in machines_exceeding_24:
                for normal_msg in self.msg_list:
                    if machine in normal_msg:
                        break
                    else:
                        self.error_list.append(f"{machine}發生總時數超過24，可能IPQC有用錯RunCard的情況")

        # 判斷是否有用其他方式收貨，要去詢問產線異常原因
        for _, row in fixed_main_df.iterrows():
            if not pd.isna(row['sum_qty']) and not pd.isna(row['Ticket_Qty']):
                if int(row['sum_qty']) < 100 and int(row['Ticket_Qty']) > 1000:
                    abnormal_machine = row['Name']
                    # 判斷正常情況不歸屬點數機異常
                    for normal_msg in self.msg_list:
                        if abnormal_machine in normal_msg:
                            break
                        else:
                            self.error_list.append(f"{abnormal_machine} 點數機資料與SAP入庫資料差異過大，可能發生用舊點數機的情況")

        # 因同時生產兩種尺寸的工單，使用舊點數機人工作業分類，故無法取得正確資料進行計算
        printed_machines = set()
        check_df = fixed_main_df.groupby(['Name', 'Line', 'Date', 'Shift', 'Period'])[
            'ProductItem'].nunique().reset_index()
        conflict_rows = check_df[check_df['ProductItem'] > 1]
        for _, row in conflict_rows.iterrows():
            key = (row['Name'], row['Line'])
            if key not in printed_machines:
                self.msg_list.append(f"{row['Name']} {row['Line']} 邊因同時生產兩種尺寸的工單，使用舊點數機人工作業分類，故無法取得正確資料進行計算。")
                printed_machines.add(key)

        end_time = time.time()
        elapsed_time = end_time - start_time
        self.logger.info(f"Time taken: {elapsed_time:.2f} seconds.")

    def generate_summary_excel(self, writer, df):
        # region 1. Column Define
        font = Font(name=self.report_font, size=10, bold=False)
        sheet = DataControl()
        sheet.add(ColumnControl('Name', 'center', '@', '機台號', font, hidden=False, width=18))
        sheet.add(ColumnControl('ProductItem', 'left', '@', '品項', font, hidden=False, width=30))
        sheet.add(ColumnControl('AQL', 'center', '@', 'AQL', font, hidden=False, width=9))
        sheet.add(ColumnControl('Shift', 'center', '@', '班別', font, hidden=False, width=9))
        sheet.add(ColumnControl('Line', 'center', '@', '線別', font, hidden=False, width=17))
        sheet.add(ColumnControl('max_speed', 'right', '0', '車速(最高)', font, hidden=False, width=10))
        sheet.add(ColumnControl('min_speed', 'right', '0', '車速(最低)', font, hidden=False, width=10))
        sheet.add(ColumnControl('avg_speed', 'right', '0', '車速(平均)', font, hidden=False, width=10))
        sheet.add(ColumnControl('LineSpeedStd', 'right', '0', '標準車速', font, hidden=False, width=10,
                                comment="標準車速上限", comment_width=200))
        sheet.add(ColumnControl('ProductionTime', 'center', '@', '生產時間', font, hidden=False, width=10.5))
        sheet.add(ColumnControl('sum_qty', 'right', '#,##0', '產量(加總)', font, hidden=False, width=12,
                                comment="點數機數量", comment_width=200))
        sheet.add(ColumnControl('Good_Qty', 'right', '#,##0', '包裝確認量', font, hidden=False, width=12))
        sheet.add(ColumnControl('WIP_Qty', 'right', '#,##0', '半成品數量', font, hidden=False, width=12))
        sheet.add(ColumnControl('Isolation_Qty', 'right', '#,##0', '隔離品數量', font, hidden=False, width=12))
        sheet.add(ColumnControl('grm_qty', 'right', '#,##0', '離型不良數量', font, hidden=True, width=12))
        sheet.add(ColumnControl('Separate', 'right', '0.00%', '針孔', font, hidden=False, width=10))
        sheet.add(ColumnControl('Scrap', 'right', '0.00%', '廢品', font, hidden=False, width=10,
                                comment="廢品數量/產量", comment_width=200))
        sheet.add(ColumnControl('SecondGrade', 'right', '0.00%', '二級品', font, hidden=False, width=10,
                                comment="二級品數量/產量", comment_width=200))
        sheet.add(ColumnControl('Target', 'right', '#,##0', '目標產能', font, hidden=False, width=10))
        sheet.add(ColumnControl('OverControl', 'right', '0.00%', '超內控', font, hidden=False, width=10))
        sheet.add(ColumnControl('Activation', 'right', '0.00%', '稼動率', font, hidden=True, width=10,
                                comment="點數機(A1B1)生產時間 / 工單預計生產時間"))
        sheet.add(ColumnControl('Capacity', 'right', '0.00%', '產能效率', font, hidden=False, width=10,
                                comment="(點數機數量+二級品數量+廢品數量) / 目標產能"))
        sheet.add(ColumnControl('Yield', 'right', '0.00%', '良率', font, hidden=False, width=10,
                                comment="(包裝確認量-隔離品數量) / (點數機數量+二級品數量+廢品數量)"))
        sheet.add(ColumnControl('OEE', 'right', '0.00%', 'OEE', font, hidden=True, width=10,
                                comment="稼動率 x 產能效率 x 良率"))
        sheet.add(ColumnControl('Isolation', 'right', '0.00%', '隔離品率', font, hidden=False, width=10,
                                comment="隔離品數量 / 包裝確認量"))
        sheet.add(ColumnControl('OpticalNGRate', 'center', '0.00%', '光檢不良率', font, hidden=False, width=10))
        sheet.add(ColumnControl('Cosmetic_DPM', 'right', '#,##0', '外觀DPM', font, hidden=False, width=10))
        sheet.add(ColumnControl('Pinhole_DPM', 'right', '#,##0', '針孔DPM', font, hidden=False, width=10))
        sheet.add(ColumnControl('Mold_Lost_Rate', 'center', '0.00%', '缺模率', font, hidden=True, width=10))
        sheet.add(ColumnControl('DMF_Rate', 'center', '0.00%', '離型不良率', font, hidden=True, width=10))

        column_index = sheet.column_index
        column_letter = sheet.column_letter
        header_columns = sheet.header_columns
        selected_columns = [col for col in sheet.column_names if col in df.columns]
        # endregion

        # region 2. DataFrame convert to Excel
        df = df[selected_columns].copy()

        # Change column names
        df.rename(columns=header_columns, inplace=True)

        namesheet = "Summary"
        # Write data to the Excel sheet with the machine name as the sheet name
        df.to_excel(writer, sheet_name=namesheet, index=False)

        # Read the written Excel file
        workbook = writer.book
        worksheet = writer.sheets[namesheet]

        sheet.apply_formatting(worksheet)

        # Freeze the first row
        worksheet.freeze_panes = worksheet['A2']
        # endregion

        # region 3. Customize
        bold_font = Font(name=self.report_font, size=10, bold=False)
        thick_border = Border(top=Side(style='thick'), bottom=Side(style='thick'))
        # Search all lines, bold font and bold line above
        index_start = 2
        index_end = 1
        for row in worksheet.iter_rows(min_row=2, max_row=worksheet.max_row):
            if row[column_index['Line']].value != '':  # Line
                for cell in row[column_index['Line']:]:
                    cell.fill = PatternFill(start_color="FDE9D9", end_color="FDE9D9", fill_type="solid")

            if row[column_index['ProductItem']].value != '' and row[column_index['Shift']].value == '':
                index_end += 1

            if row[column_index['Shift']].value != '':  # Shift
                worksheet.row_dimensions.group(index_start, index_end, hidden=True, outline_level=2)

                index_start = index_end + 1
                index_end = index_end + 1
                worksheet.row_dimensions.group(index_start, index_end, hidden=True, outline_level=1)
                index_start = index_end + 1

                for cell in row[column_index['Shift']:]:
                    cell.font = bold_font
                    cell.border = Border(top=Side(style='thin'))

            elif row[column_index['Name']].value != '':  # Machine
                # Hide detailed data
                worksheet.row_dimensions.group(index_start, index_end, hidden=False, outline_level=0)
                index_start = index_end + 1

                for cell in row:
                    cell.font = bold_font
                    cell.border = thick_border

                # Add a note (comment) to the 'Optical' column
                if str(row[column_index['ProductItem']].value).startswith('V S'):
                    note_text = "Yellow Gloves."
                    author = 'System'
                    row[column_index['OpticalNGRate']].comment = Comment(note_text, author)

            # 設置欄的 outlineLevel 讓其可以折疊/展開
            worksheet.column_dimensions[column_letter['Shift']].outlineLevel = 1
            worksheet.column_dimensions[column_letter['Line']].outlineLevel = 1
            worksheet.column_dimensions[column_letter['max_speed']].outlineLevel = 1
            worksheet.column_dimensions[column_letter['min_speed']].outlineLevel = 1

            # 總共折疊的區域
            worksheet.column_dimensions.group(column_letter['Shift'], column_letter['min_speed'], hidden=True)
        # endregion

        return workbook

    def generate_machine_excel(self, writer, df, plant, machine_name):
        # region 1. Column Define
        font = Font(name=self.report_font, size=10, bold=False)
        sheet = DataControl()
        sheet.add(ColumnControl('Date', 'center', '@', '作業日期', font, hidden=False, width=14))
        sheet.add(ColumnControl('Name', 'center', '@', '機台號', font, hidden=False, width=20))
        sheet.add(ColumnControl('Line', 'center', '@', '線別', font, hidden=False, width=9))
        sheet.add(ColumnControl('Shift', 'center', '@', '班別', font, hidden=False, width=9))
        sheet.add(ColumnControl('WorkOrderId', 'center', '@', '工單', font, hidden=False, width=17,
                                comment="IPQC工單", comment_width=200))
        sheet.add(ColumnControl('PartNo', 'center', '@', '料號', font, hidden=False, width=17))
        sheet.add(ColumnControl('ProductItem', 'center', '@', '品項', font, hidden=False, width=25))
        sheet.add(ColumnControl('AQL', 'center', '@', '工單AQL', font, hidden=False, width=9))
        sheet.add(ColumnControl('InspectedAQL', 'center', '@', '量測AQL', font, hidden=False, width=9))
        sheet.add(ColumnControl('SalePlaceCode', 'center', '@', '銷售地點代碼', font, hidden=False, width=9,
                                comment="有派工的工單", comment_width=200))
        sheet.add(ColumnControl('ProductionTime', 'center', '@', '生產時間', font, hidden=False, width=8))
        sheet.add(ColumnControl('Period', 'center', '@', 'Period', font, hidden=False, width=8))
        sheet.add(ColumnControl('max_speed', 'right', '0', '車速(最高)', font, hidden=False, width=9.5))
        sheet.add(ColumnControl('min_speed', 'right', '0', '車速(最低)', font, hidden=False, width=9.5))
        sheet.add(ColumnControl('avg_speed', 'right', '0', '車速(平均)', font, hidden=False, width=9.5))
        sheet.add(ColumnControl('LineSpeedStd', 'right', '0', '標準車速', font, hidden=False, width=9.5))
        sheet.add(ColumnControl('sum_qty', 'right', '#,##0', '產量(加總)', font, hidden=False, width=11))
        sheet.add(ColumnControl('WIP_Qty', 'right', '#,##0', '半成品入庫量', font, hidden=False, width=11))
        sheet.add(ColumnControl('Good_Qty', 'right', '#,##0', '包裝確認量', font, hidden=False, width=11))
        sheet.add(ColumnControl('Separate', 'center', '@', '針孔', font, hidden=False, width=9))
        sheet.add(ColumnControl('Target', 'center', '#,##0', '目標產能', font, hidden=False, width=11,
                                comment="60 * (標準車速上限/節距調整值)"))
        sheet.add(ColumnControl('Scrap', 'center', '0', '廢品', font, hidden=False, width=11))
        sheet.add(ColumnControl('SecondGrade', 'center', '0', '二級品', font, hidden=False, width=11))
        sheet.add(ColumnControl('Isolation_Qty', 'center', '0', '隔離品', font, hidden=True, width=11))
        sheet.add(ColumnControl('grm_qty', 'right', '#,##0', '離型不良數量', font, hidden=True, width=12))
        sheet.add(ColumnControl('OverControl', 'center', '@', '超內控', font, hidden=False, width=9))
        sheet.add(
            ColumnControl('WeightValue', 'center', '0.00', 'IPQC克重', font, hidden=False, width=11, data_type=float))
        sheet.add(ColumnControl('OpticalNGRate', 'center', '0.00%', '光檢不良率', font, hidden=False, width=10))
        sheet.add(ColumnControl('WeightLower', 'center', '0.00', '重量上限', font, hidden=True, width=11))
        sheet.add(ColumnControl('WeightUpper', 'center', '0.00', '重量下限', font, hidden=True, width=11))
        sheet.add(ColumnControl('Tensile_Value', 'center', '0.00', '抗拉強度值', font, hidden=True, width=11))
        sheet.add(ColumnControl('Tensile_Limit', 'center', '@', '抗拉強度上下限', font, hidden=True, width=11))
        sheet.add(ColumnControl('Tensile_Status', 'center', '@', '抗拉強度結果', font, hidden=True, width=11))
        sheet.add(ColumnControl('Elongation_Value', 'center', '0.00', '伸長率值', font, hidden=True, width=11))
        sheet.add(ColumnControl('Elongation_Limit', 'center', '@', '伸長率上下限', font, hidden=True, width=11))
        sheet.add(ColumnControl('Elongation_Status', 'center', '@', '伸長率結果', font, hidden=True, width=11))
        sheet.add(ColumnControl('Roll_Value', 'center', '0.00', '卷唇厚度值', font, hidden=True, width=11))
        sheet.add(ColumnControl('Roll_Limit', 'center', '@', '卷唇厚度上下限', font, hidden=True, width=11))
        sheet.add(ColumnControl('Roll_Status', 'center', '@', '卷唇厚度結果', font, hidden=True, width=11))
        sheet.add(ColumnControl('Cuff_Value', 'center', '0.00', '袖厚度值', font, hidden=True, width=11))
        sheet.add(ColumnControl('Cuff_Limit', 'center', '@', '袖厚度上下限', font, hidden=True, width=11))
        sheet.add(ColumnControl('Cuff_Status', 'center', '@', '袖厚度結果', font, hidden=True, width=11))
        sheet.add(ColumnControl('Palm_Value', 'center', '0.00', '掌厚度值', font, hidden=True, width=11))
        sheet.add(ColumnControl('Palm_Limit', 'center', '@', '掌厚度上下限', font, hidden=True, width=11))
        sheet.add(ColumnControl('Palm_Status', 'center', '@', '掌厚度結果', font, hidden=True, width=11))
        sheet.add(ColumnControl('Finger_Value', 'center', '0.00', '指厚度值', font, hidden=True, width=11))
        sheet.add(ColumnControl('Finger_Limit', 'center', '@', '指厚度上下限', font, hidden=True, width=11))
        sheet.add(ColumnControl('Finger_Status', 'center', '@', '指厚度結果', font, hidden=True, width=11))
        sheet.add(ColumnControl('FingerTip_Value', 'center', '0.00', '指尖厚度值', font, hidden=True, width=11))
        sheet.add(ColumnControl('FingerTip_Limit', 'center', '@', '指尖厚度上下限', font, hidden=True, width=11))
        sheet.add(ColumnControl('FingerTip_Status', 'center', '@', '指尖厚度結果', font, hidden=True, width=11))
        sheet.add(ColumnControl('Length_Value', 'center', '0.00', '長度值', font, hidden=True, width=11))
        sheet.add(ColumnControl('Length_Limit', 'center', '@', '長度上下限', font, hidden=True, width=11))
        sheet.add(ColumnControl('Length_Status', 'center', '@', '長度結果', font, hidden=True, width=11))
        sheet.add(ColumnControl('Weight_Value', 'center', '0.00', '重量值', font, hidden=True, width=11))
        sheet.add(ColumnControl('Weight_Limit', 'center', '@', '重量上下限', font, hidden=True, width=11))
        sheet.add(ColumnControl('Weight_Light', 'center', '@', '超輕檢驗', font, hidden=True, width=11))
        sheet.add(ColumnControl('Weight_Heavy', 'center', '@', '超重檢驗', font, hidden=True, width=11))
        sheet.add(ColumnControl('Width_Value', 'center', '0.00', '寬度值', font, hidden=True, width=11))
        sheet.add(ColumnControl('Width_Limit', 'center', '@', '寬度上下限', font, hidden=True, width=11))
        sheet.add(ColumnControl('Width_Status', 'center', '@', '寬度結果', font, hidden=True, width=11))
        sheet.add(ColumnControl('Pinhole_Value', 'center', '0.00', '針孔值', font, hidden=True, width=11))
        sheet.add(ColumnControl('Pinhole_Limit', 'center', '@', '針孔上下限', font, hidden=True, width=11))
        sheet.add(ColumnControl('Pinhole_Status', 'center', '@', '針孔結果', font, hidden=True, width=11))
        sheet.add(ColumnControl('IPQC', 'center', '@', 'IPQC', font, hidden=False, width=11))
        sheet.add(ColumnControl('WoStartDate', 'center', 'yyyy/mm/dd hh:mm:ss', '工單開始時間', font, hidden=True, width=11))
        sheet.add(ColumnControl('WoEndDate', 'center', 'yyyy/mm/dd hh:mm:ss', '工單結束時間', font, hidden=True, width=11))

        header_columns = sheet.header_columns
        column_letter = sheet.column_letter
        selected_columns = [col for col in sheet.column_names if col in df.columns]
        # endregion

        # region 2. DataFrame convert to Excel
        df = df[selected_columns].copy()

        # 轉出Excel前進行資料處理
        df['ProductionTime'] = (df['ProductionTime'] // 60).astype(str) + 'H'
        df['Period'] = df['Period'].apply(lambda x: f"{int(x):02}:00")

        # Change column names
        df.rename(columns=header_columns, inplace=True)

        namesheet = str(machine_name).split('_')[-1]
        # Write data to the Excel sheet with the machine name as the sheet name
        df.to_excel(writer, sheet_name=namesheet, index=False)

        # Read the written Excel file
        workbook = writer.book
        worksheet = writer.sheets[namesheet]

        # Freeze the first row
        worksheet.freeze_panes = worksheet['A2']

        sheet.apply_formatting(worksheet)
        # endregion

        # region 3. Customize
        # # 設置欄的 outlineLevel 讓其可以折疊/展開
        hide_columns = ['Tensile_Value', 'Tensile_Limit', 'Tensile_Status', 'Elongation_Value', 'Elongation_Limit',
                        'Elongation_Status',
                        'Roll_Value', 'Roll_Limit', 'Roll_Status', 'Cuff_Value', 'Cuff_Limit', 'Cuff_Status',
                        'Palm_Value', 'Palm_Limit', 'Palm_Status',
                        'Finger_Value', 'Finger_Limit', 'Finger_Status', 'FingerTip_Value', 'FingerTip_Limit',
                        'FingerTip_Status',
                        'Length_Value', 'Length_Limit', 'Length_Status', 'Weight_Value', 'Weight_Limit', 'Weight_Light',
                        'Weight_Heavy',
                        'Width_Value', 'Width_Limit', 'Width_Status',
                        'Pinhole_Value', 'Pinhole_Limit', 'Pinhole_Status']
        for column in hide_columns:
            worksheet.column_dimensions[column_letter[column]].outlineLevel = 1
        worksheet.column_dimensions.group(column_letter['Tensile_Value'], column_letter['Pinhole_Status'], hidden=True)

        hide_columns = ['Roll_Value', 'Roll_Limit', 'Roll_Status', 'Cuff_Value', 'Cuff_Limit', 'Cuff_Status',
                        'Palm_Value', 'Palm_Limit', 'Palm_Status',
                        'Finger_Value', 'Finger_Limit', 'Finger_Status', 'FingerTip_Value', 'FingerTip_Limit',
                        'FingerTip_Status']
        for column in hide_columns:
            worksheet.column_dimensions[column_letter[column]].outlineLevel = 2
        worksheet.column_dimensions.group(column_letter['Roll_Value'], column_letter['FingerTip_Status'], hidden=True)

        for row in range(2, worksheet.max_row + 1):  # 從第2行開始，因為第1行是標題
            weight_value_cell = worksheet[column_letter['WeightValue'] + str(row)]
            weight_limit_value = worksheet[column_letter['Weight_Limit'] + str(row)].value
            comment = Comment(text="IPQC範圍(" + weight_limit_value + ")",
                              author="System")  # 創建註解
            weight_value_cell.comment = comment

        # endregion

        return workbook

    def generate_activation_excel(self, writer, df):
        # region 1. Column Define
        font = Font(name=self.report_font, size=10, bold=False)
        sheet = DataControl()
        sheet.add(ColumnControl('CreationTime', 'center', '@', '點數機時間', font, hidden=False, width=20))
        sheet.add(ColumnControl('MES_MACHINE', 'left', '@', '機台號', font, hidden=False, width=20))
        sheet.add(ColumnControl('A1_Qty', 'center', '#,##0', 'A1數量', font, hidden=False, width=15))
        sheet.add(ColumnControl('A1_Speed', 'center', '0', 'A1車速', font, hidden=False, width=15))
        sheet.add(ColumnControl('A2_Qty', 'center', '#,##0', 'A2數量', font, hidden=False, width=15))
        sheet.add(ColumnControl('A2_Spped', 'center', '0', 'A2車速', font, hidden=False, width=15))
        sheet.add(ColumnControl('B1_Qty', 'right', '#,##0', 'B1數量', font, hidden=False, width=15))
        sheet.add(ColumnControl('B1_Speed', 'center', '0', 'B1車速', font, hidden=False, width=15))
        sheet.add(ColumnControl('B2_Qty', 'right', '#,##0', 'B2數量', font, hidden=False, width=15))
        sheet.add(ColumnControl('B2_Speed', 'center', '0', 'B2車速', font, hidden=False, width=15))

        column_letter = sheet.column_letter
        selected_columns = [col for col in sheet.column_names if col in df.columns]
        # endregion

        # region 2. DataFrame convert to Excel
        df = df[selected_columns].copy()

        namesheet = "點數機"
        # Write data to the Excel sheet with the machine name as the sheet name
        df.to_excel(writer, sheet_name=namesheet, index=False)

        # Read the written Excel file
        workbook = writer.book
        worksheet = writer.sheets[namesheet]

        sheet.apply_formatting(worksheet)

        # Freeze the first row
        worksheet.freeze_panes = worksheet['A2']
        # endregion

        # region 3. Customize
        try:
            # 設置條件格式為黃顏色填充
            yellow_fill = PatternFill(start_color='FFFF00', end_color='FFFF00', fill_type='solid')

            # 使用 CellIsRule 設置條件格式，只需設定一次
            # 此範例將適用於 A1_Qty 和 B1_Qty 列
            for col_letter in [column_letter['A1_Qty'], column_letter['B1_Qty']]:
                # 假設您知道資料的範圍為 B2:B100，您可以根據實際情況修改範圍
                worksheet.conditional_formatting.add(f'{col_letter}2:{col_letter}65535',
                                                     CellIsRule(operator='lessThanOrEqual',
                                                                formula=['10'],
                                                                fill=yellow_fill))
        except Exception as e:
            print(e)
        # endregion

        return workbook

    def generate_cosmetic_excel(self, writer, df):
        # region 1. Column Define
        font = Font(name=self.report_font, size=10, bold=False)
        sheet = DataControl()
        sheet.add(ColumnControl('runcard', 'center', '@', 'Runcard', font, hidden=False, width=14))
        sheet.add(ColumnControl('belong_to', 'center', '@', '作業日期', font, hidden=False, width=14))
        sheet.add(ColumnControl('Machine', 'center', '@', '機台', font, hidden=False))
        sheet.add(ColumnControl('Line', 'center', '@', '線別', font, hidden=False))
        sheet.add(ColumnControl('Shift', 'center', '@', '班別', font, hidden=False))
        sheet.add(ColumnControl('WorkOrder', 'center', '@', '工單', font, hidden=False))
        sheet.add(ColumnControl('PartNo', 'center', '@', '料號', font, hidden=False, width=14))
        sheet.add(ColumnControl('ProductItem', 'center', '@', '品項', font, hidden=False, width=20))
        sheet.add(ColumnControl('SalePlaceCode', 'center', '@', '銷售地點', font, hidden=False, width=14))
        sheet.add(ColumnControl('Period', 'center', '0', 'Period', font, hidden=False, width=14))
        sheet.add(ColumnControl('6100LL1', 'center', '0', '美線過重', font, hidden=False, width=14))
        sheet.add(ColumnControl('6100LL2', 'center', '0', '美線過輕', font, hidden=False, width=14))
        sheet.add(ColumnControl('6200LL1', 'center', '0', '歐線過重', font, hidden=False, width=14))
        sheet.add(ColumnControl('6200LL2', 'center', '0', '歐線過輕', font, hidden=False, width=14))
        sheet.add(ColumnControl('6300LL1', 'center', '0', '日線過重', font, hidden=False, width=14))
        sheet.add(ColumnControl('6300LL2', 'center', '0', '日線過輕', font, hidden=False, width=14))
        sheet.add(ColumnControl('7000LL1', 'center', '0', 'OBM過重', font, hidden=False, width=14))
        sheet.add(ColumnControl('7000LL2', 'center', '0', 'OBM過輕', font, hidden=False, width=14))

        sql = """
          SELECT *
          FROM [MES_OLAP].[dbo].[mes_defect_define]
          where defect_type = 'COSMETIC'
          order by defect_level, defect_code
        """
        rows = self.mes_olap_db.select_sql_dict(sql)
        for row in rows:
            desc = row['desc1'] if row['desc1'] != '' else row['desc2']
            sheet.add(ColumnControl(row['defect_code'], 'center', '0', desc, font, hidden=False, width=14))

        sheet.add(ColumnControl('cosmetic_inspect_qty_x', 'right', '0', '外觀檢查數量', font, hidden=False, width=14))

        sql = """
                  SELECT *
                  FROM [MES_OLAP].[dbo].[mes_defect_define]
                  where defect_type = 'PINHOLE'
                  order by defect_level, defect_code
                """
        rows = self.mes_olap_db.select_sql_dict(sql)
        for row in rows:
            desc = row['desc1'] if row['desc1'] != '' else row['desc2']
            sheet.add(ColumnControl(row['defect_code'], 'center', '0', desc, font, hidden=False, width=14))

        sheet.add(ColumnControl('Pinhole', 'right', '0', '針孔數量', font, hidden=False, width=14))
        sheet.add(ColumnControl('Pinhole_Sample', 'right', '0', '針孔檢查數量', font, hidden=False, width=14))

        header_columns = sheet.header_columns
        column_letter = sheet.column_letter

        # 將缺少的欄位加進 df
        missing_cols = [col for col in header_columns if col not in df.columns]
        # 建立一個含缺少欄位的新 DataFrame，值為空字串（也可以換成 np.nan）
        new_cols_df = pd.DataFrame({col: [''] * len(df) for col in missing_cols})
        df = pd.concat([df, new_cols_df], axis=1)

        # endregion

        # region 2. DataFrame convert to Excel
        df = df[header_columns].copy()

        # Change column names
        df.rename(columns=header_columns, inplace=True)

        namesheet = "外觀"
        df.to_excel(writer, sheet_name=namesheet, index=False, startrow=1)

        # Read the written Excel file
        workbook = writer.book
        worksheet = writer.sheets[namesheet]

        # Freeze the first row
        worksheet.freeze_panes = worksheet['A3']

        sheet.apply_formatting(worksheet)
        # endregion

        # region 3. Customize
        # header_alignment = Alignment(horizontal='center', wrap_text=True)
        header_alignment = Alignment(horizontal='center')
        header_border = Border(
            top=Side(style='medium'),
            bottom=Side(style='medium'),
            left=Side(style='medium'),
            right=Side(style='medium')
        )
        fill_style = PatternFill(start_color="F2F2F2", end_color="F2F2F2", fill_type="solid")

        # Weight Header
        worksheet.merge_cells(f"{column_letter['6100LL1']}1:{column_letter['7000LL2']}1")
        start_row, start_col = worksheet[f"{column_letter['6100LL1']}1"].row, worksheet[
            f"{column_letter['6100LL1']}1"].column
        end_row, end_col = worksheet[f"{column_letter['7000LL2']}1"].row, worksheet[
            f"{column_letter['7000LL2']}1"].column
        cell = worksheet[f"{column_letter['6100LL1']}1"]
        cell.value = "重量檢驗"
        cell.alignment = Alignment(horizontal="center", vertical="center")
        # cell.border = header_border
        for row in range(start_row, end_row + 1):
            for col in range(start_col, end_col + 1):
                cell = worksheet.cell(row=row, column=col)
                cell.border = header_border
                cell.fill = fill_style

        # Cosmetic Header
        worksheet.merge_cells(f"{column_letter['AL1']}1:{column_letter['KL4']}1")
        start_row, start_col = worksheet[f"{column_letter['AL1']}1"].row, worksheet[
            f"{column_letter['AL1']}1"].column
        end_row, end_col = worksheet[f"{column_letter['KL4']}1"].row, worksheet[
            f"{column_letter['KL4']}1"].column
        cell = worksheet[f"{column_letter['AL1']}1"]
        cell.value = "外觀CRITICAL"
        cell.alignment = Alignment(horizontal="center", vertical="center")
        for row in range(start_row, end_row + 1):
            for col in range(start_col, end_col + 1):
                cell = worksheet.cell(row=row, column=col)
                cell.border = header_border
                cell.fill = fill_style

        worksheet.merge_cells(f"{column_letter['AL3']}1:{column_letter['NK1']}1")
        start_row, start_col = worksheet[f"{column_letter['AL3']}1"].row, worksheet[
            f"{column_letter['AL3']}1"].column
        end_row, end_col = worksheet[f"{column_letter['NK1']}1"].row, worksheet[
            f"{column_letter['NK1']}1"].column
        cell = worksheet[f"{column_letter['AL3']}1"]
        cell.value = "外觀MAJOR"
        cell.alignment = Alignment(horizontal="center", vertical="center")
        for row in range(start_row, end_row + 1):
            for col in range(start_col, end_col + 1):
                cell = worksheet.cell(row=row, column=col)
                cell.border = header_border
                cell.fill = fill_style

        worksheet.merge_cells(f"{column_letter['BN2']}1:{column_letter['MX4']}1")
        start_row, start_col = worksheet[f"{column_letter['BN2']}1"].row, worksheet[
            f"{column_letter['BN2']}1"].column
        end_row, end_col = worksheet[f"{column_letter['MX4']}1"].row, worksheet[
            f"{column_letter['MX4']}1"].column
        cell = worksheet[f"{column_letter['BN2']}1"]
        cell.value = "外觀MINOR"
        cell.alignment = Alignment(horizontal="center", vertical="center")
        for row in range(start_row, end_row + 1):
            for col in range(start_col, end_col + 1):
                cell = worksheet.cell(row=row, column=col)
                cell.border = header_border
                cell.fill = fill_style

        # Pinhole Header
        worksheet.merge_cells(f"{column_letter['B']}1:{column_letter['N5_1']}1")
        start_row, start_col = worksheet[f"{column_letter['B']}1"].row, worksheet[
            f"{column_letter['B']}1"].column
        end_row, end_col = worksheet[f"{column_letter['N5_1']}1"].row, worksheet[
            f"{column_letter['N5_1']}1"].column
        cell = worksheet[f"{column_letter['B']}1"]
        cell.value = "針孔"
        cell.alignment = Alignment(horizontal="center", vertical="center")
        for row in range(start_row, end_row + 1):
            for col in range(start_col, end_col + 1):
                cell = worksheet.cell(row=row, column=col)
                cell.border = header_border
                cell.fill = fill_style

        # 最後一行加總
        thin_top_border = Border(top=Side(style="thin"))
        last_row = worksheet.max_row + 1

        for col_idx in range(11, len(header_columns) + 1):
            col_letter = get_column_letter(col_idx)

            # 設定 SUM 公式
            sum_formula = f"=SUM({col_letter}2:{col_letter}{last_row-1})"
            worksheet[f"{col_letter}{last_row}"] = sum_formula  # ✅ 填入最後一行

            # 在最後一行上方（倒數第 2 行）加上框線
            if last_row > 1:  # 確保至少有兩行
                cell_above = worksheet[f"{col_letter}{last_row}"]
                cell_above.border = thin_top_border  # ✅ 設定上方框線
        # endregion

        return workbook

    def generate_excel(self, fix_main_df, subtotals_df, activation_df, cosmetic_df, excel_file):
        start_time = time.time()
        with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
            self.generate_summary_excel(writer, subtotals_df)

            machine_groups = fix_main_df.groupby('Name')
            for machine_name, machine_df in machine_groups:
                # 處理停機情況
                if not machine_df['ProductItem'].iloc[0]:
                    continue

                machine_clean_df = machine_df.sort_values(by=['Date', 'Shift', 'Period'])
                self.generate_machine_excel(writer, machine_clean_df, self.plant, machine_name)

            # 稼動率Raw Data
            self.generate_activation_excel(writer, activation_df)

            # 外觀
            self.generate_cosmetic_excel(writer, cosmetic_df)

        end_time = time.time()
        elapsed_time = end_time - start_time
        self.logger.info(f"Time taken: {elapsed_time:.2f} seconds.")

    def generate_chart(self, chart_df, image_file):
        start_time = time.time()

        # Create Chart
        fig, ax1 = plt.subplots(figsize=(10, 6))

        plt.rcParams['font.sans-serif'] = ['Microsoft YaHei']
        plt.rcParams['axes.unicode_minus'] = False  # 避免負號顯示錯誤

        # Only substring Name right 3 characters
        chart_df['Name_short'] = chart_df['Name'].apply(lambda x: x[-3:])

        chart_df['Unfinished'] = (chart_df['Target'] - chart_df['sum_qty']).apply(lambda x: max(x, 0))  # 未達成數量, 負數為0
        chart_df['Achievement Rate'] = chart_df['sum_qty'] / chart_df['Target'] * 100  # 達成率（百分比）
        chart_df.loc[chart_df['Target'] == 0, 'Achievement Rate'] = None  # 當 Target 為 0，將達成率設為 None

        # Draw Bar Chart
        bar_width = 0.6
        bars = ax1.bar(chart_df['Name_short'], chart_df['sum_qty'], width=bar_width, color='lightcoral', label='日目標達成率')
        ax1.bar(chart_df['Name_short'], chart_df['Unfinished'], width=bar_width, bottom=chart_df['sum_qty'],
                color='lightgreen')

        # Set the X-axis label and the Y-axis label
        ax1.set_xlabel('機台')
        ax1.set_ylabel('日產量')
        # 設置 Y 軸的上限為 120 萬
        if "PVC" in self.plant:
            ax1.set_ylim(0, 800000)
        else:
            ax1.set_ylim(0, 1200000)

        # 自定義 Y 軸以 10 萬為單位
        def y_formatter(x, pos):
            return f'{int(x/10000)}萬'  # 將數值轉換為「萬」的單位顯示

        ax1.yaxis.set_major_formatter(FuncFormatter(y_formatter))

        achieve_rate = 95 if "NBR" in self.plant else 98

        # 在每個長條圖上方顯示達成率百分比
        for bar, unfinished, rate in zip(bars, chart_df['Unfinished'], chart_df['Achievement Rate']):
            if pd.notnull(rate):  # 僅顯示達成率不為 None 的數值
                height = bar.get_height() + unfinished  # 計算長條的總高度
                if rate < achieve_rate:
                    ax1.text(bar.get_x() + bar.get_width() / 2, height + 20000, f'{rate:.1f}%', ha='center',
                             va='bottom',
                             fontsize=10, color='red',
                             bbox=dict(boxstyle="circle", edgecolor='red', facecolor='none', linewidth=1.5))

                else:
                    ax1.text(bar.get_x() + bar.get_width() / 2, height + 20000, f'{rate:.1f}%', ha='center',
                             va='bottom',
                             fontsize=10)

        plt.title(f'{self.location} {self.plant} {self.report_date1} 日產量與日目標達成率 (達成率目標 > {achieve_rate}%)')

        # Display the legend of the bar chart and line chart together
        fig.legend(loc="upper right", bbox_to_anchor=(1, 1), bbox_transform=ax1.transAxes)

        plt.savefig(image_file)

        # Save the image to a BytesIO object
        image_stream = BytesIO()
        plt.savefig(image_stream, format='png')
        image_stream.seek(0)  # Move the pointer to the beginning of the file
        plt.close()  # Close the image to free up memory

        end_time = time.time()
        elapsed_time = end_time - start_time
        self.logger.info(f"Time taken: {elapsed_time:.2f} seconds.")

        return image_stream

    def calculate_activation(self, mach):
        try:
            wo_time = 86400

            sql = f"""
                WITH A1 AS (
                SELECT m.MES_MACHINE,Qty2,Speed,LINE,CreationTime
                                  FROM [PMG_DEVICE].[dbo].[COUNTING_DATA] d
                                  JOIN [PMG_DEVICE].[dbo].[COUNTING_DATA_MACHINE] m on d.MachineName = m.COUNTING_MACHINE
                                  where m.MES_MACHINE = '{mach}' and m.LINE = 'A1'
                                  and CreationTime between CONVERT(DATETIME, '{report_date1} 06:00:00', 120) and CONVERT(DATETIME, '{report_date2} 05:59:59', 120) 
                ),
                B1 AS (
                SELECT m.MES_MACHINE,Qty2,Speed,LINE,CreationTime
                                  FROM [PMG_DEVICE].[dbo].[COUNTING_DATA] d
                                  JOIN [PMG_DEVICE].[dbo].[COUNTING_DATA_MACHINE] m on d.MachineName = m.COUNTING_MACHINE
                                  where m.MES_MACHINE = '{mach}' and m.LINE = 'B1'
                                  and CreationTime between CONVERT(DATETIME, '{report_date1} 06:00:00', 120) and CONVERT(DATETIME, '{report_date2} 05:59:59', 120) 
                ),
                A2 AS (
                SELECT m.MES_MACHINE,Qty2,Speed,LINE,CreationTime
                                  FROM [PMG_DEVICE].[dbo].[COUNTING_DATA] d
                                  JOIN [PMG_DEVICE].[dbo].[COUNTING_DATA_MACHINE] m on d.MachineName = m.COUNTING_MACHINE
                                  where m.MES_MACHINE = '{mach}' and m.LINE = 'A2'
                                  and CreationTime between CONVERT(DATETIME, '{report_date1} 06:00:00', 120) and CONVERT(DATETIME, '{report_date2} 05:59:59', 120) 
                ),
                B2 AS (
                SELECT m.MES_MACHINE,Qty2,Speed,LINE,CreationTime
                                  FROM [PMG_DEVICE].[dbo].[COUNTING_DATA] d
                                  JOIN [PMG_DEVICE].[dbo].[COUNTING_DATA_MACHINE] m on d.MachineName = m.COUNTING_MACHINE
                                  where m.MES_MACHINE = '{mach}' and m.LINE = 'B2'
                                  and CreationTime between CONVERT(DATETIME, '{report_date1} 06:00:00', 120) and CONVERT(DATETIME, '{report_date2} 05:59:59', 120) 
                )

                Select FORMAT(A1.CreationTime, 'yyyy-MM-dd HH:mm:ss') CreationTime, A1.MES_MACHINE, A1.Qty2 A1_Qty, A1.Speed A1_Speed, A2.Qty2 A2_Qty, A2.Speed A2_Spped, B1.Qty2 B1_Qty, B1.Speed B1_Speed, B2.Qty2 B2_Qty, B2.Speed B2_Speed from A1 
                left join A2 on A1.CreationTime = A2.CreationTime
                join B1 on A1.CreationTime = B1.CreationTime
                left join B2 on A1.CreationTime = B2.CreationTime

            """
            detail_raws = self.mes_db.select_sql_dict(sql)

            df = pd.DataFrame(detail_raws)
            filtered_df = df[(df['A1_Qty'] > 10) | (df['B1_Qty'] > 10)]
            run_time = len(filtered_df) * 300
            active_rate = run_time / wo_time
            return round(active_rate, 2), df
        except Exception as e:
            print(e)

    def calculate_cosmetic_dpm(self, mach_df, cosmetic_df):
        df = pd.merge(mach_df, cosmetic_df, on=['runcard'], how='left')
        inspect_total = df['cosmetic_inspect_qty_x'].sum()
        sum_qty_total = df['cosmetic_sum_qty'].sum()

        if inspect_total > 0:
            dpm = round(sum_qty_total * 1_000_000 / inspect_total, 0)
        else:
            dpm = None  # 或設為 0 或 NaN，視你的需求而定

        return dpm

    def calculate_pinhole_dpm(self, mach_df, cosmetic_df):
        df = pd.merge(mach_df, cosmetic_df, on=['runcard'], how='left')
        df['Pinhole_Sample'] = pd.to_numeric(df['Pinhole_Sample'], errors='coerce')
        df['Pinhole'] = pd.to_numeric(df['Pinhole'], errors='coerce')
        inspect_total = df['Pinhole_Sample'].sum()
        sum_qty_total = df['Pinhole'].sum()

        if inspect_total > 0:
            dpm = round(sum_qty_total * 1_000_000 / inspect_total, 0)
        else:
            dpm = None  # 或設為 0 或 NaN，視你的需求而定

        return dpm

    # 缺模率
    def calculate_mold_lost_rate(self, mach_df):
        return 0

    # 離型不良率
    def calculate_dmf_rate(self, mach_df):
        return 0

    def send_email(self, config, subject, file_list, image_buffers, msg_list, error_list):
        logging.info(f"Start to send Email")

        error_msg = '<br>'.join(error_list)
        if len(error_list) > 0:
            error_msg = error_msg + '<br>'
        normal_msg = '<br>'.join(msg_list)
        if len(msg_list) > 0:
            normal_msg = normal_msg + '<br>'

        max_reSend = 5
        reSent = 0
        while reSent < max_reSend:
            try:
                super().send_email(config, subject, file_list, image_buffers, error_msg, normal_msg=normal_msg)
                print('Email sent successfully')
                logging.info(f"Email sent successfully")
                break
            except Exception as e:
                print(f'Email sending failed: {e}')
                logging.info(f"Email sending failed: {e}")
                reSent += 1
                if reSent >= max_reSend:
                    print('Failed to send email after 5 retries')
                    logging.info(f"Failed to send email after 5 retries")
                    break
                time.sleep(180)  # seconds


report_date1 = datetime.today() - timedelta(days=1)
report_date1 = report_date1.strftime('%Y%m%d')

report_date2 = datetime.today()
report_date2 = report_date2.strftime('%Y%m%d')

# report_date1 = "20250324"
# report_date2 = "20250325"

report = mes_daily_report(report_date1, report_date2)
report.main()