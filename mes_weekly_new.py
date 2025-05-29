import sys
import os
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)

import pandas as pd
from database import vnedc_database, mes_database
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import logging
import math
from matplotlib import font_manager
from openpyxl.comments import Comment
from openpyxl.styles import Alignment, NamedStyle, Font, Border, Side, PatternFill
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from email.mime.image import MIMEImage
from openpyxl.drawing.image import Image
from dateutil.relativedelta import relativedelta
import matplotlib.gridspec as gridspec
import numpy as np
from datetime import datetime, timedelta, date

class mes_weekly_report(object):
    plant_name = ['NBR'] #, 'PVC'
    mode = ""
    save_path = ""
    date_mark = ""
    month_week = ""
    mach_list = []
    file_list = []

    # image_buffers = []
    # mach_list = ""
    # Define Style
    percent_style = NamedStyle(name='percent_style', number_format='0.00%')
    right_align_style = NamedStyle(name='right_align_style', alignment=Alignment(horizontal='right'))
    center_align_style = NamedStyle(name='center_align_style', alignment=Alignment(horizontal='center'))

    # Define Header
    header_font = Font(bold=True)
    header_alignment = Alignment(horizontal='center')
    header_border = Border(bottom=Side(style='thin'))
    header_columns = {
        'belong_to': '日期',
        'Machine': '機台號',
        'Line': '線別',
        'WorkOrderId': '工單',
        'PartNo': '料號',
        'StandardAQL': '標準AQL',
        'InspectedAQL': '已檢查AQL',
        'Period': '時期',
        'MaxSpeed': '最大速度',
        'MinSpeed': '最小速度',
        'AvgSpeed': '平均速度',
        'StdSpeed': '標準速度',
    }

    # 配置日志记录器
    logging.basicConfig(
        level=logging.INFO,  # 设置日志级别为 DEBUG，这样所有级别的日志都会被记录
        format='%(asctime)s - %(levelname)s - %(message)s',  # 指定日志格式
        filename='weekly.log',  # 指定日志文件
        filemode='w'  # 写入模式，'w' 表示每次运行程序时会覆盖日志文件
    )

    def __init__(self, mode):
        self.db = vnedc_database()
        self.mode = mode
        self.last_week_info = (datetime.now().date() - timedelta(weeks=1)).isocalendar()[1]

        if mode == 'WEEKLY':
            folder_name = 'W' + str(self.last_week_info).zfill(2)
            save_path = os.path.join("weekly_output", folder_name)

            sql = f"""
            SELECT TOP 1 *
                FROM [VNEDC].[dbo].[week_date]
                WHERE start_date < (
                    SELECT start_date
                    FROM [VNEDC].[dbo].[week_date]
                    WHERE CAST(GETDATE() AS DATE) BETWEEN CAST(start_date AS DATE) AND CAST(end_date AS DATE)
                )ORDER BY start_date DESC;
            """

            date = vnedc_database().select_sql_dict(sql)
            self.month_week = f"{date[0]['month']}{date[0]['month_week']}"
            self.date_mark = "{start_date}_{end_date}".format(start_date=datetime.strptime(date[0]['start_date'], "%Y-%m-%d").strftime("%m%d"), end_date=datetime.strptime(date[0]['end_date'], "%Y-%m-%d").strftime("%m%d"))
        self.save_path = save_path
        # Check if folder to created
        if not os.path.exists(save_path):
            os.makedirs(save_path)

    def generate_raw_excel(self, plant):
        save_path = self.save_path
        date_mark = self.date_mark
        mode = self.mode
        month_week = '10W1'#self.month_week

        file_name = f'MES_{plant}_{mode}_Report_{date_mark}.xlsx'
        excel_file = os.path.join(save_path, file_name)
        with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
            sql = f"""SELECT belong_to, Machine, Line, WorkOrder, PartNo, StandardAQL, InspectedAQL,
                        Period, MaxSpeed, MinSpeed, AvgSpeed, StdSpeed, CountingQty, OnlinePacking, Target, ScrapQuantity, FaultyQuantity, RunTime, StopTime, c.MonthWeek
                        FROM [MES_OLAP].[dbo].[counting_hourly_info_raw] c
                        LEFT JOIN [MES_OLAP].[dbo].[mes_ipqc_data] ipqc on c.Runcard = ipqc.Runcard
                        where c.MonthWeek = '{month_week}'  and c.branch like'%{plant}%' and Machine not in ('VN_GD_PVC1_L03','VN_GD_PVC1_L04') and OnlinePacking > 0
                        --and StandardAQL is not Null and InspectedAQL is not null 
                        order by Machine, belong_to, line, period
                        """
            data = self.db.select_sql_dict(sql)
            df = pd.DataFrame(data)
            machine_groups = df.groupby('Machine')
            self.generate_summary(writer, machine_groups)
            for machine_name, machine_df in machine_groups:
                self.generate_excel(writer, machine_df, machine_name)
        # self.file_list.append(excel_file)

    def generate_excel(self, writer, df, machine_name):
        column_letter = {'belong_to': 'A', 'Machine': 'B', 'Line': 'C', 'WorkOrder': 'D',
                'PartNo': 'E', 'StandardAQL': 'F', 'InspectedAQL': 'G', 'Period': 'H',
                'MaxSpeed': 'I', 'MinSpeed': 'J', 'AvgSpeed': 'K', 'StdSpeed': 'L',
                'CountingQty': 'M', 'OnlinePacking': 'N', 'Target': 'O',
                'ScrapQuantity': 'P', 'FaultyQuantity': 'Q', 'RunTime': 'R',
                'StopTime': 'S', 'c.MonthWeek': 'T'}

        df['Period'] = df['Period'].apply(lambda x: f"{int(x):02}:00")

        # Rename columns
        df.rename(columns=self.header_columns, inplace=True)
        namesheet = str(machine_name).split('_')[-1]
        save_path = self.save_path

        header_row = 0
        data_start_row = 1

        # Write data to the Excel sheet
        df.to_excel(writer, sheet_name=namesheet, index=False, startrow=header_row)

        workbook = writer.book
        worksheet = writer.sheets.get(namesheet)
        if not worksheet:
            worksheet = workbook.add_worksheet(namesheet)

        # Apply Header Style
        for cell in worksheet[data_start_row]:
            cell.font = self.header_font
            cell.alignment = self.header_alignment
            cell.border = self.header_border

        # Adjust column formatting
        for col in worksheet.columns:
            max_length = max(len(str(cell.value)) for cell in col)
            col_letter = col[0].column_letter

            worksheet.column_dimensions[col_letter].width = max_length + 5
            for cell in col:
                if col_letter in [column_letter['MaxSpeed'], column_letter['MinSpeed'], column_letter['AvgSpeed'],
                                  column_letter['StdSpeed']]:
                    cell.alignment = self.right_align_style.alignment
                elif col_letter in [column_letter['CountingQty'], column_letter['OnlinePacking'], column_letter['Target']]:
                    cell.number_format = '#,##0'
                    cell.alignment = self.right_align_style.alignment

        return workbook

    def generate_summary(self, writer, machine_groups):
        column_letter = {'Machine': 'A', 'MonthWeek': 'B', 'Line': 'C', 'CountingQty': 'D', 'Target': 'E', 'OnlinePacking': 'F', 'Achievement Rate': 'G'}
        summary_data = []
        tmp_date = self.date_mark.replace('_', '~')
        tmp_week = f"第{self.last_week_info}週({tmp_date})"

        thin_border_top = Border(top=Side(style="thin"))
        thin_border_bottom = Border(bottom=Side(style="thin"))

        for machine_name, machine_df in machine_groups:
            for shift in machine_df['MonthWeek'].unique():
                for line in machine_df['Line'].unique():
                    filtered_df = machine_df[(machine_df['Line'] == line) & (machine_df['MonthWeek'] == shift)]
                    total_output = filtered_df['CountingQty'].sum()
                    total_target = filtered_df['Target'].sum()
                    total_packing = filtered_df['OnlinePacking'].sum()
                    rate = round((int(total_output) / int(total_target)), 3) if int(total_target) > 0 else 0

                    summary_row = {
                        'Name': machine_name,
                        'MonthWeek': shift,
                        'Line': line,
                        'CountingQty': total_output,
                        'Target': total_target,
                        'OnlinePacking': total_packing,
                        'Achievement Rate': rate
                    }
                    summary_data.append(summary_row)

            sum_qty = sum(item['CountingQty'] for item in summary_data if item['Name'] == machine_name)
            sum_target = sum(item['Target'] for item in summary_data if item['Name'] == machine_name)
            summary_data.append(
                {'Name': machine_name, 'MonthWeek': tmp_week, 'Line': '', 'CountingQty': sum_qty, 'Target': sum_target,
                 'OnlinePacking': total_packing, 'Achievement Rate': round(sum_qty / sum_target, 3) if sum_target > 0 else 0})

        summary_df = pd.DataFrame(summary_data)
        summary_df.rename(columns=self.header_columns, inplace=True)
        summary_sheet_name = "Summary"
        summary_df.to_excel(writer, sheet_name=summary_sheet_name, index=False)

        workbook = writer.book
        worksheet = writer.sheets[summary_sheet_name]

        for cell in worksheet[1]:
            cell.font = self.header_font
            cell.alignment = self.header_alignment
            cell.border = self.header_border

        for col in worksheet.columns:
            max_length = max(len(str(cell.value)) for cell in col)
            col_letter = col[0].column_letter
            worksheet.column_dimensions[col_letter].width = max_length + 5
            for cell in col:
                if col_letter in [column_letter['Target'], column_letter['CountingQty'], column_letter['OnlinePacking']]:
                    cell.number_format = '#,##0'
                    cell.alignment = self.right_align_style.alignment
                elif col_letter in [column_letter['Achievement Rate']]:
                    worksheet.column_dimensions[col_letter].width = 18
                    cell.alignment = self.center_align_style.alignment
                    cell.number_format = '0.0%'
                else:
                    cell.alignment = self.center_align_style.alignment

        name_col = 1
        current_name = None
        group_start = None

        for row in range(2, len(summary_df) + 2):
            name = worksheet.cell(row=row, column=name_col).value

            if name != current_name:
                if group_start is not None:
                    last_row = row - 1
                    for col in range(1, summary_df.shape[1] + 1):
                        worksheet.cell(row=last_row, column=col).border = thin_border_top + thin_border_bottom
                    if row - group_start > 1:
                        worksheet.row_dimensions.group(group_start, row - 2, hidden=True)

                current_name = name
                group_start = row

        if group_start is not None:
            last_row = len(summary_df) + 1
            for col in range(1, summary_df.shape[1] + 1):
                worksheet.cell(row=last_row, column=col).border = thin_border_top + thin_border_bottom
            if len(summary_df) + 1 - group_start > 1:
                worksheet.row_dimensions.group(group_start, len(summary_df) + 1 - 1, hidden=True)

    def get_mach_list(self, plant):
        sql = f"""select name FROM [PMGMES].[dbo].[PMG_DML_DataModelList]
                    where DataModelTypeId = 'DMT000003' and name like '%{'NBR' if plant == 'NBR' else 'PVC1'}%' 
                    and name not in ('VN_GD_PVC1_L03','VN_GD_PVC1_L04')
                    order by name
                """
        data = mes_database().select_sql_dict(sql)
        return data

    def main(self):
        for plant in self.plant_name:
            self.mach_list = self.get_mach_list(plant)
            self.generate_raw_excel(plant)



import argparse
parser = argparse.ArgumentParser(description="解析外部参数")
parser.add_argument("--mode", choices=['WEEKLY', 'MONTHLY'], help="MONTHLY OR WEEKLY")
args = parser.parse_args()
mode = args.mode

if not mode:
    mode = "WEEKLY"

report = mes_weekly_report(mode)
report.main()