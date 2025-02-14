import sys
import os
from PIL import Image as PILImage
from matplotlib.ticker import MultipleLocator, FuncFormatter

from database import vnedc_database, mes_database, mes_olap_database

curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
import pandas as pd
import matplotlib.pyplot as plt
import logging
from datetime import datetime, timedelta
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

class mes_weekly_report(object):
    this_start_date = ""
    this_end_date = ""
    last_end_date = ""
    last_start_date = ""
    db = None
    plant_name = ['NBR', 'PVC']
    mode = ""
    save_path = ""
    date_mark = ""
    file_list = []
    image_buffers = []
    mach_list = ""

    # Define Style
    percent_style = NamedStyle(name='percent_style', number_format='0.00%')
    right_align_style = NamedStyle(name='right_align_style', alignment=Alignment(horizontal='right'))
    center_align_style = NamedStyle(name='center_align_style', alignment=Alignment(horizontal='center'))

    # Define Header
    header_font = Font(bold=True)
    header_alignment = Alignment(horizontal='center')
    header_border = Border(bottom=Side(style='thin'))
    header_columns = {
        'Name': '機台號',
        'WorkOrderId': '工單',
        'PartNo': '料號',
        'ProductItem': '品項',
        'Line': '線別',
        'Shift': '班別',
        'max_speed': '車速(最高)',
        'min_speed': '車速(最低)',
        'avg_speed': '車速(平均)',
        'sum_qty': '產量(加總)',
        'Ticket_Qty': '入庫量(加總)',
        'ProductionTime': '生產時間',
        'LineSpeedStd': '標準車速',
        'Target': '目標產能',
        'Separate': '隔離',
        'Scrap': '廢品',
        'SecondGrade': '二級品',
        'OverControl': '超內控',
        'WeightValue': 'IPQC克重',
        'WeightLower': '重量下限',
        'WeightUpper': '重量上限',
        'Activation': '稼動率',
        'OpticalNGRate': '光檢不良率',
        'Achievement Rate': '目標達成率',
        'Date': '日期範圍',
        'CountingQty': '點數機數量',
        'FaultyQuantity': '二級品數量',
        'ScrapQuantity': '廢品數量',
        'SeparateQuantity': '隔離品數量',
        'SeparateQty': '隔離品數量',
        'OnlinePacking': '包裝確認量',
        'RunTime': '機台運轉時間',
        'StopTime': '停機時間',
        'AllTime': '可運轉時間',
        'StandSpeed': '標準車速',
        'Capacity': '產能效率',
        'Yield': '良率',
        'SeparateRate': '隔離率',
        'ScrapRate': '報廢率',
        'Plant': '廠別',
        'Year': '年',
        'MonthWeek': '週別',
        'AvgSpeed': '車速',
        'Tensile_Value': '抗拉強度值',
        'Tensile_Limit': '抗拉強度上下限',
        'Tensile_Status': '抗拉強度結果',
        'Elongation_Value': '伸長率值',
        'Elongation_Limit': '伸長率上下限',
        'Elongation_Status': '伸長率結果',
        'Roll_Value': '卷唇厚度值',
        'Roll_Limit': '卷唇厚度上下限',
        'Roll_Status': '卷唇厚度結果',
        'Cuff_Value': '袖厚度值',
        'Cuff_Limit': '袖厚度上下限',
        'Cuff_Status': '袖厚度結果',
        'Palm_Value': '掌厚度值',
        'Palm_Limit': '掌厚度上下限',
        'Palm_Status': '掌厚度結果',
        'Finger_Value': '指厚度值',
        'Finger_Limit': '指厚度上下限',
        'Finger_Status': '指厚度結果',
        'FingerTip_Value': '指尖厚度值',
        'FingerTip_Limit': '指尖厚度上下限',
        'FingerTip_Status': '指尖厚度結果',
        'Length_Value': '長度值',
        'Length_Limit': '長度上下限',
        'Length_Status': '長度結果',
        'Width_Value': '寬度值',
        'Width_Limit': '寬度上下限',
        'Width_Status': '寬度結果',
        'Weight_Value': '重量值',
        'Weight_Limit': '重量上下限',
        'Weight_Status': '重量結果',
        'Pinhole_Value': '針孔值',
        'Pinhole_Limit': '針孔上下限',
        'Pinhole_Status': '針孔結果'
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
        today = datetime.now().date()
        self.mode = mode

        last_week_date = today - timedelta(weeks=1)
        self.last_week_info = last_week_date.isocalendar()[1]

        if mode == "MONTHLY":
            this_start_date = today.replace(day=1)

            last_end_date = this_start_date - timedelta(days=1)
            self.last_end_date = last_end_date
            last_start_date = last_end_date.replace(day=1)
            self.last_start_date = last_start_date

            this_end_date = this_start_date - timedelta(days=1)
            self.this_end_date = this_end_date
            this_start_date = last_end_date.replace(day=1)
            self.this_start_date = this_start_date

            fold_name = this_start_date.strftime('%Y%m').zfill(2)
            save_path = os.path.join("monthly_output", fold_name)
        elif mode == "WEEKLY":
            days_to_sunday = today.weekday()
            this_end_date = today - timedelta(days=days_to_sunday + 1)
            self.this_end_date = this_end_date
            this_start_date = this_end_date - timedelta(days=6)
            self.this_start_date = this_start_date

            last_end_date = this_start_date - timedelta(days=1)
            self.last_end_date = last_end_date
            last_start_date = last_end_date - timedelta(days=6)
            self.last_start_date = last_start_date

            fold_name = 'W' + str(self.last_week_info).zfill(2)
            save_path = os.path.join("weekly_output", fold_name)

        self.save_path = save_path
        # Check folder to create
        if not os.path.exists(save_path):
            os.makedirs(save_path)

        self.date_mark = "{start_date}_{end_date}".format(start_date=this_start_date.strftime("%m%d"),
                                                          end_date=this_end_date.strftime("%m%d"))


    def generate_excel(self, writer, df, machine_name):
        column_letter = {'belong_to': 'A', 'Machine': 'B', 'Line': 'C', 'Shift': 'D', 'WorkOrder': 'E',
                 'PartNo': 'F', 'ProductItem': 'G', 'StandardAQL': 'H', 'InspectedAQL': 'I', 'Period': 'J',
                 'MaxSpeed': 'K', 'MinSpeed': 'L', 'AvgSpeed': 'M', 'StdSpeed': 'N',
                 'CountingQty': 'O', 'OnlinePacking': 'P', 'WIPPacking': 'Q', 'Target': 'R', 'ScrapQuantity': 'S', 'FaultyQuantity': 'T',
                 'RunTime': 'U', 'StopTime': 'V', 'AllTime': 'W', 'MonthWeek': 'X',
                 'Tensile_Value': 'Y', 'Tensile_Limit': 'Z', 'Tensile_Status': 'AA',
                 'Elongation_Value': 'AB', 'Elongation_Limit': 'AC', 'Elongation_Status': 'AD',
                 'Roll_Value': 'AE', 'Roll_Limit': 'AF', 'Roll_Status': 'AG',
                 'Cuff_Value': 'AH', 'Cuff_Limit': 'AI', 'Cuff_Status': 'AJ',
                 'Palm_Value': 'AK', 'Palm_Limit': 'AL', 'Palm_Status': 'AM',
                 'Finger_Value': 'AN', 'Finger_Limit': 'AO', 'Finger_Status': 'AP',
                 'FingerTip_Value': 'AQ', 'FingerTip_Limit': 'AR', 'FingerTip_Status': 'AS',
                 'Length_Value': 'AT', 'Length_Limit': 'AU', 'Length_Status': 'AV',
                 'Weight_Value': 'AW', 'Weight_Limit': 'AX', 'Weight_Status': 'AY',
                 'Width_Value': 'AZ', 'Width_Limit': 'BA', 'Width_Status': 'BB',
                 'Pinhole_Value': 'BC', 'Pinhole_Limit': 'BD', 'Pinhole_Status': 'BE', 'IPQC': 'BF',
                 'SeparateQty': 'BG'
                }

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
                elif col_letter in [column_letter['CountingQty'], column_letter['OnlinePacking'], column_letter['Target'], column_letter['SeparateQty']]:
                    cell.number_format = '#,##0'
                    cell.alignment = self.right_align_style.alignment

        # # 設置欄的 outlineLevel 讓其可以折疊/展開
        hide_columns = ['Tensile_Value','Tensile_Limit','Tensile_Status','Elongation_Value','Elongation_Limit','Elongation_Status',
                        'Roll_Value','Roll_Limit','Roll_Status','Cuff_Value','Cuff_Limit','Cuff_Status','Palm_Value','Palm_Limit','Palm_Status',
                        'Finger_Value','Finger_Limit','Finger_Status','FingerTip_Value','FingerTip_Limit','FingerTip_Status',
                        'Length_Value','Length_Limit','Length_Status', 'Weight_Value', 'Weight_Limit', 'Weight_Status', 'Width_Value','Width_Limit','Width_Status',
                        'Pinhole_Value','Pinhole_Limit','Pinhole_Status']
        for column in hide_columns:
            worksheet.column_dimensions[column_letter[column]].outlineLevel = 1

        worksheet.column_dimensions.group(column_letter['Tensile_Value'], column_letter['Pinhole_Status'], hidden=True)

        return workbook

    def generate_raw_excel(self, plant):
        save_path = self.save_path
        date_mark = self.date_mark
        mode = self.mode
        month_week = '2W2'#self.month_week
        year = 2025

        file_name = f'MES_{plant}_{mode}_Report_{date_mark}.xlsx'
        excel_file = os.path.join(save_path, file_name)
        with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
            sql = f"""SELECT WorkDate, Machine, Line, Shift, WorkOrder, PartNo, ProductItem, StandardAQL, InspectedAQL,
                        Period, MaxSpeed, MinSpeed, AvgSpeed, StdSpeed, CountingQty, OnlinePacking, WIPPacking, Target, ScrapQuantity, FaultyQuantity, RunTime, StopTime, 60 as AllTime, c.MonthWeek,
                        Tensile_Value,Tensile_Limit,Tensile_Status,Elongation_Value,Elongation_Limit,Elongation_Status,
                        Roll_Value,Roll_Limit,Roll_Status,Cuff_Value,Cuff_Limit,Cuff_Status,Palm_Value,Palm_Limit,Palm_Status,
                        Finger_Value,Finger_Limit,Finger_Status,FingerTip_Value,FingerTip_Limit,FingerTip_Status,
                        Length_Value,Length_Limit,Length_Status, Weight_Value, Weight_Limit, Weight_Status, Width_Value,Width_Limit,Width_Status,
                        Pinhole_Value,Pinhole_Limit,Pinhole_Status
                        FROM [MES_OLAP].[dbo].[counting_daily_info_raw] c
                        LEFT JOIN [MES_OLAP].[dbo].[mes_ipqc_data] ipqc on c.Runcard = ipqc.Runcard
                        where c.Year = {year} and c.MonthWeek = '{month_week}'  
                        and c.branch like'%{plant}%' and Machine not in ('VN_GD_PVC1_L03','VN_GD_PVC1_L04')
                        --and StandardAQL is not Null and InspectedAQL is not null 
                        order by Machine, WorkDate, Cast(Period as Int), Line
                        """
            data = self.db.select_sql_dict(sql)
            df = pd.DataFrame(data)

            # 設定IPQC欄位判斷條件
            df['IPQC'] = df[
                ['Tensile_Status', 'Elongation_Status', 'Roll_Status', 'Cuff_Status', 'Palm_Status',
                 'Finger_Status', 'FingerTip_Status', 'Length_Status', 'Weight_Status', 'Width_Status',
                 'Pinhole_Status']].apply(lambda row: 'NG' if 'NG' in row.values else 'PASS', axis=1)
            df['SeparateQty'] = df.apply(
                lambda row: row['OnlinePacking'] + row['WIPPacking'] if row['IPQC'] == 'NG' else None, axis=1)

            machine_groups = df.groupby('Machine')
            summary_df = self.generate_summary(writer, machine_groups)

            for machine_name, machine_df in machine_groups:
                self.generate_excel(writer, machine_df, machine_name)

            self.delete_counting_weekly_info_raw(plant, year, month_week)
            self.insert_counting_weekly_info_raw(plant, year, month_week, summary_df)

            self.generate_12aspect_excel(writer, plant)
        # self.file_list.append(excel_file)

    def generate_12aspect_excel(self, writer, plant):
        colmn_letter = {'Plant': 'A', 'Year': 'B', 'MonthWeek': 'C', 'CountingQty': 'D', 'SeparateQty': 'E', 'AvgSpeed': 'F',
                        'Activation': 'G', 'Capacity': 'H', 'Yield': 'I', 'OEE': 'J', 'SeparateRate': 'K', 'ScrapRate': 'L',
                        'Target': 'M', 'OnlinePacking': 'N',}

        sql = f"""
            SELECT [Plant], [Year], [MonthWeek]
              ,[CountingQty],[SeparateQty] SeparateQuantity,[AvgSpeed]
              ,[Activation],[Capacity],[Yield]
              ,[OEE],[SeparateRate],[ScrapRate],[Target],[OnlinePacking]
          FROM [MES_OLAP].[dbo].[counting_weekly_info_raw] where Plant = '{plant}'
        """
        data = self.db.select_sql_dict(sql)
        df = pd.DataFrame(data)

        df.rename(columns=self.header_columns, inplace=True)

        sheet_name = "週累計"
        df.to_excel(writer, sheet_name=sheet_name, index=False)



        # Read the written Excel file
        workbook = writer.book
        worksheet = writer.sheets[sheet_name]

        # Apply Header Style
        for cell in worksheet[1]:  # First line is Header
            cell.font = self.header_font
            cell.alignment = self.header_alignment
            cell.border = self.header_border
            # Formatting

        for col in worksheet.columns:
            max_length = max(len(str(cell.value)) for cell in col)
            col_letter = col[0].column_letter

            worksheet.column_dimensions[col_letter].width = max_length + 5

            # Set alignment
            for cell in col:
                if col_letter in [colmn_letter['CountingQty'], colmn_letter['SeparateQty'],
                                  colmn_letter['OnlinePacking'], colmn_letter['Target']]:  # Apply right alignment for specific columns
                    cell.number_format = '#,##0'
                    cell.alignment = self.right_align_style.alignment
                elif col_letter in [colmn_letter['Activation'], colmn_letter['Capacity'], colmn_letter['Yield'],
                                    colmn_letter['OEE'], colmn_letter['SeparateRate'], colmn_letter['ScrapRate']]:
                    worksheet.column_dimensions[col_letter].width = 15
                    cell.alignment = self.center_align_style.alignment
                    cell.number_format = '0.0%'
                else:
                    cell.alignment = self.center_align_style.alignment

    def generate_summary(self, writer, machine_groups):
        colmn_letter = {'Name': 'A', 'Week': 'B', 'shift': 'C', 'Line': 'D', 'CountingQty': 'E', 'FaultyQty': 'F', 'ScrapQty': 'G',
                        'SeparateQty': 'H', 'OnlinePacking': 'I', 'RunTime': 'J', 'AllTime': 'K', 'Activation': 'L',
                        'StdSpeed': 'M', 'Target': 'N', 'Capacity': 'O', 'Yield': 'P', 'OEE': 'Q', 'Achievement': 'R'}
        summary_data = []
        tmp_date = self.date_mark.replace('_', '~')
        mode = self.mode
        this_start_date = self.this_start_date

        if mode == 'WEEKLY':
            tmp_week = f"第{self.last_week_info}週({tmp_date})"
        else:
            tmp_week = f"{this_start_date.strftime('%Y %m')}({tmp_date})"

        thin_border_top = Border(top=Side(style="thin"))
        thin_border_bottom = Border(bottom=Side(style="thin"))
        for machine_name, machine_df in machine_groups:
            for shift in machine_df['Shift'].unique():  # Loop through each unique Line
                for line in machine_df['Line'].unique():
                    filtered_df = machine_df[(machine_df['Line'] == line) & (machine_df['Shift'] == shift)]
                    countingQty = filtered_df['CountingQty'].sum()
                    faultyQty = filtered_df['FaultyQuantity'].sum()
                    scrapQty = filtered_df['ScrapQuantity'].sum()
                    separateQty = filtered_df['SeparateQty'].sum()
                    onlinePacking = filtered_df['OnlinePacking'].sum()
                    runTime = filtered_df['RunTime'].sum()
                    stopTime = filtered_df['StopTime'].sum()
                    allTime = filtered_df['AllTime'].sum()
                    stdSpeed = round(filtered_df['StdSpeed'].mean(), 0)
                    target = filtered_df['Target'].sum()
                    rate = round((int(onlinePacking) / int(target)), 3) if int(target) > 0 else 0

                    summary_row = {
                        'Name': machine_name,
                        'Date': tmp_week,
                        'Shift': shift,
                        'Line': line,
                        'CountingQty': countingQty,
                        'FaultyQuantity': faultyQty,
                        'ScrapQuantity': scrapQty,
                        'SeparateQuantity': separateQty,
                        'OnlinePacking': onlinePacking,
                        'RunTime': runTime,
                        'AllTime': allTime,
                        'Activation': '',
                        'StandSpeed': stdSpeed,
                        'Target': target,
                        'Capacity': '',
                        'Yield': '',
                        'OEE': '',
                        'Achievement Rate': rate
                    }
                    summary_data.append(summary_row)

            # Summary Row
            countingQty = sum(item['CountingQty'] for item in summary_data if item['Name'] == machine_name)
            faultyQty = sum(item['FaultyQuantity'] for item in summary_data if item['Name'] == machine_name)
            scrapQty = sum(item['ScrapQuantity'] for item in summary_data if item['Name'] == machine_name)
            separateQty = sum(item['SeparateQuantity'] for item in summary_data if item['Name'] == machine_name)
            onlinePacking = sum(item['OnlinePacking'] for item in summary_data if item['Name'] == machine_name)
            runTime = sum(item['RunTime'] for item in summary_data if item['Name'] == machine_name)
            allTime = sum(item['AllTime'] for item in summary_data if item['Name'] == machine_name)

            stdSpeed_values = [item['StandSpeed'] for item in summary_data if item['Name'] == machine_name]
            stdSpeed = round(sum(stdSpeed_values) / len(stdSpeed_values), 0) if stdSpeed_values else 0

            target = sum(item['Target'] for item in summary_data if item['Name'] == machine_name)
            activation = round(runTime/allTime, 3) if int(allTime) > 0 else 0
            output = countingQty+faultyQty+scrapQty
            capacity = round(output/target, 3) if int(target) > 0 else 0
            _yield = round((onlinePacking-separateQty)/output, 3) if int(output) > 0 else 0
            oee = round(activation * capacity * _yield, 3)
            rate = round(onlinePacking/target, 3) if int(target) > 0 else 0

            summary_data.append({'Name': machine_name, 'Date': tmp_week, 'Shift': '', 'Line': '',
                                 'CountingQty': countingQty, 'FaultyQuantity': faultyQty, 'ScrapQuantity': scrapQty,
                                 'SeparateQuantity': separateQty, 'OnlinePacking': onlinePacking,
                                 'RunTime': runTime, 'AllTime': allTime, 'StandSpeed': stdSpeed, 'Target': target,
                                 'Activation': activation, 'Capacity': capacity, 'Yield': _yield, 'OEE': oee, 'Achievement Rate': rate})
        summary_df = pd.DataFrame(summary_data)
        # Change column names
        summary_df.rename(columns=self.header_columns, inplace=True)
        summary_sheet_name = "Summary"
        summary_df.to_excel(writer, sheet_name=summary_sheet_name, index=False)
        # Read the written Excel file
        workbook = writer.book
        worksheet = writer.sheets[summary_sheet_name]

        # Apply Header Style
        for cell in worksheet[1]:  # First line is Header
            cell.font = self.header_font
            cell.alignment = self.header_alignment
            cell.border = self.header_border
            # Formatting

        for col in worksheet.columns:
            max_length = max(len(str(cell.value)) for cell in col)
            col_letter = col[0].column_letter

            worksheet.column_dimensions[col_letter].width = max_length + 5

            # Set alignment
            for cell in col:
                if col_letter in [colmn_letter['CountingQty'], colmn_letter['FaultyQty'], colmn_letter['ScrapQty'], colmn_letter['SeparateQty'],
                                  colmn_letter['OnlinePacking'], colmn_letter['Target'], colmn_letter['RunTime'], colmn_letter['AllTime']]:  # Apply right alignment for specific columns
                    cell.number_format = '#,##0'
                    cell.alignment = self.right_align_style.alignment
                elif col_letter in [colmn_letter['Activation'], colmn_letter['Capacity'], colmn_letter['Yield'], colmn_letter['OEE'], colmn_letter['Achievement']]:
                    worksheet.column_dimensions[col_letter].width = 15
                    cell.alignment = self.center_align_style.alignment
                    cell.number_format = '0.0%'
                else:
                    cell.alignment = self.center_align_style.alignment

        name_col = 1
        current_name = None
        group_start = None

        for row in range(2, len(summary_df) + 2):  # Data starts from row 2 in Excel
            name = worksheet.cell(row=row, column=name_col).value

            if name != current_name:
                # Finalize the previous group
                if group_start is not None:
                    last_row = row - 1
                    for col in range(1, summary_df.shape[1] + 1):  # Apply border to all columns
                        worksheet.cell(row=last_row, column=col).border = thin_border_top + thin_border_bottom
                    if row - group_start > 1:
                        worksheet.row_dimensions.group(group_start, row - 2, hidden=True)

                # Start a new group
                current_name = name
                group_start = row

        # Finalize the last group
        if group_start is not None:
            last_row = len(summary_df) + 1
            for col in range(1, summary_df.shape[1] + 1):
                worksheet.cell(row=last_row, column=col).border = thin_border_top + thin_border_bottom
            if len(summary_df) + 1 - group_start > 1:
                worksheet.row_dimensions.group(group_start, len(summary_df) + 1 - 1, hidden=True)

        comment = Comment(text="機台運作時間/可運轉時間", author="System")
        comment.width = 600
        worksheet[colmn_letter['Activation'] + "1"].comment = comment

        comment = Comment(text="(點數機數量+二級品數量+廢品數量)/目標產能", author="System")
        comment.width = 600
        worksheet[colmn_letter['Capacity'] + "1"].comment = comment

        comment = Comment(text="(包裝確認量-隔離品數量)/(點數機數量+二級品數量+廢品數量)", author="System")
        comment.width = 600
        worksheet[colmn_letter['Yield'] + "1"].comment = comment

        comment = Comment(text="稼動率*效能效率*良率", author="System")
        comment.width = 600
        worksheet[colmn_letter['OEE'] + "1"].comment = comment

        comment = Comment(text="包裝確認量/預估產能", author="System")
        comment.width = 600
        worksheet[colmn_letter['Achievement'] + "1"].comment = comment

        comment = Comment(text="IPQC其中一項判定不合格", author="System")
        comment.width = 600
        worksheet[colmn_letter['SeparateQty'] + "1"].comment = comment


        return summary_df

    def generate_chart(self, plant):
        this_start_date = self.this_start_date
        this_end_date = self.this_end_date
        last_start_date = self.last_start_date
        last_end_date = self.last_end_date
        mode = self.mode
        save_path = self.save_path
        date_mark = self.date_mark
        last_week_info = self.last_week_info
        yticks_labels = []

        plt.rcParams['font.sans-serif'] = ['Microsoft YaHei']

        sql = f"""SELECT name,
                    sum(case when cast(belong_to as date) between '{this_start_date}' and '{this_end_date}' then sum_qty else 0 end) as this_time,
                    sum(case when cast(belong_to as date) between '{this_start_date}' and '{this_end_date}' then target else 0 end) as target_this_time,
                    sum(case when cast(belong_to as date) between '{last_start_date}' and '{last_end_date}' then sum_qty else 0 end) as last_time,
                    sum(case when cast(belong_to as date) between '{last_start_date}' and '{last_end_date}' then target else 0 end) as target_last_time,
                    (sum(case when cast(belong_to as date) between '{this_start_date}' and '{this_end_date}' then target else 0 end) -
                    sum(case when cast(belong_to as date) between '{this_start_date}' and '{this_end_date}' then sum_qty else 0 end)) as this_unfinish,
                    (sum(case when cast(belong_to as date) between '{last_start_date}' and '{last_end_date}' then target else 0 end) -
                    sum(case when cast(belong_to as date) between '{last_start_date}' and '{last_end_date}' then sum_qty else 0 end)) as last_unfinish
                    FROM [MES_OLAP].[dbo].[mes_daily_report_raw]
                    where name like '%{plant}%'
                    and name not in ('VN_GD_PVC1_L03','VN_GD_PVC1_L04')
                    group by name
                    order by name"""
        data = self.db.select_sql_dict(sql)

        # Output Bar Chart
        x_labels = [str(item['name']).split('_')[-1] for item in data]
        x_range = range(0, len(x_labels) * 2, 2)

        this_data = [int(item['this_time']) for item in data]
        this_unfinish = [int(item['this_unfinish']) if int(item['this_unfinish']) > 0 else 0 for item in data]

        max_data = max(this_data, default=0)
        step_data = 10

        rounded_max_data = int(
            (((max_data / (10 ** (len(str(max_data)) - 2))) // step_data) * step_data + step_data) * (
                    10 ** (len(str(max_data)) - 2)))
        rounded_step_data = step_data * (10 ** (len(str(max_data)) - 2))

        this_rate = [round((item['this_time'] / item['target_this_time']) * 100, 2) if int(
            item['target_this_time']) > 0 else 0 for item in data]
        last_rate = [round((item['last_time'] / item['target_last_time']) * 100, 2) if int(
            item['target_last_time']) > 0 else 0 for item in data]
        max_rate = max(max(this_rate, default=0), max(last_rate, default=0))
        rounded_max_rate = (math.ceil(max_rate / 10) * 10)
        rounded_step_rate = 20

        bar_width = 0.6
        plt.figure(figsize=(16, 9))
        fig, ax1 = plt.subplots(figsize=(16, 9))
        if mode == "WEEKLY":

            this_month_bars = ax1.bar([i for i in x_range], this_data, width=bar_width,
                                      label=f"週產量",
                                      align='center', color='#10ba81')
            unfinish_bars = ax1.bar([i for i in x_range], this_unfinish, width=bar_width, bottom=this_data,
                                      label=f"週目標差異",
                                      align='center', color='lightgreen')
        if mode == "MONTHLY":

            this_month_bars = ax1.bar([i for i in x_range], this_data, width=bar_width,
                                      label=f"{this_start_date.strftime('%Y %m')}月產量", align='center', color='#10ba81')

            unfinish_bars = ax1.bar([i for i in x_range], this_unfinish, width=bar_width, bottom=this_data,
                                      label=f"{this_start_date.strftime('%Y %m')}月目標差異", align='center', color='lightgreen')

        ax1.set_xticks(x_range)
        ax1.set_xticklabels(x_labels, rotation=0, ha="center", fontsize=12)

        if len(str(max_data)) > 7:
            yticks_positions = list(range(0, rounded_max_data + 1 * rounded_step_data, rounded_step_data))
            yticks_positions.append(int(rounded_max_data + 2 * rounded_step_data))
            yticks_labels = [f"{int(i//(10**(len(str(max_data)))))}" + '百萬' if len(str(i)) > 6 else f"{i}" for i
                             in yticks_positions]

            for index, bar in enumerate(this_month_bars):
                height = bar.get_height()
                unfinish_height = unfinish_bars[index].get_height()
                ax1.text(
                    bar.get_x() + bar.get_width() / 2,
                    height+unfinish_height,
                    f'{round(height/(10**(len(str(max_data)) - 2)),2)}'[:4] if height > 0 else '',
                    ha='center', va='bottom', fontsize=12  # Align the text
                )
        elif 4 < len(str(max_data)) <= 7:
            yticks_positions = list(range(0, rounded_max_data + 1 * rounded_step_data, rounded_step_data))
            yticks_positions.append(int(rounded_max_data + 4 * rounded_step_data))
            yticks_labels = [f"{int(i//(10**(len(str(max_data)) - 1)))}" + '百萬' if len(str(i)) > 4 and int(
                i // (10 ** (len(str(max_data))))) % 60 == 0 else "" for i in yticks_positions]

            for index, bar in enumerate(this_month_bars):
                height = bar.get_height()
                unfinish_height = unfinish_bars[index].get_height()
                ax1.text(
                    bar.get_x() + bar.get_width() / 2,
                    height+unfinish_height,
                    f'{round(height/(10**(len(str(max_data)) - 1)),2)}'[:4] if height > 0 else '',
                    ha='center', va='bottom', fontsize=12  # Align the text
                )
        yticks_labels[-1] = ""
        ax1.set_yticks(yticks_positions)
        ax1.set_yticklabels(yticks_labels, fontsize=12)

        # Achievement Rate Line Chart (橘色的線)
        ax2 = ax1.twinx()
        if mode == "WEEKLY":
            # line_label = f"{this_start_date.strftime('%d/%m')}-{this_end_date.strftime('%d/%m')}"
            sr_achieve_rate = "本週達成率"
            name = f"{this_start_date.strftime('%d/%m')}-{this_end_date.strftime('%d/%m')}"
            # name1 = f"{last_start_date.strftime('%d/%m')}-{last_end_date.strftime('%d/%m')}"
            # filtered_data = [(x, rate) for x, rate in zip(x_range, last_rate) if rate != 0]
            # x_filtered, last_rate_filtered = zip(*filtered_data)
            # last_rate_line = ax2.plot(x_filtered, last_rate_filtered,
            #                           label=f"{last_start_date.strftime('%d/%m')}-{last_end_date.strftime('%d/%m')}",
            #                           color='#F8CBAD', marker='o', linewidth=1.5)
            filtered_data = [(x, rate) for x, rate in zip(x_range, this_rate) if rate != 0]  # 折線圖上的文字
            x_filtered, this_rate_filtered = zip(*filtered_data)
            this_rate_line = ax2.plot(x_filtered, this_rate_filtered,
                                      label=sr_achieve_rate,
                                      color='#ED7D31', marker='o', linewidth=1.5)

        if mode == "MONTHLY":
            # line_label = f"{this_start_date.strftime('%B %Y')}"
            sr_achieve_rate = "本月達成率"
            name = f"{this_start_date.strftime('%Y %m')}"
            # name1 = f"{last_start_date.strftime('%B %Y')}"
            # filtered_data = [(x, rate) for x, rate in zip(x_range, last_rate) if rate != 0]
            # x_filtered, last_rate_filtered = zip(*filtered_data)
            # last_rate_line = ax2.plot(x_filtered, last_rate_filtered, label=f"{last_start_date.strftime('%B %Y')}",
            #                           color='#F8CBAD', marker='o', linewidth=1.5)
            filtered_data = [(x, rate) for x, rate in zip(x_range, this_rate) if rate != 0]
            x_filtered, this_rate_filtered = zip(*filtered_data)
            this_rate_line = ax2.plot(x_filtered, this_rate_filtered,
                                      label=sr_achieve_rate,
                                      color='#ED7D31', marker='o', linewidth=1.5)

        # Label Name
        sr_target = "達成率目標%"
        # Chart Label
        ry_label = "達成率(%)"
        ly_label = "Product (百萬)"
        if self.mode == "WEEKLY":
            name = f"{this_start_date.strftime('%m/%d')}-{this_end_date.strftime('%m/%d')}"
            title = f"\n{plant} 第{last_week_info}週({name})目標達成率\n"
        else:
            name = f"{this_start_date.strftime('%Y %m')}"
            title = f"\n{plant} {name}月目標達成率\n"

        # Achievement Rate Standard Line
        if plant == "NBR":
            target_rate = 95
        else:
            target_rate = 98
        yticks_positions = list(range(0, rounded_max_rate + 1 * rounded_step_rate, rounded_step_rate))
        if target_rate not in yticks_positions:
            yticks_positions.append(target_rate)
        yticks_positions.append(rounded_max_rate + 0.25 * rounded_step_rate)
        yticks_positions = sorted(yticks_positions)

        yticks_labels = [f"{i}" + '%' for i in yticks_positions]
        yticks_labels[-1] = ""
        ax2.set_yticks(yticks_positions)
        ax2.set_yticklabels(yticks_labels, fontsize=12)
        ax2.axhline(y=target_rate, color='red', linestyle='--', linewidth=1, label=sr_target)
        #ax2.axhline(y=95, color='red', linestyle='--', linewidth=1)

        #ax1.set_xlabel(f'{plant} (line)', labelpad=10, fontsize=12)
        ax1.xaxis.set_label_coords(0.975, -0.014)
        ax1.set_ylabel('Product output', fontsize=12)
        ax2.set_ylabel('Archive rates', fontsize=12)

        # Achievement Rate Label 線上的數字
        # for i, value in enumerate(last_rate):
        #     ax2.text(
        #         x_range[i],
        #         value + 1.5 if value > this_rate[i] else value - 3.5,
        #         f"{value:.1f}" if value != 0 else '',
        #         ha='center', va='bottom', fontsize=8, color='white',
        #         bbox=dict(facecolor='#F8CBAD', edgecolor='none', boxstyle='round,pad=0.3')
        #     )

        for i, value in enumerate(this_rate):
            ax2.text(
                x_range[i],
                value + 1.5 if value > last_rate[i] else value - 3.5,
                f"{value:.1f}%" if value != 0 else '',
                ha='center', va='bottom', fontsize=12, color='white',
                bbox=dict(facecolor='#ED7D31', edgecolor='none', boxstyle='round,pad=0.3')
            )

        handles1, labels1 = ax1.get_legend_handles_labels()
        handles2, labels2 = ax2.get_legend_handles_labels()
        fig.legend(
            handles1 + handles2,
            labels1 + labels2,
            loc='center left',
            fontsize=12,
            title="Note",
            title_fontsize=12,
            bbox_to_anchor=(1.0, 0.5),
            ncol=1
        )

        # Customize axes and borders
        ax = plt.gca()  # Get current axes
        ax.spines['top'].set_color('white')
        ax.spines['top'].set_linewidth(1.5)

        ax1.tick_params(axis='y', colors='black')  # Y-axis ticks color
        y_tick_lines = ax1.get_yticklines()
        y_tick_lines[-2].set_visible(False)
        y_tick_lines[-1].set_visible(False)
        ax1.annotate(
            '',
            xy=(0, 1.0),
            xytext=(0, 0.98),
            xycoords='axes fraction',
            arrowprops=dict(facecolor='black', arrowstyle='-|>,widthA=0.4,widthB=1.4', linewidth=0.5)
        )
        ax1.set_ylabel(ly_label, labelpad=20, rotation=0)
        ax1.yaxis.set_label_coords(-0.01, 1.01)

        ax2.tick_params(axis='y', colors='black')
        y_tick_lines = ax2.get_yticklines()
        y_tick_lines[-2].set_visible(False)
        y_tick_lines[-1].set_visible(False)
        ax2.annotate(
            '',
            xy=(1, 1.0),
            xytext=(1, 0.98),
            xycoords='axes fraction',
            arrowprops=dict(facecolor='black', arrowstyle='-|>,widthA=0.4,widthB=1.4', linewidth=0.5)
        )

        ax2.set_ylabel(ry_label, labelpad=20, rotation=0)
        ax2.yaxis.set_label_coords(1.01, 1.03)

        # plt.text(
        #     x_range[-1] / 2,
        #     -rounded_max_rate * 0.125,
        #     title,
        #     fontsize=16, color='black', ha='center', va='center'
        # )
        plt.title(title, fontsize=20)

        plt.tight_layout()

        file_name = f'MES_{plant}_{mode}_{date_mark}_Chart.png'
        chart_img = os.path.join(save_path, file_name)

        plt.savefig(f"{chart_img}", dpi=100, bbox_inches="tight", pad_inches=0.45)
        plt.close()
        self.image_buffers.append(chart_img)

    # 過濾掉無效數據 (scrap 和 secondgrade)
    def filter_valid_data(self, x_range, y_values):
        filtered_x = [x for i, x in enumerate(x_range) if
                      y_values[i] is not None and not np.isnan(y_values[i])]
        filtered_y = [y for y in y_values if y is not None and not np.isnan(y)]
        return filtered_x, filtered_y

    def rate_chart(self, plant):
        this_start_date = self.this_start_date
        this_end_date = self.this_end_date
        date_mark = self.date_mark
        save_path = self.save_path

        if plant == 'NBR':
            plant_ = 'NBR'
        else:
            plant_ = 'PVC1'

        data1 = self.mach_list

        sql = f"""WITH raw_data AS (
                        SELECT name, date, 
                               CAST(Scrap AS FLOAT) AS Scrap, 
                               CAST(SecondGrade AS FLOAT) AS SecondGrade,
                               CAST(sum_qty as Float) as sum_qty
                        FROM [MES_OLAP].[dbo].[mes_daily_report_raw]
                        WHERE belong_to between '{this_start_date}' AND '{this_end_date}'
                    )
                    SELECT 
                        name,
                        SUM(Case when Scrap > 0 then Scrap else 0 end) AS scrap,
                        SUM(case when SecondGrade > 0 then SecondGrade else 0 end) AS secondgrade,
                        SUM(case when sum_qty > 0 then sum_qty else 0 end) as sum_qty
                    FROM raw_data
                    WHERE name like '%{plant_}%'
                    GROUP BY name
                    ORDER BY name;
                """
        data2 = vnedc_database().select_sql_dict(sql)
        data2_dict = {item['name']: item for item in data2}
        data = []
        for item in data1:
            name = item['name']
            if name in data2_dict:
                data.append(data2_dict[name])
            else:
                data.append({'name': name, 'scrap': 0, 'secondgrade': 0, 'sum_qty': 0})

        x_labels = [str(item['name']).split('_')[-1] for item in data]
        x_range = range(len(x_labels))

        scrap = [round((item['scrap'] / item['sum_qty']) * 100, 2) if item['sum_qty'] > 0 else 0 for item in data]
        secondgrade = [round((item['secondgrade'] / item['sum_qty']) * 100, 2) if item['sum_qty'] > 0 else 0 for item in
                       data]

        # 過濾 scrap 數據
        filtered_x_scrap, filtered_scrap = self.filter_valid_data(x_range, scrap)

        # 過濾 secondgrade 數據
        filtered_x_secondgrade, filtered_secondgrade = self.filter_valid_data(x_range, secondgrade)

        # Create the figure and subplots
        plt.figure(figsize=(16, 9))
        fig = plt.figure(figsize=(16, 9))
        gs = gridspec.GridSpec(2, 1, height_ratios=[1, 1], hspace=0)  # No space between subplots

        # Subplot for scrap
        y_max1 = 3
        y_ticks = np.arange(0, y_max1, 0.4)  # 分成10個刻度
        y_ticks = y_ticks[:-1]

        ax1 = fig.add_subplot(gs[0])
        ax1.plot(filtered_x_scrap, filtered_scrap, label="廢品率 (%)", marker='o', linestyle='-', color='#ED7D31',
                 linewidth=2)
        ax1.axhline(y=0.8, color='#ED7D31', linestyle='--', linewidth=1.5, label="廢品標準線(0.8)")
        ax1.set_xticks(x_range)
        ax1.set_xticklabels([])  # Hide x-axis labels for the top plot
        # ax1.set_ylabel('廢品率 (%)', fontsize=12, rotation=0)
        ax1.text(-0.1, 0.6, '廢品率 (%)', fontsize=12, rotation=0, ha='center', va='center', transform=ax1.transAxes)
        ax1.set_ylim(0, y_max1)
        ax1.set_yticks(y_ticks)
        ax1.yaxis.set_major_formatter(FuncFormatter(self.add_percent))

        offset = 0.03
        for i, scrap_val in enumerate(filtered_scrap):
            ax1.text(filtered_x_scrap[i], scrap_val+offset, f"{scrap_val:.2f}%", ha='center', va='bottom', fontsize=12,
                     color='#ED7D31')

        # Subplot for secondgrade
        y_max2 = 1.6
        y_ticks = np.arange(0, y_max2, 0.2)  # 分成10個刻度
        y_ticks = y_ticks[:-1]

        ax2 = fig.add_subplot(gs[1])
        ax2.plot(filtered_x_secondgrade, filtered_secondgrade, label="二級品率 (%)", marker='o', linestyle='-',
                 color='#70AD47', linewidth=2)
        ax2.axhline(y=0.2, color='#70AD47', linestyle='--', linewidth=1.5, label="二級品標準線(0.2)")
        ax2.set_xticks(x_range)
        ax2.set_xticklabels(x_labels, rotation=0, ha="center", fontsize=12)
        # ax2.set_ylabel('二級品率 (%)', fontsize=12, rotation=0)
        ax2.text(-0.1, 0.6, '二級品率 (%)', fontsize=12, rotation=0, ha='center', va='center', transform=ax2.transAxes)
        ax2.set_ylim(0, y_max2)
        ax2.set_yticks(y_ticks)
        ax2.yaxis.set_major_formatter(FuncFormatter(self.add_percent))

        offset = 0.03
        for i, secondgrade_val in enumerate(filtered_secondgrade):
            ax2.text(filtered_x_secondgrade[i], secondgrade_val+offset, f"{secondgrade_val:.2f}%", ha='center', va='bottom',
                     fontsize=12, color='#70AD47')

        # Add a title for the entire figure
        fig.suptitle(f"二級品及廢品率 ({plant})", fontsize=20)

        # Add legend for both plots
        handles1, labels1 = ax1.get_legend_handles_labels()
        handles2, labels2 = ax2.get_legend_handles_labels()
        fig.legend(
            handles1 + handles2,
            labels1 + labels2,
            loc='center left',
            fontsize=10,
            title="Note",
            title_fontsize=12,
            bbox_to_anchor=(1.0, 0.5),
            ncol=1
        )

        plt.tight_layout()

        file_name = f'MES_{plant}_{date_mark}_Rate_Chart_Line.png'
        chart_img = os.path.join(save_path, file_name)

        plt.savefig(chart_img, dpi=100, bbox_inches="tight", pad_inches=0.45)
        plt.close()

        self.image_buffers.append(chart_img)

    def chart_table(self, plant, x_labels, secondgrade, scrap):
        date_mark = self.date_mark
        save_path = self.save_path

        table_data = [
            [plant] + x_labels,
            ["廢品(%)"] + scrap,
            ["二級品(%)"] + secondgrade,
        ]

        # 創建圖表
        fig, ax = plt.subplots(figsize=(12, 3))  # 調整尺寸以適配內容
        ax.axis("tight")
        ax.axis("off")

        # 添加表格
        table = ax.table(cellText=table_data, loc="center", cellLoc="center")

        font_path = font_manager.findfont("Microsoft YaHei")
        font_prop = font_manager.FontProperties(fname=font_path, size=14)
        header_color = "#0A4E9B"
        row_colors = ["#E8F3FF", "#FFFFFF"]

        for (row, col), cell in table.get_celld().items():
            cell.set_width(0.1)
            # Header styling
            if row == 0:
                cell.set_facecolor(header_color)
                cell.set_text_props(color="white", weight="bold", fontproperties=font_prop)
            # Row styling
            else:
                cell.set_facecolor(row_colors[row % 2])
                cell.set_text_props(fontproperties=font_prop)
            # Adjust height for all cells
            cell.set_height(0.2)

        plt.tight_layout(rect=[0, 0, 0, 0])
        file_name = f'MES_{plant}_{date_mark}_Rate_Chart_Table.png'
        chart_img = os.path.join(save_path, file_name)
        plt.savefig(chart_img, bbox_inches="tight", dpi=100, pad_inches=0)
        plt.close()

        self.image_buffers.append(chart_img)

    def send_email(self, file_list, image_buffers):
        mode = self.mode
        date_mark = self.date_mark
        this_start_date = self.this_start_date
        logging.info(f"Start to send Email")
        smtp_config, to_emails, admin_emails = self.read_config('mes_weekly_report_mail.config')

        # SMTP Sever config setting
        smtp_server = smtp_config.get('smtp_server')
        smtp_port = int(smtp_config.get('smtp_port', 587))
        smtp_user = smtp_config.get('smtp_user')
        smtp_password = smtp_config.get('smtp_password')
        sender_alias = "GD Report"
        sender_email = smtp_user
        # Mail Info
        msg = MIMEMultipart()
        msg['From'] = f"{sender_alias} <{sender_email}>"
        msg['To'] = ', '.join(to_emails)

        if mode == "WEEKLY":
            msg['Subject'] = f'[GD Report] 第{self.last_week_info}週達成率報表 {date_mark}'
        elif mode == "MONTHLY":
            name = f"{this_start_date.strftime('%Y %m')}"
            msg['Subject'] = f'[GD Report] {name}月達成率報表 {date_mark}'

        # Mail Content
        html = """\
                <html>
                  <body>
                """
        for i in range(len(image_buffers)):
            html += f'<img src="cid:chart_image{i}"><br>'

        html += """\
                  </body>
                </html>
                """

        msg.attach(MIMEText(html, 'html'))

        # Attach Excel
        for file_path in file_list:
            if os.path.exists(file_path):
                with open(file_path, "rb") as attachment:
                    part = MIMEBase("application", "octet-stream")
                    part.set_payload(attachment.read())
                encoders.encode_base64(part)
                part.add_header(
                    "Content-Disposition",
                    f"attachment; filename={os.path.basename(file_path)}"
                )
                msg.attach(part)
            else:
                print(f"File not found: {file_path}")
        logging.info(f"Attached Excel files")

        # Attach Picture
        for idx, image_path in enumerate(image_buffers):
            if os.path.exists(image_path):
                with open(image_path, "rb") as img_file:
                    img = MIMEImage(img_file.read())
                img.add_header("Content-ID", f"<chart_image{idx}>")
                msg.attach(img)
            else:
                logging.info(f"Image not found: {image_path}")
                print(f"Image not found: {image_path}")
        logging.info(f"Attached Picture")

        # Send Email
        try:
            # server = smtplib.SMTP(smtp_server, smtp_port)
            # server.starttls()  # 启用 TLS 加密
            # server.login(smtp_user, smtp_password)  # 登录到 SMTP 服务器
            # server.sendmail(smtp_user, to_emails, msg.as_string())
            # server.quit()

            # 發送郵件（不進行密碼驗證）
            server = smtplib.SMTP(smtp_server, smtp_port)
            server.ehlo()  # 啟動與伺服器的對話
            server.sendmail(smtp_user, to_emails, msg.as_string())
            print("Sent Email Successfully")
        except Exception as e:
            print(f"Sent Email Fail: {e}")
            logging.info(f"Sent Email Fail: {e}")
        finally:
            attachment.close()

    def read_config(self, config_file):
        smtp_config = {}
        to_emails = []
        admin_emails = []

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
                        smtp_config[key.strip()] = value.strip()
                elif current_section == 'recipients':
                    to_emails.append(line)
                elif current_section == 'admin_email':
                    admin_emails.append(line)

        return smtp_config, to_emails, admin_emails

    def generate_previous_weeks_with_dates(self):
        weeks_to_generate = 15
        # 獲取今天的日期
        today = datetime.now()

        # 計算上一週的週一（開始日期）和週日（結束日期）
        last_week_start = today - timedelta(days=today.weekday() + 7)  # 上週週一
        last_week_end = last_week_start + timedelta(days=6)  # 上週週日

        # 獲取上一週的年份與週數
        year, week = last_week_start.isocalendar()[:2]

        weeks_list = []
        week_dates = []

        # 添加上一週及其日期範圍
        weeks_list.append(f"W{week}")
        week_dates.append([last_week_start.date(), last_week_end.date()])

        # 向前推算其餘 51 週
        for _ in range(weeks_to_generate-1):
            # 向前推一週
            last_week_start -= timedelta(weeks=1)
            last_week_end = last_week_start + timedelta(days=6)

            # 計算週別
            year, week = last_week_start.isocalendar()[:2]

            # 添加到列表
            #weeks_list.append(f"{year} W{week}")
            weeks_list.append(f"W{week}")
            week_dates.append([last_week_start.date(), last_week_end.date()])

        # 反轉列表，使週別和日期按照時間順序排列
        weeks_list.reverse()
        week_dates.reverse()

        return weeks_list, week_dates

    def add_percent(self, y, pos):
        return f"{y:.1f}%"  # 格式化為整數百分比

    def weekly_chart(self, plant):
        save_path = self.save_path

        data1 = self.mach_list

        for machine in data1:
            try:
                this_data = []
                this_rate = []
                x_labels = []
                week_date = []
                unfinish_data = []

                today = datetime.now()

                if machine['name'] in ['VN_GD_PVC1_L01', 'VN_GD_PVC1_L02']:
                    start_date = date(2025, 1, 6)
                    start_week_number = 1
                elif machine['name'] in ['VN_GD_PVC1_L05', 'VN_GD_PVC1_L06']:
                    start_date = date(2024, 12, 2)
                    start_week_number = 49
                else:
                    # Get the starting week (Sep 30)
                    start_date = date(2024, 9, 30)
                    start_week_number = 40

                x_labels, week_date = self.generate_previous_weeks_with_dates()

                print(machine['name'])
                for i, item in enumerate(week_date):
                    week_name = x_labels[i]
                    if item[0] < start_date:
                        this_data.append(0)
                        this_rate.append(0)
                        unfinish_data.append(0)
                        continue

                    print(f"Week {week_name} - start {item[0]} - end - {item[1]}")
                    sql = f"""
                            SELECT name,qty, target, (case when target-qty > 0 then target-qty else 0 end) unfinish_qty FROM (
                            SELECT name, sum(case when sum_qty > 0 then sum_qty else 0 end) as qty, sum(case when target > 0 then target else 0 end) as target
                            FROM [MES_OLAP].[dbo].[mes_daily_report_raw] where Name = '{machine['name']}'
                            and (belong_to between '{item[0]}' and '{item[1]}')
                            group by name) A"""
                    rows = vnedc_database().select_sql_dict(sql)
                    if len(rows) == 0:
                        this_data.append(0)
                        this_rate.append(0)
                        unfinish_data.append(0)
                    else:
                        this_data.append(rows[0]['qty'])
                        unfinish_data.append(rows[0]['unfinish_qty'])
                        try:
                            rate = int((rows[0]['qty'] / rows[0]['target']) * 100) if rows[0]['target'] > 0 else 100
                        except Exception as e:
                            rate = 100
                            print(f"{e} at {machine['name']} at week start {item[0]} - end {item[1]}")
                            pass
                        this_rate.append(rate)
                    # x_labels.append(f'W {week}')

                x_range = range(0, len(x_labels) * 2, 2)

                max_data = max(this_data, default=0)
                step_data = 10
                rounded_max_data = int(
                    (((max_data / (10 ** (len(str(max_data)) - 2))) // step_data) * step_data + step_data) * (
                                10 ** (len(str(max_data)) - 2)))
                rounded_step_data = step_data * (10 ** (len(str(max_data)) - 2))
                max_rate = max(this_rate, default=0)
                rounded_max_rate = (math.ceil(max_rate / 10) * 10)
                rounded_step_rate = 20

                bar_width = 0.6
                plt.figure(figsize=(24, 9))
                fig, ax1 = plt.subplots(figsize=(24, 9))
                this_month_bars = ax1.bar([i for i in x_range], this_data, width=bar_width,
                                          label=f"週產量", align='center', color='#10ba81')
                unfinish_bars = ax1.bar([i for i in x_range], unfinish_data, width=bar_width, bottom=this_data,
                                        label=f"週目標差異",
                                        align='center', color='lightgreen')
                ax1.set_xticks(x_range)
                ax1.set_xticklabels(x_labels, rotation=0, ha="center", fontsize=12)
                if len(str(max_data)) > 7:
                    yticks_positions = list(range(0, rounded_max_data + 1 * rounded_step_data, rounded_step_data))
                    yticks_positions.append(int(rounded_max_data + 2 * rounded_step_data))
                    yticks_labels = [
                        f"{int(i//(10**(len(str(max_data)) - 2)))}" + '百萬' if len(str(i)) > 6 else f"{i}" for i
                        in yticks_positions]
                    for index, bar in enumerate(this_month_bars):
                        height = bar.get_height()
                        unfinish_height = unfinish_bars[index].get_height()
                        ax1.text(
                            bar.get_x() + bar.get_width() / 2,
                            height + unfinish_height,
                            f'{round(height/(10**(len(str(max_data)) - 2)),2)}'[:4] if height > 0 else '',
                            ha='center', va='bottom', fontsize=12  # Align the text
                        )
                elif 4 < len(str(max_data)) <= 7:
                    yticks_positions = list(range(0, rounded_max_data, rounded_step_data))
                    yticks_positions.append(int(rounded_max_data + 3 * rounded_step_data))
                    # yticks_labels = [f"{int(i//(10**(len(str(max_data)) - 3)))}" + '萬 PCS' if len(str(i)) > 4 and int(
                    #     i // (10 ** (len(str(max_data)) - 3))) % 60 == 0 else "" for i in yticks_positions]
                    yticks_labels = [f"{int(i/1000000)} 百萬" if i > 0 else 0 for i in yticks_positions]
                    for index, bar in enumerate(this_month_bars):
                        height = bar.get_height()
                        unfinish_height = unfinish_bars[index].get_height()
                        ax1.text(
                            bar.get_x() + bar.get_width() / 2,
                            height + unfinish_height,
                            f'{round(height/(10**(len(str(max_data)) - 1)),2)}'[:4] if height > 0 else '',
                            ha='center', va='bottom', fontsize=12  # Align the text
                        )

                yticks_labels[-1] = ""
                ax1.set_yticks(yticks_positions)
                ax1.set_yticklabels(yticks_labels, fontsize=12)

                ax2 = ax1.twinx()
                sr_achieve_rate = "達成率"

                filtered_data = [(x, rate) for x, rate in zip(x_range, this_rate) if rate != 0]
                x_filtered, this_rate_filtered = zip(*filtered_data)
                this_rate_line = ax2.plot(x_filtered, this_rate_filtered,
                                          label=sr_achieve_rate,
                                          color='#ED7D31', marker='o', linewidth=1.5)
                sr_target = "達成率目標"

                ry_label = "達成率(%)"
                ly_label = "Product (百萬)"

                name = f"各週產出量"
                title = f"\n{plant} ({machine['name']})\n"

                if plant == "NBR":
                    target_rate = 95
                else:
                    target_rate = 98

                yticks_positions = list(range(0, rounded_max_rate + 1 * rounded_step_rate, rounded_step_rate))
                if target_rate not in yticks_positions:
                    yticks_positions.append(target_rate)
                yticks_positions.append(rounded_max_rate + 0.25 * rounded_step_rate)
                yticks_positions = sorted(yticks_positions)

                yticks_labels = [f"{i}" + '%' for i in yticks_positions]
                yticks_labels[-1] = ""
                ax2.set_yticks(yticks_positions)
                ax2.set_yticklabels(yticks_labels)
                ax2.axhline(y=target_rate, color='red', linestyle='--', linewidth=1, label=sr_target)

                ax1.xaxis.set_label_coords(0.975, -0.014)
                ax1.set_ylabel('Product output', fontsize=12)
                ax2.set_ylabel('Archive rates', fontsize=12)

                for i, value in enumerate(this_rate):
                    ax2.text(
                        x_range[i],
                        value + 1.5,
                        f"{value:.1f}%" if value != 0 else '',
                        ha='center', va='bottom', fontsize=12, color='white',
                        bbox=dict(facecolor='#ED7D31', edgecolor='none', boxstyle='round,pad=0.3')
                    )

                handles1, labels1 = ax1.get_legend_handles_labels()
                handles2, labels2 = ax2.get_legend_handles_labels()
                fig.legend(
                    handles1 + handles2,
                    labels1 + labels2,
                    loc='center left',
                    fontsize=12,
                    title="Note",
                    title_fontsize=12,
                    bbox_to_anchor=(1.0, 0.5),
                    ncol=1
                )

                # Customize axes and borders
                ax = plt.gca()  # Get current axes
                ax.spines['top'].set_color('white')
                ax.spines['top'].set_linewidth(1.5)

                ax1.tick_params(axis='y', colors='black')  # Y-axis ticks color
                y_tick_lines = ax1.get_yticklines()
                y_tick_lines[-2].set_visible(False)
                y_tick_lines[-1].set_visible(False)
                ax1.annotate(
                    '',
                    xy=(0, 1.0),
                    xytext=(0, 0.98),
                    xycoords='axes fraction',
                    arrowprops=dict(facecolor='black', arrowstyle='-|>,widthA=0.4,widthB=1.4', linewidth=0.5)
                )
                ax1.set_ylabel(ly_label, labelpad=20, rotation=0)
                ax1.yaxis.set_label_coords(-0.01, 1.01)

                ax2.tick_params(axis='y', colors='black')
                y_tick_lines = ax2.get_yticklines()
                y_tick_lines[-2].set_visible(False)
                y_tick_lines[-1].set_visible(False)
                ax2.annotate(
                    '',
                    xy=(1, 1.0),
                    xytext=(1, 0.98),
                    xycoords='axes fraction',
                    arrowprops=dict(facecolor='black', arrowstyle='-|>,widthA=0.4,widthB=1.4', linewidth=0.5)
                )

                ax2.set_ylabel(ry_label, labelpad=20, rotation=0)
                ax2.yaxis.set_label_coords(1.01, 1.03)

                plt.title(title, fontsize=20)

                plt.tight_layout()

                file_name = f"MES_{machine['name']}_Chart.png"
                # save_path = os.path.join("yearly_output")
                chart_img = os.path.join(save_path, file_name)

                plt.savefig(f"{chart_img}", dpi=100, bbox_inches="tight", pad_inches=0.45)
                plt.close('all')
            except Exception as e:
                print(f"{machine['name']}: {e}")
                pass
    def monthly_chart(self, plant):
        save_path = self.save_path

        data1 = self.mach_list

        for machine in data1:
            print(f"Machine {machine}")
            try:
                today = datetime.today()
                month_date = []
                this_data = []
                this_rate = []
                x_labels = []
                unfinish_data = []

                for i in range(12):
                    end_date = (today - relativedelta(months=i)).replace(day=1) - timedelta(days=1)
                    start_date = end_date.replace(day=1)
                    month_date.append([start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'), start_date.strftime("%B %Y")])
                    x_labels.append(start_date.strftime("%Y-%m"))
                month_date.reverse()
                x_labels.reverse()
                for i, item in enumerate(month_date):
                    print(f"Month {item[2]} - start {item[0]} - end - {item[1]}")
                    sql = f"""
                            SELECT name,qty, target, (case when target-qty > 0 then target-qty then 0 end) unfinish_qty FROM (
                            SELECT name, sum(case when sum_qty > 0 then sum_qty else 0 end) as qty, sum(case when target > 0 then target else 0 end) as target
                            FROM [MES_OLAP].[dbo].[mes_daily_report_raw] where Name = '{machine['name']}'
                            and (belong_to between '{item[0]}' and '{item[1]}')
                            group by name) A"""
                    rows = vnedc_database().select_sql_dict(sql)
                    if len(rows) == 0:
                        this_data.append(0)
                        this_rate.append(0)
                        unfinish_data.append(0)
                    else:
                        this_data.append(rows[0]['qty'])
                        unfinish_data.append(rows[0]['unfinish_qty'])
                        try:
                            rate = int((rows[0]['qty'] / rows[0]['target']) * 100) if rows[0]['target'] > 0 else 100
                        except Exception as e:
                            rate = 100
                            print(f"{e} at {machine['name']} at {item[2]} start {item[0]} - end {item[1]}")
                            pass
                        this_rate.append(rate)

                x_range = range(0, len(x_labels) * 2, 2)

                max_data = max(this_data, default=0)
                step_data = 5
                rounded_max_data = int(
                    (((max_data / (10 ** (len(str(max_data)) - 2))) // step_data) * step_data + step_data) * (
                            10 ** (len(str(max_data)) - 2)))
                rounded_step_data = step_data * (10 ** (len(str(max_data)) - 2))
                max_rate = max(this_rate, default=0)
                rounded_max_rate = (math.ceil(max_rate / 10) * 10)
                rounded_step_rate = 20

                bar_width = 0.6
                plt.figure(figsize=(16, 9))
                fig, ax1 = plt.subplots(figsize=(16, 9))
                this_month_bars = ax1.bar([i for i in x_range], this_data, width=bar_width,
                                          label=f"月產量", align='center', color='#10ba81')
                unfinish_bars = ax1.bar([i for i in x_range], unfinish_data, width=bar_width, bottom=this_data,
                                        label=f"月目標差異",
                                        align='center', color='lightgreen')
                ax1.set_xticks(x_range)
                ax1.set_xticklabels(x_labels, rotation=0, ha="center", fontsize=12)
                if len(str(max_data)) > 7:
                    yticks_positions = list(range(0, rounded_max_data + 1 * rounded_step_data, rounded_step_data))
                    yticks_positions.append(int(rounded_max_data + 2 * rounded_step_data))
                    yticks_labels = [
                        f"{int(i//(10**(len(str(max_data)) - 2)))}" + '百萬' if len(str(i)) > 6 else f"{i}" for i
                        in yticks_positions]
                    for index, bar in enumerate(this_month_bars):
                        height = bar.get_height()
                        unfinish_height = unfinish_bars[index].get_height()
                        ax1.text(
                            bar.get_x() + bar.get_width() / 2,
                            height + unfinish_height,
                            f'{round(height/(10**(len(str(max_data)) - 2)),2)}'[
                            :4] if height > 0 else '',
                            ha='center', va='bottom', fontsize=12  # Align the text
                        )
                elif 4 < len(str(max_data)) <= 7:
                    yticks_positions = list(range(0, rounded_max_data, rounded_step_data))
                    yticks_positions.append(int(rounded_max_data + 3 * rounded_step_data))
                    # yticks_labels = [f"{int(i//(10**(len(str(max_data)) - 3)))}" + '萬 PCS' if len(str(i)) > 4 and int(
                    #     i // (10 ** (len(str(max_data)) - 3))) % 60 == 0 else "" for i in yticks_positions]
                    yticks_labels = [f"{int(i/1000000)} 百萬" if i > 0 else 0 for i in yticks_positions]
                    for index, bar in enumerate(this_month_bars):
                        height = bar.get_height()
                        unfinish_height = unfinish_bars[index].get_height()
                        ax1.text(
                            bar.get_x() + bar.get_width() / 2,
                            height + unfinish_height,
                            f'{int(height/(10**(len(str(max_data)) - 3)))}'[:4] if height > 0 else '',
                            ha='center', va='bottom', fontsize=12  # Align the text
                        )

                yticks_labels[-1] = ""
                ax1.set_yticks(yticks_positions)
                ax1.set_yticklabels(yticks_labels, fontsize=12)

                ax2 = ax1.twinx()
                sr_achieve_rate = "達成率"

                filtered_data = [(x, rate) for x, rate in zip(x_range, this_rate) if rate != 0]
                x_filtered, this_rate_filtered = zip(*filtered_data)
                this_rate_line = ax2.plot(x_filtered, this_rate_filtered,
                                          label=sr_achieve_rate,
                                          color='#ED7D31', marker='o', linewidth=1.5)
                sr_target = "達成率目標"

                ry_label = "達成率(%)"
                ly_label = "Product (百萬)"

                name = f"各週產出量"
                title = f"\n{plant} ({machine['name']})\n"

                if plant == "NBR":
                    target_rate = 95
                else:
                    target_rate = 98

                yticks_positions = list(range(0, rounded_max_rate + 1 * rounded_step_rate, rounded_step_rate))
                if target_rate not in yticks_positions:
                    yticks_positions.append(target_rate)
                yticks_positions.append(rounded_max_rate + 0.25 * rounded_step_rate)
                yticks_positions = sorted(yticks_positions)

                yticks_labels = [f"{i}" + '%' for i in yticks_positions]
                yticks_labels[-1] = ""
                ax2.set_yticks(yticks_positions)
                ax2.set_yticklabels(yticks_labels)
                ax2.axhline(y=target_rate, color='red', linestyle='--', linewidth=1, label=sr_target)

                ax1.xaxis.set_label_coords(0.975, -0.014)
                ax1.set_ylabel('Product output', fontsize=12)
                ax2.set_ylabel('Archive rates', fontsize=12)

                for i, value in enumerate(this_rate):
                    ax2.text(
                        x_range[i],
                        value + 1.5,
                        f"{value:.1f}%" if value != 0 else '',
                        ha='center', va='bottom', fontsize=10, color='white',
                        bbox=dict(facecolor='#ED7D31', edgecolor='none', boxstyle='round,pad=0.3')
                    )

                handles1, labels1 = ax1.get_legend_handles_labels()
                handles2, labels2 = ax2.get_legend_handles_labels()
                fig.legend(
                    handles1 + handles2,
                    labels1 + labels2,
                    loc='center left',
                    fontsize=10,
                    title="Note",
                    title_fontsize=12,
                    bbox_to_anchor=(1.0, 0.5),
                    ncol=1
                )

                # Customize axes and borders
                ax = plt.gca()  # Get current axes
                ax.spines['top'].set_color('white')
                ax.spines['top'].set_linewidth(1.5)

                ax1.tick_params(axis='y', colors='black')  # Y-axis ticks color
                y_tick_lines = ax1.get_yticklines()
                y_tick_lines[-2].set_visible(False)
                y_tick_lines[-1].set_visible(False)
                ax1.annotate(
                    '',
                    xy=(0, 1.0),
                    xytext=(0, 0.98),
                    xycoords='axes fraction',
                    arrowprops=dict(facecolor='black', arrowstyle='-|>,widthA=0.4,widthB=1.4', linewidth=0.5)
                )
                ax1.set_ylabel(ly_label, labelpad=20, rotation=0)
                ax1.yaxis.set_label_coords(-0.01, 1.01)

                ax2.tick_params(axis='y', colors='black')
                y_tick_lines = ax2.get_yticklines()
                y_tick_lines[-2].set_visible(False)
                y_tick_lines[-1].set_visible(False)
                ax2.annotate(
                    '',
                    xy=(1, 1.0),
                    xytext=(1, 0.98),
                    xycoords='axes fraction',
                    arrowprops=dict(facecolor='black', arrowstyle='-|>,widthA=0.4,widthB=1.4', linewidth=0.5)
                )

                ax2.set_ylabel(ry_label, labelpad=20, rotation=0)
                ax2.yaxis.set_label_coords(1.01, 1.03)

                plt.title(title, fontsize=20)

                plt.tight_layout()

                file_name = f"MES_{machine['name']}_Chart.png"
                # save_path = os.path.join("yearly_output")
                chart_img = os.path.join(save_path, file_name)

                plt.savefig(f"{chart_img}", dpi=100, bbox_inches="tight", pad_inches=0.45)
                plt.close('all')
            except:
                pass

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

    def main(self):
        for plant in self.plant_name:
            self.mach_list = self.get_mach_list(plant)
            # self.generate_chart(plant)
            # if self.mode == 'WEEKLY':
            #     self.weekly_chart(plant)
            # if self.mode == 'MONTHLY':
            #     self.monthly_chart(plant)
            # self.rate_chart(plant)
            self.generate_raw_excel(plant)

        # self.send_email(self.file_list, self.image_buffers)

    def delete_counting_weekly_info_raw(self, plant, year, month_week):
        mes_olap = mes_olap_database()
        sql = f"""
            delete from [MES_OLAP].[dbo].[counting_weekly_info_raw]
            where Plant = '{plant}' and [Year] = {year} and MonthWeek = '{month_week}'
        """
        mes_olap.execute_sql(sql)

    def insert_counting_weekly_info_raw(self, plant, year, month_week, summary_df):
        mes_olap = mes_olap_database()
        # Only show data which activation not null
        df = summary_df.loc[summary_df["稼動率"].notna() & (summary_df["稼動率"] != "")]

        counting_sum = df["點數機數量"].sum()
        separate_sum = df["隔離品數量"].sum()
        target_sum = df["目標產能"].sum()
        onlinePacking_sum = df["包裝確認量"].sum()
        faulty_sum = df["二級品數量"].sum()
        scrap_sum = df["廢品數量"].sum()

        speed_avg = round(df["標準車速"].mean(), 0)
        activation_avg = round(df["稼動率"].mean(), 3)
        capacity_avg = round(df["產能效率"].mean(), 3)
        yield_avg = round(df["良率"].mean(), 3)
        oee_avg = round(activation_avg*capacity_avg*yield_avg, 3)

        separate_rate = round(separate_sum / (counting_sum + faulty_sum + scrap_sum), 3)
        scrap_rate = round(scrap_sum / (counting_sum + faulty_sum + scrap_sum), 3)

        sql = f"""
        Insert into [MES_OLAP].[dbo].[counting_weekly_info_raw](Plant, [Year], MonthWeek, CountingQty, SeparateQty, AvgSpeed, 
        Activation, Capacity, Yield, OEE, SeparateRate, ScrapRate, Target, OnlinePacking)
        Values('{plant}', {year}, '{month_week}', {counting_sum}, {separate_sum}, {speed_avg}, 
        {activation_avg}, {capacity_avg},{yield_avg},{oee_avg},{separate_rate},{scrap_rate},{target_sum},{onlinePacking_sum})
        """
        print(sql)
        mes_olap.execute_sql(sql)

import argparse
from datetime import datetime, timedelta, date

parser = argparse.ArgumentParser(description="解析外部参数")
parser.add_argument("--mode", choices=['WEEKLY', 'MONTHLY'], help="MONTHLY OR WEEKLY")
args = parser.parse_args()
mode = args.mode

if not mode:
    mode = "WEEKLY"

report = mes_weekly_report(mode)
report.main()