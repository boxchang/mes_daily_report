import sys
import os
from PIL import Image as PILImage
from matplotlib.ticker import MultipleLocator

from database import vnedc_database, mes_database
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
        'Date': '日期範圍'
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
            self.this_start_date = this_start_date
            this_end_date = (this_start_date.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
            self.this_end_date = this_end_date

            last_end_date = this_start_date - timedelta(days=1)
            self.last_end_date = last_end_date
            last_start_date = last_end_date.replace(day=1)
            self.last_start_date = last_start_date

            # Manual Run
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
        colmn_letter = {'Date': 'A', 'Name': 'B', 'Line': 'C', 'Shift': 'D', 'WorkOrderId': 'E', 'PartNo': 'F',
                        'ProductItem': 'G',
                        'AQL': 'H', 'ProductionTime': 'I', 'Period': 'J', 'max_speed': 'K', 'min_speed': 'L',
                        'avg_speed': 'M', 'LineSpeedStd': 'N', 'sum_qty': 'O', 'Ticket_Qty': 'P', 'Separate': 'Q',
                        'Target': 'R',
                        'Scrap': 'S', 'SecondGrade': 'T', 'OverControl': 'U', 'WeightValue': 'V', 'WeightLower': 'W',
                        'WeightUpper': 'X', 'Activation': 'Y', 'OpticalNGRate': 'Z'}

        # Preprocess data for Excel
        df['ProductionTime'] = (df['ProductionTime'] // 60).astype(str) + 'H'
        df['Period'] = df['Period'].apply(lambda x: f"{int(x):02}:00")

        # Rename columns
        df.rename(columns=self.header_columns, inplace=True)

        namesheet = str(machine_name).split('_')[-1]
        save_path = self.save_path
        file_name = f"MES_{machine_name}_Chart.png"
        chart_img = os.path.join(save_path, file_name)
        if os.path.exists(chart_img):
            header_row = 31
            data_start_row = 32
        else:
            header_row = 0
            data_start_row = 1

        # Write data to the Excel sheet
        df.to_excel(writer, sheet_name=namesheet, index=False, startrow=header_row)

        workbook = writer.book
        worksheet = writer.sheets.get(namesheet)

        if not worksheet:
            worksheet = workbook.add_worksheet(namesheet)

        try:
            img = Image(chart_img)
            img.height = 6 * 96
            img.width = 11.69 * 96
            img.anchor = 'A1'
            worksheet.add_image(img)
        except:
            print('No counting machine data yet!')
            pass
        # Freeze the first row of data
        # worksheet.freeze_panes = worksheet[f'A{data_start_row+1}']

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
                if col_letter in [colmn_letter['max_speed'], colmn_letter['min_speed'], colmn_letter['avg_speed'],
                                  colmn_letter['LineSpeedStd']]:
                    cell.alignment = self.right_align_style.alignment
                elif col_letter in [colmn_letter['sum_qty'], colmn_letter['Target']]:
                    cell.number_format = '#,##0'
                    cell.alignment = self.right_align_style.alignment
                elif col_letter in [colmn_letter['WeightValue']]:
                    try:
                        cell.value = float(cell.value)
                    except:
                        pass
                elif col_letter in [colmn_letter['OpticalNGRate']]:
                    worksheet.column_dimensions[col_letter].width = 10
                    cell.alignment = self.center_align_style.alignment
                    cell.number_format = '0.0%'
                elif col_letter in [colmn_letter['WeightLower'], colmn_letter['WeightUpper']]:
                    worksheet.column_dimensions[col_letter].hidden = True
                elif col_letter in [colmn_letter['Ticket_Qty']]:
                    cell.number_format = '#,##0'
                    cell.alignment = self.right_align_style.alignment
                    worksheet.column_dimensions[col_letter].hidden = True
                else:
                    cell.alignment = self.center_align_style.alignment

        # Add comments to WeightValue cells based on WeightLower and WeightUpper
        for row in range(data_start_row, worksheet.max_row + 1):
            weight_value_cell = worksheet[colmn_letter['WeightValue'] + str(row)]
            weight_lower_cell = worksheet[colmn_letter['WeightLower'] + str(row)].value
            weight_upper_cell = worksheet[colmn_letter['WeightUpper'] + str(row)].value

            if weight_lower_cell or weight_upper_cell:
                comment = Comment(text=f"IPQC範圍({weight_lower_cell}-{weight_upper_cell})", author="System")
                weight_value_cell.comment = comment

        return workbook

    def generate_raw_excel(self, plant):
        this_start_date = self.this_start_date
        this_end_date = self.this_end_date
        save_path = self.save_path
        date_mark = self.date_mark
        mode = self.mode

        file_name = f'MES_{plant}_{mode}_Report_{date_mark}.xlsx'
        excel_file = os.path.join(save_path, file_name)
        with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
            sql = f"""SELECT belong_to
                      ,[Name]
                      ,[Line]
                      ,[Shift]
                      ,[WorkOrderId]
                      ,[PartNo]
                      ,[ProductItem]
                      ,[AQL]
                      ,[ProductionTime]
                      ,[Period]
                      ,[max_speed]
                      ,[min_speed]
                      ,[avg_speed]
                      ,[LineSpeedStd]
                      ,[sum_qty]
                      ,[ticket_qty]
                      ,CASE 
                           WHEN [Separate] = 'null' THEN '' 
                           ELSE [Separate] 
                       END AS [Separate]
                      ,[Target]
                      ,[Scrap]
                      ,[SecondGrade]
                      ,[OverControl]
                      ,[WeightValue]
                      ,[WeightLower]
                      ,[WeightUpper]
                      ,[Activation]
                      ,[OpticalNGRate]
                        FROM [MES_OLAP].[dbo].[mes_daily_report_raw]
                        where name like '%{plant}%' and belong_to between '{this_start_date}' and '{this_end_date}'
                        order by name, belong_to, line, shift, period
                        """
            data = self.db.select_sql_dict(sql)
            df = pd.DataFrame(data)

            machine_groups = df.groupby('Name')
            self.generate_summary(writer, machine_groups)
            for machine_name, machine_df in machine_groups:
                self.generate_excel(writer, machine_df, machine_name)
        self.file_list.append(excel_file)

    def generate_summary(self, writer, machine_groups):
        colmn_letter = {'Name': 'A', 'Week': 'B', 'shift': 'C', 'Line': 'D', 'Output': 'E', 'Target': 'F', 'Achievement Rate': 'G'}
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
                    total_output = filtered_df['sum_qty'].sum()
                    total_target = filtered_df['Target'].sum()
                    rate = round((int(total_output) / int(total_target)), 3) if int(total_target) > 0 else 0

                    summary_row = {
                        'Name': machine_name,
                        'Date': tmp_week,
                        'Shift': shift,
                        'Line': line,
                        'sum_qty': total_output,
                        'Target': total_target,
                        'Achievement Rate': rate
                    }
                    summary_data.append(summary_row)
            avg_rate = [item['Achievement Rate'] for item in summary_data if item['Name'] == machine_name]
            sum_qty = sum(item['sum_qty'] for item in summary_data if item['Name'] == machine_name)
            sum_target = sum(item['Target'] for item in summary_data if item['Name'] == machine_name)

            summary_data.append({'Name': machine_name, 'Date': tmp_week, 'Shift': '', 'Line': '', 'sum_qty': sum_qty, 'Target': sum_target, 'Achievement Rate': round(sum_qty/sum_target, 3)})
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
                if col_letter in [colmn_letter['Output'], colmn_letter['Target']]:  # Apply right alignment for specific columns
                    cell.number_format = '#,##0'
                    cell.alignment = self.right_align_style.alignment
                elif col_letter in [colmn_letter['Achievement Rate']]:
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
                    sum(case when cast(belong_to as date) between '{last_start_date}' and '{last_end_date}' then target else 0 end) as target_last_time
                    FROM [MES_OLAP].[dbo].[mes_daily_report_raw]
                    where name like '%{plant}%'
                    group by name
                    order by name"""
        data = self.db.select_sql_dict(sql)

        # Output Bar Chart
        x_labels = [str(item['name']).split('_')[-1] for item in data]
        x_range = range(0, len(x_labels) * 2, 2)

        this_data = [int(item['this_time']) for item in data]
        # last_data = [int(item['last_time']) for item in data]
        max_data = max(this_data, default=0)
        step_data = 5
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
            # last_month_bars = ax1.bar([i - bar_width / 2 for i in x_range], last_data, width=bar_width,
            #                           label=f"{last_start_date.strftime('%d/%m')}-{last_end_date.strftime('%d/%m')}",
            #                           align='center', color='#eeeeee')
            this_month_bars = ax1.bar([i for i in x_range], this_data, width=bar_width,
                                      label=f"週產量",
                                      align='center', color='#10ba81')
        if mode == "MONTHLY":
            # last_month_bars = ax1.bar([i - bar_width / 2 for i in x_range], last_data, width=bar_width,
            #                           label=f"{last_start_date.strftime('%B %Y')}", align='center', color='#eeeeee')
            this_month_bars = ax1.bar([i for i in x_range], this_data, width=bar_width,
                                      label=f"{this_start_date.strftime('%Y %m')}月產量", align='center', color='#10ba81')

        ax1.set_xticks(x_range)
        ax1.set_xticklabels(x_labels, rotation=0, ha="center", fontsize=12)

        if len(str(max_data)) > 7:
            yticks_positions = list(range(0, rounded_max_data + 1 * rounded_step_data, rounded_step_data))
            yticks_positions.append(int(rounded_max_data + 2 * rounded_step_data))
            yticks_labels = [f"{int(i//(10**(len(str(max_data)) - 2)))}" + 'M PCS' if len(str(i)) > 6 else f"{i}" for i
                             in yticks_positions]
            # for bar in last_month_bars:
            #     height = bar.get_height()
            #     ax1.text(
            #         bar.get_x() + bar.get_width() / 2,
            #         height,
            #         f'{round(height/(10**(len(str(max_data)) - 2)),2)}'.replace('.', ',')[:4] if height > 0 else '',
            #         ha='center', va='bottom', fontsize=8  # Align the text
            #     )
            for bar in this_month_bars:
                height = bar.get_height()
                ax1.text(
                    bar.get_x() + bar.get_width() / 2,
                    height,
                    f'{round(height/(10**(len(str(max_data)) - 2)),2)}'.replace('.', ',')[:4] if height > 0 else '',
                    ha='center', va='bottom', fontsize=8  # Align the text
                )
        elif 4 < len(str(max_data)) <= 7:
            yticks_positions = list(range(0, rounded_max_data + 1 * rounded_step_data, rounded_step_data))
            yticks_positions.append(int(rounded_max_data + 4 * rounded_step_data))
            yticks_labels = [f"{int(i//(10**(len(str(max_data)) - 2)))}" + 'M PCS' if len(str(i)) > 4 and int(
                i // (10 ** (len(str(max_data)) - 3))) % 60 == 0 else "" for i in yticks_positions]
            # for bar in last_month_bars:
            #     height = bar.get_height()
            #     ax1.text(
            #         bar.get_x() + bar.get_width() / 2,
            #         height,
            #         f'{int(height/(10**(len(str(max_data)) - 3)))}'.replace('.', ',')[:4] if height > 0 else '',
            #         ha='center', va='bottom', fontsize=8  # Align the text
            #     )
            for bar in this_month_bars:
                height = bar.get_height()
                ax1.text(
                    bar.get_x() + bar.get_width() / 2,
                    height,
                    f'{int(height/(10**(len(str(max_data)) - 3)))}'.replace('.', ',')[:4] if height > 0 else '',
                    ha='center', va='bottom', fontsize=8  # Align the text
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
        ly_label = "Product (M PCS)"
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
        ax2.set_yticklabels(yticks_labels)
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

    def rate_chart(self, plant):
        this_start_date = self.this_start_date
        this_end_date = self.this_end_date
        date_mark = self.date_mark
        save_path = self.save_path

        if plant == 'NBR':
            plant_ = 'NBR'
        else:
            plant_ = 'PVC1'

        sql_ = f"""
                select name FROM [PMGMES].[dbo].[PMG_DML_DataModelList] 
                where DataModelTypeId = 'DMT000003' and name like '%{plant_}%' order by name
            """
        data1 = mes_database().select_sql_dict(sql_)

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

        plt.figure(figsize=(16, 9))
        fig, ax1 = plt.subplots(figsize=(16, 9))

        # Plotting lines
        ax1.plot(x_range, scrap, label="報廢率 (%)", marker='o', linestyle='-', color='#ED7D31', linewidth=2)
        ax1.plot(x_range, secondgrade, label="二級品率 (%)", marker='o', linestyle='-', color='#70AD47', linewidth=2)

        # Adding standard line
        ax1.axhline(y=0.8, color='#ED7D31', linestyle='--', linewidth=1.5, label="廢 品標準線(0.8)")
        ax1.axhline(y=0.2, color='#70AD47', linestyle='--', linewidth=1.5, label="二級品標準線(0.2)")

        # Adding data labels
        for i, (scrap_val, secondgrade_val) in enumerate(zip(scrap, secondgrade)):
            if scrap_val > 0:
                ax1.text(i, scrap_val, f"{scrap_val:.2f}", ha='center', va='bottom', fontsize=8, color='#ED7D31')
            if secondgrade_val > 0:
                ax1.text(i, secondgrade_val, f"{secondgrade_val:.2f}", ha='center', va='bottom', fontsize=8,
                         color='#70AD47')

        ax1.set_xticks(x_range)
        ax1.set_xticklabels(x_labels, rotation=0, ha="center", fontsize=12)
        ax1.set_ylabel('二級品及報廢率 (%)', fontsize=12)
        ax1.set_title(f"二級品及報廢率 ({plant})", fontsize=20)

        handles, labels = ax1.get_legend_handles_labels()
        fig.legend(
            handles,
            labels,
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

    def weekly_chart(self, plant):
        save_path = self.save_path
        if plant == 'NBR':
            plant_ = 'NBR'
        else:
            plant_ = 'PVC1'
        sql_ = f"""
                select name FROM [PMGMES].[dbo].[PMG_DML_DataModelList] 
                where DataModelTypeId = 'DMT000003' and name like '%{plant_}%' order by name
            """
        data1 = mes_database().select_sql_dict(sql_)
        for machine in data1:
            try:
                this_data = []
                this_rate = []
                x_labels = []
                week_date = []

                today = datetime.now()

                # Get the starting week (Sep 30)
                start_date = datetime(2024, 10, 1)
                start_week_number = 41
                weeks_to_generate = 52

                current_date = start_date
                current_week_number = start_week_number

                while current_date <= today:
                    week_start = current_date
                    week_end = current_date + timedelta(days=6)

                    if today < week_end:
                        break

                    week_number = (current_week_number - 1) % 52 + 1
                    x_labels.append(f'W{week_number}')

                    week_date.append([week_start, week_end])

                    current_date += timedelta(days=7)
                    current_week_number += 1

                print(machine['name'])
                for i, item in enumerate(week_date):
                    print(f"Week {i + 1 } - start {item[0]} - end - {item[1]}")
                    sql = f"""SELECT name, sum(case when sum_qty > 0 then sum_qty else 0 end) as qty, sum(case when target > 0 then target else 0 end) as target
                            FROM [MES_OLAP].[dbo].[mes_daily_report_raw] where Name = '{machine['name']}'
                            and (belong_to between '{item[0]}' and '{item[1]}')
                            group by name"""
                    rows = vnedc_database().select_sql_dict(sql)
                    if len(rows) == 0:
                        this_data.append(0)
                        this_rate.append(0)
                    else:
                        this_data.append(rows[0]['qty'])
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
                                          label=f"週產量", align='center', color='#10ba81')
                ax1.set_xticks(x_range)
                ax1.set_xticklabels(x_labels, rotation=0, ha="center", fontsize=12)
                if len(str(max_data)) > 7:
                    yticks_positions = list(range(0, rounded_max_data + 1 * rounded_step_data, rounded_step_data))
                    yticks_positions.append(int(rounded_max_data + 2 * rounded_step_data))
                    yticks_labels = [
                        f"{int(i//(10**(len(str(max_data)) - 2)))}" + 'M PCS' if len(str(i)) > 6 else f"{i}" for i
                        in yticks_positions]
                    for bar in this_month_bars:
                        height = bar.get_height()
                        ax1.text(
                            bar.get_x() + bar.get_width() / 2,
                            height,
                            f'{round(height/(10**(len(str(max_data)) - 2)),2)}'.replace('.', ',')[
                            :4] if height > 0 else '',
                            ha='center', va='bottom', fontsize=8  # Align the text
                        )
                elif 4 < len(str(max_data)) <= 7:
                    yticks_positions = list(range(0, rounded_max_data, rounded_step_data))
                    yticks_positions.append(int(rounded_max_data + 3 * rounded_step_data))
                    # yticks_labels = [f"{int(i//(10**(len(str(max_data)) - 3)))}" + '萬 PCS' if len(str(i)) > 4 and int(
                    #     i // (10 ** (len(str(max_data)) - 3))) % 60 == 0 else "" for i in yticks_positions]
                    yticks_labels = [f"{int(i/1000000)} M PCS" if i > 0 else 0 for i in yticks_positions]
                    for bar in this_month_bars:
                        height = bar.get_height()
                        ax1.text(
                            bar.get_x() + bar.get_width() / 2,
                            height,
                            f'{int(height/(10**(len(str(max_data)) - 3)))}'.replace('.', ',')[:4] if height > 0 else '',
                            ha='center', va='bottom', fontsize=10  # Align the text
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
                ly_label = "Product (M PCS)"

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
            except Exception as e:
                print(f"{machine['name']}: {e}")
                pass
    def monthly_chart(self, plant):
        save_path = self.save_path
        if plant == 'NBR':
            plant_ = 'NBR'
        else:
            plant_ = 'PVC1'
        sql_ = f"""
                select name FROM [PMGMES].[dbo].[PMG_DML_DataModelList] 
                where DataModelTypeId = 'DMT000003' and name like '%{plant_}%' order by name
            """
        data1 = mes_database().select_sql_dict(sql_)
        for machine in data1:
            print(f"Machine {machine}")
            try:
                today = datetime.today()
                month_date = []
                this_data = []
                this_rate = []
                x_labels = []

                for i in range(12):
                    end_date = (today - relativedelta(months=i)).replace(day=1) - timedelta(days=1)
                    start_date = end_date.replace(day=1)
                    month_date.append([start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'), start_date.strftime("%B %Y")])
                    x_labels.append(start_date.strftime("%Y-%m"))
                month_date.reverse()
                x_labels.reverse()
                for i, item in enumerate(month_date):
                    print(f"Month {item[2]} - start {item[0]} - end - {item[1]}")
                    sql = f"""SELECT name, sum(case when sum_qty > 0 then sum_qty else 0 end) as qty, sum(case when target > 0 then target else 0 end) as target
                            FROM [MES_OLAP].[dbo].[mes_daily_report_raw] where Name = '{machine['name']}'
                            and (belong_to between '{item[0]}' and '{item[1]}')
                            group by name"""
                    rows = vnedc_database().select_sql_dict(sql)
                    if len(rows) == 0:
                        this_data.append(0)
                        this_rate.append(0)
                    else:
                        this_data.append(rows[0]['qty'])
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
                                          label=f"週產量", align='center', color='#10ba81')
                ax1.set_xticks(x_range)
                ax1.set_xticklabels(x_labels, rotation=0, ha="center", fontsize=12)
                if len(str(max_data)) > 7:
                    yticks_positions = list(range(0, rounded_max_data + 1 * rounded_step_data, rounded_step_data))
                    yticks_positions.append(int(rounded_max_data + 2 * rounded_step_data))
                    yticks_labels = [
                        f"{int(i//(10**(len(str(max_data)) - 2)))}" + 'M PCS' if len(str(i)) > 6 else f"{i}" for i
                        in yticks_positions]
                    for bar in this_month_bars:
                        height = bar.get_height()
                        ax1.text(
                            bar.get_x() + bar.get_width() / 2,
                            height,
                            f'{round(height/(10**(len(str(max_data)) - 2)),2)}'.replace('.', ',')[
                            :4] if height > 0 else '',
                            ha='center', va='bottom', fontsize=8  # Align the text
                        )
                elif 4 < len(str(max_data)) <= 7:
                    yticks_positions = list(range(0, rounded_max_data, rounded_step_data))
                    yticks_positions.append(int(rounded_max_data + 3 * rounded_step_data))
                    # yticks_labels = [f"{int(i//(10**(len(str(max_data)) - 3)))}" + '萬 PCS' if len(str(i)) > 4 and int(
                    #     i // (10 ** (len(str(max_data)) - 3))) % 60 == 0 else "" for i in yticks_positions]
                    yticks_labels = [f"{int(i/1000000)} M PCS" if i > 0 else 0 for i in yticks_positions]
                    for bar in this_month_bars:
                        height = bar.get_height()
                        ax1.text(
                            bar.get_x() + bar.get_width() / 2,
                            height,
                            f'{int(height/(10**(len(str(max_data)) - 3)))}'.replace('.', ',')[:4] if height > 0 else '',
                            ha='center', va='bottom', fontsize=10  # Align the text
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
                ly_label = "Product (M PCS)"

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

    def main(self):
        for plant in self.plant_name:
            self.generate_chart(plant)
            if self.mode == 'WEEKLY':
                self.weekly_chart(plant)
            if self.mode == 'MONTHLY':
                self.monthly_chart(plant)
            self.rate_chart(plant)
            self.generate_raw_excel(plant)
        self.send_email(self.file_list, self.image_buffers)


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