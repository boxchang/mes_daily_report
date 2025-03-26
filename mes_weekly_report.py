import sys
import os
from database import vnedc_database
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
        'OpticalNGRate': '光檢不良率'
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
        if mode == "MONTHLY":
            save_path = os.path.join("monthly_output")
            this_start_date = today.replace(day=1)
            self.this_start_date = this_start_date
            this_end_date = (this_start_date.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
            self.this_end_date = this_end_date

            last_end_date = this_start_date - timedelta(days=1)
            self.last_end_date = last_end_date
            last_start_date = last_end_date.replace(day=1)
            self.last_start_date = last_start_date
        elif mode == "WEEKLY":
            save_path = os.path.join("weekly_output")
            days_to_sunday = today.weekday()
            this_end_date = today - timedelta(days=days_to_sunday + 1)
            self.this_end_date = this_end_date
            this_start_date = this_end_date - timedelta(days=6)
            self.this_start_date = this_start_date

            last_end_date = this_start_date - timedelta(days=1)
            self.last_end_date = last_end_date
            last_start_date = last_end_date - timedelta(days=6)
            self.last_start_date = last_start_date
        self.save_path = save_path
        # Check folder to create
        if not os.path.exists(save_path):
            os.makedirs(save_path)

        self.date_mark = "{start_date}_{end_date}".format(start_date=this_start_date.strftime("%m%d"), end_date=this_end_date.strftime("%m%d"))

    def generate_excel(self, writer, df, machine_name):
        colmn_letter = {'Date':'A', 'Name': 'B', 'Line': 'C', 'Shift': 'D', 'WorkOrderId': 'E', 'PartNo': 'F', 'ProductItem': 'G',
                       'AQL': 'H', 'ProductionTime': 'I', 'Period': 'J', 'max_speed': 'K', 'min_speed': 'L',
                        'avg_speed': 'M', 'LineSpeedStd': 'N', 'sum_qty': 'O', 'Ticket_Qty': 'P', 'Separate': 'Q', 'Target': 'R',
                        'Scrap': 'S', 'SecondGrade': 'T', 'OverControl': 'U', 'WeightValue': 'V', 'WeightLower': 'W',
                        'WeightUpper': 'X', 'Activation': 'Y', 'OpticalNGRate': 'Z'
                        }

        # 轉出Excel前進行資料處理
        df['ProductionTime'] = (df['ProductionTime'] // 60).astype(str) + 'H'
        df['Period'] = df['Period'].apply(lambda x: f"{int(x):02}:00")

        # Change column names
        df.rename(columns=self.header_columns, inplace=True)

        namesheet = str(machine_name).split('_')[-1]
        # Write data to the Excel sheet with the machine name as the sheet name
        df.to_excel(writer, sheet_name=namesheet, index=False)

        # Read the written Excel file
        workbook = writer.book
        worksheet = writer.sheets[namesheet]

        # Freeze the first row
        worksheet.freeze_panes = worksheet['A2']

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
                if col_letter in [colmn_letter['max_speed'], colmn_letter['min_speed'], colmn_letter['avg_speed'], colmn_letter['LineSpeedStd']]:  # Apply right alignment for specific columns
                    cell.alignment = self.right_align_style.alignment
                elif col_letter in [colmn_letter['sum_qty'], colmn_letter['Target']]:
                    cell.number_format = '#,##0'
                    cell.alignment = self.right_align_style.alignment
                elif col_letter in [colmn_letter['WeightValue']]:
                    try:
                        cell.value = float(cell.value)
                    except ValueError:
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

        for row in range(2, worksheet.max_row + 1):  # 從第2行開始，因為第1行是標題
            weight_value_cell = worksheet[colmn_letter['WeightValue']+str(row)]
            weight_lower_cell = worksheet[colmn_letter['WeightLower']+str(row)].value
            weight_upper_cell = worksheet[colmn_letter['WeightUpper']+str(row)].value

            if weight_lower_cell or weight_upper_cell:
                comment = Comment(text="IPQC範圍("+weight_lower_cell+"-"+weight_upper_cell+")", author="System")  # 創建註解
                weight_value_cell.comment = comment

        return workbook

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
                        FROM [MES_OLAP].[dbo].[counting_daily_info_raw] c
                        LEFT JOIN [MES_OLAP].[dbo].[mes_ipqc_data] ipqc on c.Runcard = ipqc.Runcard
                        where c.MonthWeek = '{month_week}'  and c.branch like'%{plant}%' and OnlinePacking > 0
                        and not (WorkOrder != '' and InspectedAQL ='')
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

    def generate_chart(self, plant):
        this_start_date = self.this_start_date
        this_end_date = self.this_end_date
        last_start_date = self.last_start_date
        last_end_date = self.last_end_date
        mode = self.mode
        save_path = self.save_path
        date_mark = self.date_mark

        plt.rcParams['font.sans-serif'] = ['Microsoft YaHei']

        sql = f"""SELECT name,
                    sum(case when cast(date as date) between '{this_start_date}' and '{this_end_date}' then sum_qty else 0 end) as this_time,
                    sum(case when cast(date as date) between '{this_start_date}' and '{this_end_date}' then target else 0 end) as target_this_time,
                    sum(case when cast(date as date) between '{last_start_date}' and '{last_end_date}' then sum_qty else 0 end) as last_time,
                    sum(case when cast(date as date) between '{last_start_date}' and '{last_end_date}' then target else 0 end) as target_last_time
                    FROM [MES_OLAP].[dbo].[mes_daily_report_raw]
                    where name like '%{plant}%'
                    group by name
                    order by name"""
        data = self.db.select_sql_dict(sql)

        # Output Bar Chart
        x_labels = [str(item['name']).split('_')[-1] for item in data]
        x_labels = [str(item['name']).split('_')[-1] for item in data]
        x_range = range(0, len(x_labels) * 2, 2)

        this_data = [int(item['this_time']) for item in data]
        last_data = [int(item['last_time']) for item in data]
        max_data = max(max(this_data, default=0), max(last_data, default=0))
        step_data = 3
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
            last_month_bars = ax1.bar([i - bar_width / 2 for i in x_range], last_data, width=bar_width,
                                      label=f"{last_start_date.strftime('%d/%m')}-{last_end_date.strftime('%d/%m')}",
                                      align='center', color='#eeeeee')
            this_month_bars = ax1.bar([i + bar_width / 2 for i in x_range], this_data, width=bar_width,
                                      label=f"{this_start_date.strftime('%d/%m')}-{this_end_date.strftime('%d/%m')}",
                                      align='center', color='#10ba81')
        if mode == "MONTHLY":
            last_month_bars = ax1.bar([i - bar_width / 2 for i in x_range], last_data, width=bar_width,
                                      label=f"{last_start_date.strftime('%B %Y')}", align='center', color='#eeeeee')
            this_month_bars = ax1.bar([i + bar_width / 2 for i in x_range], this_data, width=bar_width,
                                      label=f"{this_start_date.strftime('%B %Y')}", align='center', color='#10ba81')

        ax1.set_xticks(x_range)
        ax1.set_xticklabels(x_labels, rotation=0, ha="center", fontsize=12)

        if len(str(max_data)) > 7:
            yticks_positions = list(range(0, rounded_max_data + 1 * rounded_step_data, rounded_step_data))
            yticks_positions.append(int(rounded_max_data + 2 * rounded_step_data))
            yticks_labels = [f"{int(i//(10**(len(str(max_data)) - 2)))}" + '百萬' if len(str(i)) > 6 else f"{i}" for i
                             in yticks_positions]
            for bar in last_month_bars:
                height = bar.get_height()
                ax1.text(
                    bar.get_x() + bar.get_width() / 2,
                    height,
                    f'{round(height/(10**(len(str(max_data)) - 2)),2)}'.replace('.', ',')[:4] if height > 0 else '',
                    ha='center', va='bottom', fontsize=8  # Align the text
                )
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
            yticks_labels = [f"{int(i//(10**(len(str(max_data)) - 3)))}" + '萬' if len(str(i)) > 4 and int(
                i // (10 ** (len(str(max_data)) - 3))) % 60 == 0 else "" for i in yticks_positions]
            for bar in last_month_bars:
                height = bar.get_height()
                ax1.text(
                    bar.get_x() + bar.get_width() / 2,
                    height,
                    f'{int(height/(10**(len(str(max_data)) - 3)))}'.replace('.', ',')[:4] if height > 0 else '',
                    ha='center', va='bottom', fontsize=8  # Align the text
                )
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
            name1 = f"{last_start_date.strftime('%d/%m')}-{last_end_date.strftime('%d/%m')}"
            # filtered_data = [(x, rate) for x, rate in zip(x_range, last_rate) if rate != 0]
            # x_filtered, last_rate_filtered = zip(*filtered_data)
            # last_rate_line = ax2.plot(x_filtered, last_rate_filtered,
            #                           label=f"{last_start_date.strftime('%d/%m')}-{last_end_date.strftime('%d/%m')}",
            #                           color='#F8CBAD', marker='o', linewidth=1.5)
            filtered_data = [(x, rate) for x, rate in zip(x_range, this_rate) if rate != 0]
            x_filtered, this_rate_filtered = zip(*filtered_data)
            this_rate_line = ax2.plot(x_filtered, this_rate_filtered,
                                      label=sr_achieve_rate,
                                      color='#ED7D31', marker='o', linewidth=1.5)
        if mode == "MONTHLY":
            # line_label = f"{this_start_date.strftime('%B %Y')}"
            sr_achieve_rate = "本月達成率"
            name = f"{this_start_date.strftime('%B %Y')}"
            name1 = f"{last_start_date.strftime('%B %Y')}"
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
            title = f"\n{plant} 週產量與週目標達成率 ({name})\n"
        else:
            title = f"\n{plant} 月產量與月目標達成率 ({name})\n"

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

        ax1.set_xlabel(f'{plant} (line)', labelpad=10, fontsize=12)
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
                f"{value:.1f}" if value != 0 else '',
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

        plt.text(
            x_range[-1] / 2,
            -rounded_max_rate * 0.125,
            title,
            fontsize=16, color='black', ha='center', va='center'
        )
        plt.tight_layout()

        file_name = f'MES_{plant}_{mode}_{date_mark}_Chart.png'
        chart_img = os.path.join(save_path, file_name)

        plt.savefig(f"{chart_img}", dpi=350, bbox_inches="tight", pad_inches=0.45)

        # Table Picture
        for item in data:
            item["name"] = str(item["name"]).split('_')[-1]
            item["Gap"] = int(int(item['this_time']) - int(item['last_time']))
            item["Gap percent"] = (f"+{round(float(int(item['Gap'])/int(item['last_time']))*100,2):,.2f}%" if int(
                item[
                    "Gap"]) > 0 else f"{round(float(int(item['Gap']) / int(item['last_time'])) * 100, 2):,.2f}%") if int(
                item['last_time']) > 0 else ' '
            item["Gap"] = f"{int(int(item['this_time']) - int(item['last_time'])):,}"
            item["this_time"] = f"{item['this_time']:,}"
            item["last_time"] = f"{item['last_time']:,}"

        df = pd.DataFrame(data)
        df.rename(columns={"name": f"{plant}"}, inplace=True)
        df.rename(columns={"this_time": f"{name} quantity"}, inplace=True)
        df.rename(columns={"last_time": f"{name1} quantity"}, inplace=True)
        remove_columns = ['target_this_time', 'target_last_time']
        df.drop(columns=remove_columns, inplace=True)

        font_path = font_manager.findfont("Cambria")
        font_prop = font_manager.FontProperties(fname=font_path, size=13)
        header_color = "#0A4E9B"
        row_colors = ["#E8F3FF", "#FFFFFF"]

        fig, ax = plt.subplots(figsize=(12, 8))
        ax.axis("off")
        table = ax.table(cellText=df.values, colLabels=df.columns, cellLoc="center", loc="center")
        column_widths = {0: 0.15, 1: 0.3, 2: 0.3}

        for (row, col), cell in table.get_celld().items():
            # Apply custom widths
            if col in column_widths:
                cell.set_width(column_widths[col])
            # Header styling
            if row == 0:
                cell.set_facecolor(header_color)
                cell.set_text_props(color="white", weight="bold", fontproperties=font_prop)
            # Row styling
            else:
                cell.set_facecolor(row_colors[row % 2])
                cell.set_text_props(fontproperties=font_prop)
            # Adjust height for all cells
            cell.set_height(0.08)

        if plant == 'PVC':
            plt.text(
                0.5, 1.06,  # Position: centered horizontally, above the table
                title,
                fontsize=16, color="black", ha="center", va="center", fontproperties=font_prop
            )
        elif plant == 'NBR':
            plt.text(
                0.5, 1.14,  # Position: centered horizontally, above the table
                title,
                fontsize=16, color="black", ha="center", va="center", fontproperties=font_prop
            )

        file_name = f'MES_{plant}_{mode}_{date_mark}_Table.png'
        table_img = os.path.join(save_path, file_name)

        plt.savefig(f"{table_img}", dpi=350, bbox_inches="tight")
        self.image_buffers.append(chart_img)


    def send_email(self, file_list, image_buffers):
        mode = self.mode
        date_mark = self.date_mark
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
            msg['Subject'] = f'[GD Report] 產量週報表 {date_mark}'
        elif mode == "MONTHLY":
            msg['Subject'] = f'[GD Report] 產量月報表 {date_mark}'

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


    def main(self):
        for plant in self.plant_name:
            self.generate_raw_excel(plant)
            #self.generate_chart(plant)

        #self.send_email(self.file_list, self.image_buffers)




report = mes_weekly_report("WEEKLY")
report.main()