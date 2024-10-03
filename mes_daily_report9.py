import sys
import os
import numpy as np
from matplotlib.ticker import FuncFormatter
from openpyxl.utils import get_column_letter
from database import mes_database, mes_olap_database

curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from email.mime.image import MIMEImage
import pandas as pd
from openpyxl.styles import Alignment, NamedStyle, Font, Border, Side, PatternFill
import matplotlib.pyplot as plt
from io import BytesIO
import logging
import time
from openpyxl.comments import Comment

class mes_daily_report(object):
    report_date1 = ""
    report_date2 = ""

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
            'ProductItem': '品項',
            'Line': '線別',
            'Shift': '班別',
            'max_speed': '車速(最高)',
            'min_speed': '車速(最低)',
            'avg_speed': '車速(平均)',
            'sum_qty': '產量(加總)',
            'ProductionTime': '生產時間',
            'LineSpeedStd': '標準車速',
            'Target': '目標產能',
            'Separate': '隔離%',
            'Scrap': '廢品%',
            'SecondGrade': '二級品%',
            'OverControl': '超內控%',
            'WeightValue': 'IPQC克重',
            'WeightLower': '重量下限',
            'WeightUpper': '重量上限'
        }

    # 配置日志记录器
    logging.basicConfig(
        level=logging.DEBUG,  # 设置日志级别为 DEBUG，这样所有级别的日志都会被记录
        format='%(asctime)s - %(levelname)s - %(message)s',  # 指定日志格式
        filename='app.log',  # 指定日志文件
        filemode='w'  # 写入模式，'w' 表示每次运行程序时会覆盖日志文件
    )

    def __init__(self, report_date1, report_date2):
        self.report_date1 = report_date1
        self.report_date2 = report_date2

    def read_config(self, config_file):
        smtp_config = {}
        to_emails = []

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

        return smtp_config, to_emails

    def send_email(self, file_list, image_buffers, data_date):

        smtp_config, to_emails = self.read_config('mes_daily_report_mail.config')

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
        msg['Subject'] = f'[GD Report] 產量日報表 {data_date}'

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
        for file in file_list:
            excel_file = file['excel_file']
            file_name = file['file_name']
            with open(excel_file, 'rb') as attachment:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(attachment.read())
                encoders.encode_base64(part)
                part.add_header('Content-Disposition', f"attachment; filename= {file_name}")
                msg.attach(part)
        logging.info(f"Attached Excel files")

        # Attach Picture
        for i, buffer in enumerate(image_buffers):
            image = MIMEImage(buffer.read())
            image.add_header('Content-ID', f'<chart_image{i}>')
            msg.attach(image)
        logging.info(f"Attached Picture")

        # Send Email
        # try:
        #     # server = smtplib.SMTP(smtp_server, smtp_port)
        #     # server.starttls()  # 启用 TLS 加密
        #     # server.login(smtp_user, smtp_password)  # 登录到 SMTP 服务器
        #     # server.sendmail(smtp_user, to_emails, msg.as_string())
        #     # server.quit()
        #
        #     # 發送郵件（不進行密碼驗證）
        #     server = smtplib.SMTP(smtp_server, smtp_port)
        #     server.ehlo()  # 啟動與伺服器的對話
        #     server.sendmail(smtp_user, to_emails, msg.as_string())
        #     print("Sent Email Successfully")
        # except Exception as e:
        #     print(f"Sent Email Fail: {e}")
        #     logging.info(f"Sent Email Fail: {e}")
        # finally:
        #     attachment.close()

    def main(self):
        report_date1 = self.report_date1
        report_date2 = self.report_date2
        db = mes_database()

        file_list = []

        # Email Attachment
        image_buffers = []

        # Save Path media/daily_output/
        save_path = os.path.join("daily_output")

        # Check folder to create
        if not os.path.exists(save_path):
            os.makedirs(save_path)

        for plant in ['NBR', 'PVC']:

            # Create Excel file
            file_name = f'MES_{plant}_DAILY_Report_{report_date1}.xlsx'
            excel_file = os.path.join(save_path, file_name)

            with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:

                df_main = self.get_df_main(db, report_date1, report_date2, plant)

                df_detail = self.get_df_detail(db, report_date1, report_date2, plant)

                df_final = pd.merge(df_main, df_detail, on=['Name', 'Period', 'Line'], how='left')

                df_final['Period'] = df_final['Period'].astype(str).str.zfill(2) + ":00"

                df_selected = df_final[['Date', 'Name', 'Line', 'Shift', 'WorkOrderId', 'PartNo', 'ProductItem', 'AQL', 'ProductionTime', 'Period', 'max_speed', 'min_speed', 'avg_speed', 'LineSpeedStd', 'sum_qty', 'Separate', 'Target', 'Scrap', 'SecondGrade', 'OverControl', 'WeightValue', 'WeightLower', 'WeightUpper']]

                df_with_subtotals, df_chart = self.sorting_data(df_selected)
                self.generate_summary_excel(writer, df_with_subtotals)

                machine_groups = df_selected.groupby('Name')

                for machine_name, machine_df in machine_groups:
                    # 處理停機情況
                    if not machine_df['ProductItem'].iloc[0]:
                        continue

                    machine_df = machine_df.sort_values(by=['Date', 'Shift', 'Period'])
                    self.generate_excel(writer, machine_df, plant, machine_name)

            file_list.append({'file_name': file_name, 'excel_file': excel_file})

            # Generate Chart
            image_buffer = self.generate_chart(save_path, plant, report_date1, df_chart)
            image_buffers.append(image_buffer)

            self.delete_mes_olap(report_date1, report_date2)
            self.insert_mes_olap(df_selected)

        # Send Email
        max_reSend = 5
        reSent = 0
        while reSent<max_reSend:
            try:
                self.send_email(file_list, image_buffers, report_date1)
                print('Email sent successfully')
                break
            except Exception as e:
                print(f'Email sending failed: {e}')
                reSent += 1
                if reSent >= max_reSend:
                    print('Failed to send email after 5 retries')
                    break
                time.sleep(180) #seconds
    def delete_mes_olap(self,report_date1,report_date2):
        db = mes_olap_database()
        table_name = '[MES_OLAP].[dbo].[mes_daily_report_raw]'
        sql_delete = f"""
            delete
            from {table_name}
            WHERE ((date = '{report_date2}' AND period BETWEEN '00:00' AND '05:00')
            OR (date = '{report_date1}' AND period BETWEEN '06:00' AND '23:00'))
        """
        db.execute_sql(sql_delete)


    def insert_mes_olap(self, df):
        df = df.replace({np.nan: 'null'})
        db = mes_olap_database()
        table_name = '[MES_OLAP].[dbo].[mes_daily_report_raw]'
        for index, row in df.iterrows():
            Date = row['Date']
            Name = row['Name']
            Line = row['Line']
            Shift = row['Shift']
            WorkOrderId = row['WorkOrderId']
            PartNo = row['PartNo']
            ProductItem = row['ProductItem']
            AQL = row['AQL']
            ProductionTime = row['ProductionTime']
            Period = row['Period']
            max_speed = row['max_speed']
            min_speed = row['min_speed']
            avg_speed = row['avg_speed']
            LineSpeedStd = row['LineSpeedStd']
            sum_qty = row['sum_qty']
            Separate = row['Separate']
            Target = row['Target']
            Scrap = row['Scrap']
            SecondGrade = row['SecondGrade']
            OverControl = row['OverControl']
            WeightValue = row['WeightValue']
            WeightLower = row['WeightLower']
            WeightUpper = row['WeightUpper']

            sql = f"""insert into {table_name}(Date,Name,Line,Shift,WorkOrderId,PartNo,ProductItem,AQL,ProductionTime,
            Period,max_speed,min_speed,avg_speed,LineSpeedStd,sum_qty,Separate,Target,Scrap,SecondGrade,OverControl,
            WeightValue,WeightLower,WeightUpper, update_time) Values('{Date}','{Name}','{Line}',N'{Shift}','{WorkOrderId}','{PartNo}',
            '{ProductItem}','{AQL}',{ProductionTime},'{Period}',{max_speed},{min_speed},{avg_speed},{LineSpeedStd},
            {sum_qty},'{Separate}',{Target},'{Scrap}','{SecondGrade}','{OverControl}',{WeightValue},{WeightLower},{WeightUpper}, GETDATE())"""
            # print(sql)
            db.execute_sql(sql)


    def shift(self, period):
        try:
            if 6 <= int(period) <= 17:
                return '早班'
            else:
                return '晚班'
        except Exception as ex:
            return ''

       # Work Order
    def get_df_main(self, db, report_date1, report_date2, plant):
        scada_table = ""
        upper_column = ""
        lower_column = ""

        if plant == "NBR":
            upper_column = "UpperLineSpeed_Min"
            lower_column = "LowerLineSpeed_Min"
            scada_table = "[PMGMES].[dbo].[PMG_MES_NBR_SCADA_Std]"
        elif plant == "PVC":
            upper_column = "UpperSpeed"
            lower_column = "LowerSpeed"
            scada_table = "[PMGMES].[dbo].[PMG_MES_PVC_SCADA_Std]"

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
                    (nss.{upper_column}+nss.{lower_column})/2 LineSpeedStd,
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
					r.Id runcard
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
            CountingData AS (
                SELECT 
                    m.mes_machine,
                    CAST(DATEPART(hour, c.CreationTime) AS INT) AS Period,
                    m.line,
                    COUNT(*) * 5 AS CountedValue
                FROM 
                    [PMG_DEVICE].[dbo].[COUNTING_DATA] c
                    JOIN [PMG_DEVICE].[dbo].[COUNTING_DATA_MACHINE] m ON c.MachineName = m.counting_machine
                WHERE 
                    m.mes_machine LIKE '%{plant}%' AND COUNTING_MACHINE LIKE '%CountingMachine%'
                    AND c.CreationTime BETWEEN CONVERT(DATETIME, '{report_date1} 06:00:00', 120) AND CONVERT(DATETIME, '{report_date2} 05:59:59', 120)
                    AND (c.speed < 60 OR c.speed IS NULL)
                GROUP BY 
                    m.mes_machine,
                    FORMAT(c.CreationTime, 'yyyy-MM-dd'),
                    DATEPART(hour, c.CreationTime),
                    m.line
            ),
			Machines as (
				select distinct MES_MACHINE Name from [PMG_DEVICE].[dbo].[COUNTING_DATA] c
                    join [PMG_DEVICE].[dbo].[COUNTING_DATA_MACHINE] m on c.MachineName = m.COUNTING_MACHINE
                    where creationTime between CONVERT(DATETIME, '{report_date1} 06:00:00', 120) AND CONVERT(DATETIME, '{report_date2} 05:59:59', 120)
                    and m.MES_MACHINE like '%{plant}%'
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
                wo.PlanQty,
                wo.Qty,
                wo.Status,
                CAST(wo.LineSpeedStd AS INT) AS LineSpeedStd,
                60 AS ProductionTime,
                hole_result Separate,
                '' Scrap,
                '' SecondGrade,
                CAST(60 * wo.LineSpeedStd AS INT) AS Target,
                weight_result OverControl,
                CAST(round(weight_value,2) AS DECIMAL(10, 2)) WeightValue,
				CAST(round(weight_lower,2) AS DECIMAL(10, 2)) WeightLower,
				CAST(round(weight_upper,2) AS DECIMAL(10, 2)) WeightUpper,
                runcard
                
            FROM 
                Machines mach
                LEFT JOIN WorkOrderInfo wo ON mach.Name = wo.Name
                LEFT JOIN CountingData cd ON wo.Name = cd.mes_machine AND wo.Line = cd.line AND wo.Period = cd.Period
            ORDER BY 
                mach.Name, 
                wo.Period, 
                wo.Line;
                """
        raws = db.select_sql_dict(sql)

        df_main = pd.DataFrame(raws)

        # Add Column Shift
        df_main['Shift'] = df_main['Period'].apply(self.shift)

        return df_main

    # Counting Machine Data
    def get_df_detail(self, db, report_date1, report_date2, plant):

        sql = f"""
                        SELECT FORMAT(CreationTime, 'yyyy-MM-dd') AS Date,CAST(DATEPART(hour, CreationTime) as INT) Period ,m.mes_machine Name,m.line Line, max(Speed) max_speed,min(Speed) min_speed,round(avg(Speed),0) avg_speed,sum(Qty2) sum_qty
                          FROM [PMG_DEVICE].[dbo].[COUNTING_DATA] c, [PMG_DEVICE].[dbo].[COUNTING_DATA_MACHINE] m
                          where CreationTime between CONVERT(DATETIME, '{report_date1} 06:00:00', 120) and CONVERT(DATETIME, '{report_date2} 05:59:59', 120)
                          and c.MachineName = m.counting_machine and m.mes_machine like '%{plant}%'
                          group by m.mes_machine,FORMAT(CreationTime, 'yyyy-MM-dd'),DATEPART(hour, CreationTime),m.line
                          order by m.mes_machine,FORMAT(CreationTime, 'yyyy-MM-dd'),DATEPART(hour, CreationTime),m.line
                        """
        detail_raws = db.select_sql_dict(sql)
        df_detail = pd.DataFrame(detail_raws)

        return df_detail

    def generate_excel(self, writer, df, plant, machine_name):
        # 轉出Excel前進行資料處理
        df['ProductionTime'] = (df['ProductionTime'] // 60).astype(str) + 'H'
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
                if col_letter in ['K', 'L', 'M', 'N']:  # Apply right alignment for specific columns
                    cell.alignment = self.right_align_style.alignment
                elif col_letter in ['O', 'Q']:
                    cell.number_format = '#,##0'
                    cell.alignment = self.center_align_style.alignment
                elif col_letter in ['U']:
                    try:
                        cell.value = float(cell.value)
                    except ValueError:
                        pass
                else:
                    cell.alignment = self.center_align_style.alignment

        worksheet[f'V1'].value = None  # 清空克重下限欄位
        worksheet[f'W1'].value = None  # 清空克重上限欄位
        for row in range(2, worksheet.max_row + 1):  # 從第2行開始，因為第1行是標題
            weight_value_cell = worksheet[f'U{row}']
            weight_lower_cell = worksheet[f'V{row}'].value
            weight_upper_cell = worksheet[f'W{row}'].value

            if weight_lower_cell or weight_upper_cell:
                comment = Comment(text="IPQC範圍("+weight_lower_cell+"-"+weight_upper_cell+")", author="System")  # 創建註解
                weight_value_cell.comment = comment

            worksheet[f'V{row}'].value = None  # 清空克重下限欄位
            worksheet[f'W{row}'].value = None  # 清空克重上限欄位

        return workbook

    def generate_summary_excel(self, writer, df):

        colmn_index = {'Name': 0, 'ProductItem': 1, 'AQL': 2, 'Shift': 3, 'Line': 4, 'max_speed': 5, 'min_speed': 6,
                       'avg_speed': 7, 'LineSpeedStd': 8, 'ProductionTime': 9, 'sum_qty': 10, 'Separate': 11, 'Scrap': 12, 'SecondGrade': 13, 'Target': 14, 'OverControl': 15}
        colmn_letter = {'Name': 'A', 'ProductItem': 'B', 'AQL': 'C', 'Shift': 'D', 'Line': 'E',
                       'max_speed': 'F', 'min_speed': 'G', 'avg_speed': 'H', 'LineSpeedStd': 'I', 'ProductionTime': 'J', 'sum_qty': 'K', 'Separate': 'L', 'Scrap': 'M', 'SecondGrade': 'N', 'Target': 'O', 'OverControl': 'P'}

        # Create a bold font style
        bold_font = Font(bold=True)

        # Create a border style with a bold line above
        thick_border = Border(top=Side(style='thick'), bottom=Side(style='thick'))

        namesheet = "Summary"
        # Write data to the Excel sheet with the machine name as the sheet name
        df.to_excel(writer, sheet_name=namesheet, index=False)

        # Read the written Excel file
        workbook = writer.book
        worksheet = writer.sheets[namesheet]

        # Freeze the first row
        worksheet.freeze_panes = worksheet['A2']

        # Formatting
        for col in worksheet.columns:
            col_letter = col[0].column_letter
            max_length = max(len(str(cell.value)) for cell in col)

            for cell in col:
                if col_letter in [colmn_letter['Name']]:  # 检查是否为指定的列
                    worksheet.column_dimensions[col_letter].width = max_length + 5
                elif col_letter in [colmn_letter['ProductItem']]:
                    worksheet.column_dimensions[col_letter].width = max_length + 5
                    self.left_align_style = Alignment(horizontal='left')
                    cell.alignment = self.left_align_style
                elif col_letter in [colmn_letter['max_speed'], colmn_letter['min_speed'], colmn_letter['avg_speed'], colmn_letter['LineSpeedStd']]:  # 检查是否为指定的列
                    worksheet.column_dimensions[col_letter].width = 15
                    cell.alignment = self.right_align_style.alignment
                elif col_letter in [colmn_letter['ProductionTime']]:
                    worksheet.column_dimensions[col_letter].width = 15
                    cell.alignment = self.center_align_style.alignment
                elif col_letter in [colmn_letter['sum_qty'], colmn_letter['Target']]:
                    worksheet.column_dimensions[col_letter].width = 15
                    cell.alignment = self.right_align_style.alignment
                    cell.number_format = '#,##0'
                elif col_letter in [colmn_letter['Separate'], colmn_letter['Scrap'], colmn_letter['SecondGrade'], colmn_letter['OverControl']]:
                    worksheet.column_dimensions[col_letter].width = 10
                    cell.alignment = self.center_align_style.alignment
                    cell.number_format = '0.0%'  # 百分比格式，小數點 1 位
                else:
                    cell.alignment = self.center_align_style.alignment


        # Search all lines, bold font and bold line above
        index_start = 2
        index_end = 1
        for row in worksheet.iter_rows(min_row=2, max_row=worksheet.max_row):
            if row[colmn_index['Line']].value != '':  # Line
                for cell in row[colmn_index['Line']:]:
                    cell.fill = PatternFill(start_color="FDE9D9", end_color="FDE9D9", fill_type="solid")


            if row[colmn_index['ProductItem']].value != '' and row[colmn_index['Shift']].value == '':
                index_end += 1

            if row[colmn_index['Shift']].value != '': # Shift
                worksheet.row_dimensions.group(index_start, index_end, hidden=True, outline_level=2)

                index_start = index_end + 1
                index_end = index_end + 1
                worksheet.row_dimensions.group(index_start, index_end, hidden=True, outline_level=1)
                index_start = index_end + 1

                for cell in row[colmn_index['Shift']:]:
                    cell.font = bold_font
                    cell.border = Border(top=Side(style='thin'))

            elif row[colmn_index['Name']].value != '':  # Machine
                # Hide detailed data
                worksheet.row_dimensions.group(index_start, index_end, hidden=False, outline_level=0)
                index_start = index_end + 1

                for cell in row:
                    cell.font = bold_font
                    cell.border = thick_border

            # 設置欄的 outlineLevel 讓其可以折疊/展開
            worksheet.column_dimensions[colmn_letter['Shift']].outlineLevel = 1
            worksheet.column_dimensions[colmn_letter['Line']].outlineLevel = 1
            worksheet.column_dimensions[colmn_letter['max_speed']].outlineLevel = 1
            worksheet.column_dimensions[colmn_letter['min_speed']].outlineLevel = 1

            # 總共折疊的區域
            worksheet.column_dimensions.group(colmn_letter['Shift'], colmn_letter['min_speed'], hidden=True)

        return workbook

    # Sorting data
    def sorting_data(self, df):

        chart_rows = []

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

        try:
            # Drop the 'Period' and 'Date' column from each group
            group_without_period = df.drop(columns=['Period', 'Date'])

            # Data group by 'Name and then calculating
            mach_grouped = group_without_period.groupby(['Name'])

            rows = []
            chart_rows = []

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
                    # line_sum_df = line_group.groupby(['Name', 'Shift', 'Line']).agg({
                    #     'min_speed': 'min',  # Min speed
                    #     'max_speed': 'max',  # Max speed
                    #     'avg_speed': 'mean',  # Average speed
                    #     'sum_qty': 'sum',
                    #     'ProductionTime': 'sum',
                    #     'UpperLineSpeed': 'mean',
                    #     'Target': 'sum',
                    # }).reset_index()
                    #
                    # # Add AQL Column to fit format
                    # line_sum_df.insert(line_sum_df.columns.get_loc('Name') + 1, 'ProductItem', None)
                    # line_sum_df.insert(line_sum_df.columns.get_loc('Name') + 2, 'AQL', None)
                    mean_speed = line_group['LineSpeedStd'].mean()
                    mean_speed = round(mean_speed) if not pd.isna(mean_speed) else 0

                    line_sum = {
                        'Name': '',
                        'ProductItem': join_values(line_group['ProductItem']),
                        'AQL': join_values(line_group['AQL']),
                        'Shift': join_values(line_group['Shift']),
                        'Line': join_values(line_group['Line']),
                        'max_speed': line_group['max_speed'].max(),
                        'min_speed': line_group['min_speed'].min(),
                        'avg_speed': line_group['avg_speed'].mean(),
                        'LineSpeedStd': mean_speed,
                        'ProductionTime': min2hour(line_group['ProductionTime']),
                        'sum_qty': line_group['sum_qty'].sum(),
                        'Separate': counting_ng_ratio(line_group['Separate']),
                        'Scrap': '',
                        'SecondGrade': '',
                        'Target': line_group['Target'].sum(),
                        'OverControl': counting_ng_ratio(line_group['OverControl']),
                    }
                    line_sum_df = pd.DataFrame([line_sum])

                    tmp_rows.append(line_sum_df)

                df_tmp = pd.concat(tmp_rows, ignore_index=True)

                # Sorting Data
                # Day Shift
                day_df = df_tmp[df_tmp['Shift'] == '早班'].copy()

                mean_speed = day_df['LineSpeedStd'].mean()
                mean_speed = round(mean_speed) if not pd.isna(mean_speed) else 0

                subtotal = {
                    'Name': '',
                    'ProductItem': '',
                    'AQL': '',
                    'Shift': join_values(day_df['Shift']),
                    'Line': '',
                    'max_speed': day_df['max_speed'].max(),
                    'min_speed': day_df['min_speed'].min(),
                    'avg_speed': day_df['avg_speed'].mean(),
                    'LineSpeedStd': mean_speed,
                    'ProductionTime': day_df['ProductionTime'].mean(),
                    'sum_qty': day_df['sum_qty'].sum(),
                    'Separate': day_df['Separate'].mean(),
                    'Scrap': '',
                    'SecondGrade': '',
                    'Target': day_df['Target'].sum(),
                    'OverControl': day_df['OverControl'].mean(),
                }
                subtotal_df = pd.DataFrame([subtotal])
                subtotal_df['avg_speed'] = subtotal_df['avg_speed'].round(0)

                day_df[['Name', 'Shift']] = ''
                day_df['avg_speed'] = day_df['avg_speed'].round(0)
                if not day_df.empty:
                    rows.append(day_df)  # Day row data
                    rows.append(subtotal_df)  # Day Shift total summary

                # Night Shift
                night_df = df_tmp[df_tmp['Shift'] == '晚班'].copy()

                mean_speed = night_df['LineSpeedStd'].mean()
                mean_speed = round(mean_speed) if not pd.isna(mean_speed) else 0

                subtotal = {
                    'Name': '',
                    'ProductItem': '',
                    'AQL': '',
                    'Shift': join_values(night_df['Shift']),
                    'Line': '',
                    'max_speed': night_df['max_speed'].max(),
                    'min_speed': night_df['min_speed'].min(),
                    'avg_speed': night_df['avg_speed'].mean(),
                    'LineSpeedStd': mean_speed,
                    'ProductionTime': night_df['ProductionTime'].mean(),
                    'sum_qty': night_df['sum_qty'].sum(),
                    'Separate': night_df['Separate'].mean(),
                    'Scrap': '',
                    'SecondGrade': '',
                    'Target': night_df['Target'].sum(),
                    'OverControl': night_df['OverControl'].mean(),
                }
                subtotal_df = pd.DataFrame([subtotal])
                subtotal_df['avg_speed'] = subtotal_df['avg_speed'].round(0)

                night_df[['Name', 'Shift']] = ''
                night_df['avg_speed'] = night_df['avg_speed'].round(0)
                if not night_df.empty:
                    rows.append(night_df)  # Night row data
                    rows.append(subtotal_df)  # Night Shift total summary

                # Machine total summary
                day_production_time = 0
                night_production_time = 0
                if not np.isnan(day_df['ProductionTime'].mean()):
                    day_production_time = day_df['ProductionTime'].mean()

                if not np.isnan(night_df['ProductionTime'].mean()):
                    night_production_time = night_df['ProductionTime'].mean()

                mean_speed = mach_group['LineSpeedStd'].mean()
                mean_speed = round(mean_speed) if not pd.isna(mean_speed) else 0

                subtotal = {
                    'Name': join_values(mach_group['Name']),
                    'ProductItem': join_values(mach_group['ProductItem']),
                    'AQL': join_values(mach_group['AQL']),
                    'Shift': '',
                    'Line': '',
                    'max_speed': mach_group['max_speed'].max(),
                    'min_speed': mach_group['min_speed'].min(),
                    'avg_speed': mach_group['avg_speed'].mean(),
                    'LineSpeedStd': mean_speed,
                    'ProductionTime': day_production_time+night_production_time,
                    'sum_qty': mach_group['sum_qty'].sum(),
                    'Separate': counting_ng_ratio(mach_group['Separate']),
                    'Scrap': '',
                    'SecondGrade': '',
                    'Target': mach_group['Target'].sum(),
                    'OverControl': counting_ng_ratio(mach_group['OverControl']),
                }
                subtotal_df = pd.DataFrame([subtotal])
                subtotal_df['avg_speed'] = subtotal_df['avg_speed'].round(0)
                rows.append(subtotal_df)  # Machine total summary
                chart_rows.append(subtotal_df)

            # Combine the grouped data into a DataFrame
            df_with_subtotals = pd.concat(rows, ignore_index=True)

            # ProductionTime加上小時的文字
            df_with_subtotals['ProductionTime'] = df_with_subtotals['ProductionTime'].astype(str) + 'H'
            # Change column names
            df_with_subtotals.rename(columns=self.header_columns, inplace=True)

            # Group the total quantity of each machine into a DataFrame
            df_chart = pd.concat(chart_rows, ignore_index=True)

        except Exception as e:
            print(f"An error occurred: {e}")

        return df_with_subtotals, df_chart

    def generate_chart(self, save_path, plant, report_date, df_chart):
        # Create Chart
        fig, ax1 = plt.subplots(figsize=(10, 6))

        plt.rcParams['font.sans-serif'] = ['Microsoft YaHei']

        # Only substring Name right 3 characters
        df_chart['Name_short'] = df_chart['Name'].apply(lambda x: x[-3:])

        df_chart['Unfinished'] = (df_chart['Target'] - df_chart['sum_qty']).apply(lambda x: max(x, 0))  # 未達成數量, 負數為0
        df_chart['Achievement Rate'] = df_chart['sum_qty'] / df_chart['Target'] * 100  # 達成率（百分比）
        df_chart.loc[df_chart['Target'] == 0, 'Achievement Rate'] = None  # 當 Target 為 0，將達成率設為 None

        # Draw Bar Chart
        bar_width = 0.6
        bars = ax1.bar(df_chart['Name_short'], df_chart['sum_qty'], width=bar_width, color='lightcoral', label='達成率')
        ax1.bar(df_chart['Name_short'], df_chart['Unfinished'], width=bar_width, bottom=df_chart['sum_qty'], color='lightgreen')

        # Create a second Y axis
        # ax2 = ax1.twinx()

        # Draw Line Chart (speed)
        # ax2.plot(df_chart['Name_short'], df_chart['Achievement Rate'], color='red', marker='o')
        # ax2.set_ylabel('Achievement Rate (%)', color='red')

        # Set the X-axis label and the Y-axis label
        ax1.set_xlabel('機台')
        ax1.set_ylabel('目標產能')
        # 設置 Y 軸的上限為 120 萬
        ax1.set_ylim(0, 1200000)

        # 自定義 Y 軸以 10 萬為單位
        def y_formatter(x, pos):
            return f'{int(x/10000)}萬'  # 將數值轉換為「萬」的單位顯示

        ax1.yaxis.set_major_formatter(FuncFormatter(y_formatter))

        # 在每個長條圖上方顯示達成率百分比
        for bar, unfinished, rate in zip(bars, df_chart['Unfinished'], df_chart['Achievement Rate']):
            if pd.notnull(rate):  # 僅顯示達成率不為 None 的數值
                height = bar.get_height() + unfinished  # 計算長條的總高度
                ax1.text(bar.get_x() + bar.get_width() / 2, height + 20000, f'{rate:.1f}%', ha='center', va='bottom',
                         fontsize=10)

        plt.title(f'{plant} {report_date}產量')

        # Display the legend of the bar chart and line chart together
        fig.legend(loc="upper right", bbox_to_anchor=(1, 1), bbox_transform=ax1.transAxes)

        # Save the image to a local file
        image_file = f'{plant}_bar_chart_{report_date}.png'
        image_file = os.path.join(save_path, image_file)

        plt.savefig(image_file)

        # Save the image to a BytesIO object
        image_stream = BytesIO()
        plt.savefig(image_stream, format='png')
        image_stream.seek(0)  # Move the pointer to the beginning of the file
        plt.close()  # Close the image to free up memory

        return image_stream

    # def generate_chart(self, save_path, plant, report_date, df_chart):
    #     # Create Chart
    #     fig, ax1 = plt.subplots()
    #
    #     # Only substring Name right 3 characters
    #     df_chart['Name_short'] = df_chart['Name'].apply(lambda x: x[-3:])
    #
    #     # Draw Bar Chart
    #     bars = ax1.bar(df_chart['Name_short'], df_chart['sum_qty'])
    #
    #     # Display quantity above each bar
    #     for bar in bars:
    #         yval = bar.get_height()  # 获取条形的高度，也就是数量
    #         ax1.text(bar.get_x() + bar.get_width() / 2, yval, f'{int(yval):,}',
    #                  ha='center', va='bottom')  # 显示数量并居中
    #
    #     # Create a second Y axis
    #     ax2 = ax1.twinx()
    #
    #     # Draw Line Chart (speed)
    #     ax2.plot(df_chart['Name_short'], df_chart['avg_speed'], color='red', marker='o')
    #
    #
    #     # Set the X-axis label and the Y-axis label
    #     ax1.set_xlabel('Machine')
    #     ax1.set_ylabel('Output')
    #     ax2.set_ylabel('Average Speed', color='red')
    #     plt.title(f'{plant} Sum Quantity per Machine')
    #
    #     # Display the legend of the bar chart and line chart together
    #     fig.legend(loc="upper right", bbox_to_anchor=(1, 1), bbox_transform=ax1.transAxes)
    #
    #     # Save the image to a local file
    #     image_file = f'{plant}_bar_chart_{report_date}.png'
    #     image_file = os.path.join(save_path, image_file)
    #
    #     plt.savefig(image_file)
    #
    #     # Save the image to a BytesIO object
    #     image_stream = BytesIO()
    #     plt.savefig(image_stream, format='png')
    #     image_stream.seek(0)  # Move the pointer to the beginning of the file
    #     plt.close()  # Close the image to free up memory
    #
    #     return image_stream

from datetime import datetime, timedelta
report_date1 = datetime.today() - timedelta(days=1)
report_date1 = report_date1.strftime('%Y%m%d')

report_date2 = datetime.today()
report_date2 = report_date2.strftime('%Y%m%d')

# report_date1 = "2024-09-26"
# report_date2 = "2024-09-27"

report = mes_daily_report(report_date1, report_date2)
report.main()
