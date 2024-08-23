import sys
import os
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
from jobs.database import mes_database
from openpyxl.styles import Alignment, NamedStyle, Font, Border, Side

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

    def __init__(self, report_date1, report_date2):
        self.report_date1 = report_date1
        self.report_date2 = report_date2


    def send_email(self, file_name, excel_file, image_buffers, data_date):
        # SMTP Sever config setting
        smtp_server = 'smtp.gmail.com'
        smtp_port = 465
        smtp_user = 'driversystemalert@gmail.com'
        smtp_password = 'cqvvjiccyxlwsdot'

        # Receiver
        to_emails = ['lelongcuong429@gmail.com']

        # Mail Info
        msg = MIMEMultipart()
        msg['From'] = smtp_user
        msg['To'] = ', '.join(to_emails)
        msg['Subject'] = f'江田廠產量日報表 {data_date}'

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

        # Attach Picture
        for i, buffer in enumerate(image_buffers):
            image = MIMEImage(buffer.read())
            image.add_header('Content-ID', f'<chart_image{i}>')
            msg.attach(image)

        # Attach Excel
        with open(excel_file, 'rb') as attachment:
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(attachment.read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', f"attachment; filename= {file_name}")
            msg.attach(part)

        # Send Email
        try:
            server = smtplib.SMTP(smtp_server, smtp_port)
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.sendmail(smtp_user, to_emails, msg.as_string())
            server.quit()
            print("Sent Email Successfully")
        except Exception as e:
            print(f"Sent Email Fail: {e}")
        finally:
            attachment.close()

    def main(self):
        report_date1 = self.report_date1
        report_date2 = self.report_date2
        db = mes_database()

        # Save Path media/daily_output/
        save_path = os.path.join("..", "media", "daily_output")

        # Check folder to create
        if not os.path.exists(save_path):
            os.makedirs(save_path)

        for plant in ['NBR', 'PVC']:

            # Create Excel file
            file_name = f'MES_OUTPUT_DAILY_Report_{report_date1}_{plant}.xlsx'
            excel_file = os.path.join(save_path, file_name)

            with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:

                df_main = self.get_df_main(db, report_date1, report_date2, plant)

                df_detail = self.get_df_detail(db, report_date1, report_date2, plant)

                df_final = pd.merge(df_main, df_detail, on=['Name', 'Period', 'Line'], how='left')

                df_final['Period'] = df_final['Period'].astype(str).str.zfill(2) + ":00"

                df_selected = df_final[['Date', 'Name', 'Line', 'Shift', 'WorkOrderId', 'PartNo', 'ProductItem', 'AQL', 'Period', 'max_speed', 'min_speed', 'avg_speed', 'sum_qty']]

                machine_groups = df_selected.groupby('Name')

                for machine_name, machine_df in machine_groups:
                    machine_df = machine_df.sort_values(by=['Date', 'Shift', 'Period'])
                    self.generate_excel(writer, machine_df, plant, machine_name)


    def shift(self, period):
        if 6 <= int(period) <= 17:
            return '早班'
        else:
            return '晚班'

       # Work Order
    def get_df_main(self, db, report_date1, report_date2, plant):
        sql = f"""
                          SELECT w.MachineId,Name,wi.LineId Line,CAST(r.Period as INT) Period,wi.StartDate, wi.EndDate, wi.WorkOrderId,WorkOrderDate,CustomerName,PartNo,ProductItem,w.AQL,w.PlanQty,wi.Qty,w.Status
                          FROM [PMG_MES_WorkOrderInfo] wi, [PMG_MES_WorkOrder] w, [PMG_DML_DataModelList] dl,[PMG_MES_RunCard] r
                          where wi.WorkOrderId = w.id and w.MachineId = dl.Id and r.WorkOrderId = w.Id and r.LineName = wi.LineId
                          and wi.StartDate between CONVERT(DATETIME, '{report_date1} 05:30:00', 120) and CONVERT(DATETIME, '{report_date2} 05:29:59', 120)
                          and Name like '%{plant}%'
                          order by Name,CAST(r.Period as INT),wi.LineId
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
                if col_letter in ['J', 'K', 'L', 'M', 'N']:  # Apply right alignment for specific columns
                    cell.alignment = self.right_align_style.alignment
                else:
                    cell.alignment = self.center_align_style.alignment

        return workbook

from datetime import datetime, timedelta
report_date1 = datetime.today() - timedelta(days=1)
report_date1 = report_date1.strftime('%Y%m%d')

report_date2 = datetime.today()
report_date2 = report_date2.strftime('%Y%m%d')

report = mes_daily_report(report_date1, report_date2)
report.main()
