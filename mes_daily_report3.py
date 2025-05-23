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
import matplotlib.pyplot as plt
from io import BytesIO


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
        smtp_server = 'mail.egvnco.com'
        smtp_port = 587
        smtp_user = 'box.chang@egvnco.com'
        smtp_password = '1qazxsw2'

        # Receiver
        to_emails = ['box.chang@egvnco.com']

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

        # Create Excel file
        file_name = f'MES_OUTPUT_DAILY_Report_{report_date1}.xlsx'
        excel_file = os.path.join(save_path, file_name)

        # Email Attachment
        image_buffers = []

        with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
            for plant in ['NBR', 'PVC']:
                # Get Work Order as df_main
                df_main = self.get_df_main(db, report_date1, report_date2, plant)

                # Get Counting Machine as df_detail
                df_detail = self.get_df_detail(db, report_date1, report_date2, plant)

                # Work Order data combine Counting Machine data
                df_final = pd.merge(df_main, df_detail, on=['Name', 'Period', 'Line'], how='left')

                # Choose needed rows
                df_selected = df_final[['Name', 'ProductItem', 'AQL', 'Period', 'Shift',  'Line', 'max_speed', 'min_speed', 'avg_speed', 'sum_qty']]

                # Create new DataFrame to save final results
                # Raw data + sum raw
                df_with_subtotals = self.sorting_data(df_selected)

                # Excel data
                workbook = self.generate_excel(writer, df_with_subtotals, plant)

                # Save document
                workbook.save(excel_file)

                # Generate Chart
                image_buffer = self.generate_chart(save_path, plant, report_date1, df_chart)
                image_buffers.append(image_buffer)

        # Send Email
        self.send_email(file_name, excel_file, image_buffers, report_date1)



    def shift(self, period):
        if 6 <= int(period) <= 17:
            return '早班'
        else:
            return '晚班'

    def generate_chart(self, save_path, plant, report_date, df_chart):
        # Create Chart
        fig, ax1 = plt.subplots()

        # Only substring Name right 3 characters
        df_chart['Name_short'] = df_chart['Name'].apply(lambda x: x[-3:])

        # Draw Bar Chart
        bars = ax1.bar(df_chart['Name_short'], df_chart['sum_qty'], color='blue', label='Quantity')

        # Display quantity above each bar
        for bar in bars:
            yval = bar.get_height()  # 获取条形的高度，也就是数量
            ax1.text(bar.get_x() + bar.get_width() / 2, yval, f'{int(yval):,}',
                     ha='center', va='bottom')  # 显示数量并居中

        # Create a second Y axis
        ax2 = ax1.twinx()

        # Draw Line Chart (speed)
        ax2.plot(df_chart['Name_short'], df_chart['avg_speed'], color='red', marker='o', label='Speed')


        # Set the X-axis label and the Y-axis label
        ax1.set_xlabel('Machine')
        ax1.set_ylabel('Output')
        ax2.set_ylabel('Speed', color='red')
        plt.title(f'{plant} Sum Quantity per Machine')

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

    # Optical Machine Data
    def get_df_optical(self, db, report_date1, report_date2, plant):

        sql = f"""
                        SELECT CAST(DATEPART(hour, Cdt) as INT) Period,m.mes_machine Name,m.line Line,Sum(OKQty) OKQty, Sum(NGQty) NGQty,
                        CASE 
                                WHEN Sum(NGQty) = 0 THEN 0
                                ELSE Round(CAST(Sum(NGQty) AS FLOAT) / (Sum(OKQty) + Sum(NGQty)),3)
                            END AS Ng_Ratio
                          FROM [PMG_DEVICE].[dbo].[OpticalDevice] d, [PMG_DEVICE].[dbo].[COUNTING_DATA_MACHINE] m
                          where Cdt between CONVERT(DATETIME, '{report_date1} 05:00:00', 120) and CONVERT(DATETIME, '{report_date2} 05:59:59', 120)
                          and d.DeviceId = m.Counting_machine
                          and m.mes_machine like '%{plant}%'
                          group by m.mes_machine, DATEPART(hour, Cdt),m.line
                        """
        optical_raws = db.select_sql_dict(sql)
        df_optical = pd.DataFrame(optical_raws)

        return df_optical

    # Sorting data
    def sorting_data(self, df):
        def join_values(col):
            return '/'.join(map(str, sorted(set(col))))

        try:
            # Drop the 'Period' column from each group
            group_without_period = df.drop(columns=['Period'])

            # Data group by 'Name and then calculating
            mach_grouped = group_without_period.groupby(['Name'])

            rows = []

            for mach_name, mach_group in mach_grouped:

                line_grouped = mach_group.groupby(['Shift', 'Line'])

                tmp_rows = []
                for line_name, line_group in line_grouped:

                    line_sum_df = line_group.groupby(['Name', 'ProductItem', 'AQL', 'Shift', 'Line']).agg({
                        'min_speed': 'min',  # Min speed
                        'max_speed': 'max',  # Max speed
                        'avg_speed': 'mean',  # Average speed
                        'sum_qty': 'sum',
                    }).reset_index()
                    tmp_rows.append(line_sum_df)

                df_tmp = pd.concat(tmp_rows, ignore_index=True)

                # Sorting Data
                day_df = df_tmp[df_tmp['Shift'] == '早班']
                day_df[['Name', 'ProductItem', 'AQL']] = ''
                rows.append(day_df)  # Day row data
                subtotal = {
                    'Name': '',
                    'ProductItem': '',
                    'AQL': '',
                    'Shift': join_values(day_df['Shift']),
                    'Line': '',
                    'max_speed': day_df['max_speed'].max(),
                    'min_speed': day_df['min_speed'].min(),
                    'avg_speed': day_df['avg_speed'].mean(),
                    'sum_qty': day_df['sum_qty'].sum(),
                }
                subtotal_df = pd.DataFrame([subtotal])
                rows.append(subtotal_df)  # Day Shift total summary

                night_df = df_tmp[df_tmp['Shift'] == '晚班']
                night_df[['Name', 'ProductItem', 'AQL']] = ''
                rows.append(night_df)  # Night row data
                subtotal = {
                    'Name': '',
                    'ProductItem': '',
                    'AQL': '',
                    'Shift': join_values(night_df['Shift']),
                    'Line': '',
                    'max_speed': night_df['max_speed'].max(),
                    'min_speed': night_df['min_speed'].min(),
                    'avg_speed': night_df['avg_speed'].mean(),
                    'sum_qty': night_df['sum_qty'].sum(),
                }
                subtotal_df = pd.DataFrame([subtotal])
                rows.append(subtotal_df)  # Night Shift total summary

                subtotal = {
                    'Name': join_values(mach_group['Name']),
                    'ProductItem': join_values(mach_group['ProductItem']),
                    'AQL': join_values(mach_group['AQL']),
                    'Shift': '',
                    'Line': '',
                    'max_speed': mach_group['max_speed'].max(),
                    'min_speed': mach_group['min_speed'].min(),
                    'avg_speed': mach_group['avg_speed'].mean(),
                    'sum_qty': mach_group['sum_qty'].sum(),
                }
                subtotal_df = pd.DataFrame([subtotal])
                rows.append(subtotal_df)  # Machine total summary

            # Combine the grouped data into a DataFrame
            df_with_subtotals = pd.concat(rows, ignore_index=True)

        except Exception as e:
            print(e)

        return df_with_subtotals

    def generate_excel(self, writer, df_with_subtotals, plant):

        # Create a bold font style
        bold_font = Font(bold=True)

        # Create a border style with a bold line above
        thick_border = Border(top=Side(style='thick'), bottom=Side(style='thick'))

        df_with_subtotals.to_excel(writer, sheet_name=f'{plant}', index=False)

        # Read the written Excel file
        workbook = writer.book
        worksheet = writer.sheets[f'{plant}']

        # Freeze the first row
        worksheet.freeze_panes = worksheet['A2']

        # Apply Header Style
        for cell in worksheet[1]:  # First line is Header
            cell.font = self.header_font
            cell.alignment = self.header_alignment
            cell.border = self.header_border

        # Set the percentage format
        for cell in worksheet['N'][1:]:  # 'N' is 'Ng_Ratio' Column
            cell.style = self.percent_style

        # Add grouping (click to expand/collapse in Excel)
        current_row = 2
        for i, (_, group) in enumerate(grouped):
            group_size = len(group)
            start_row = current_row
            end_row = start_row + group_size - 1

            # Hide detailed data
            worksheet.row_dimensions.group(start_row, end_row, hidden=True)

            # Move to the next group position
            current_row = end_row + 2  # +2 to account for the subtotal row

        # Formatting
        for col in worksheet.columns:
            max_length = max(len(str(cell.value)) for cell in col)
            col_letter = col[0].column_letter

            if col_letter in ['G', 'H', 'I']:  # 检查是否为指定的列
                worksheet.column_dimensions[col_letter].width = 10  # 如果内容为空，设置为20
            else:
                worksheet.column_dimensions[col_letter].width = max_length + 5

            # Set alignment
            for cell in col:
                if col_letter in ['J', 'K', 'L', 'M', 'N']:  # 针对指定列应用右对齐
                    cell.alignment = self.right_align_style.alignment
                else:
                    cell.alignment = self.center_align_style.alignment

        # Search all lines, bold font and bold line above
        for row in worksheet.iter_rows(min_row=2, max_row=worksheet.max_row):
            if row[6].value == 'Subtotal':  # 假设“Subtotal”在第7列 ('G')
                for cell in row:
                    cell.font = bold_font
                    cell.border = thick_border

        return workbook


from datetime import datetime, timedelta
report_date1 = datetime.today() - timedelta(days=1)
report_date1 = report_date1.strftime('%Y%m%d')

report_date2 = datetime.today()
report_date2 = report_date2.strftime('%Y%m%d')

report = mes_daily_report(report_date1, report_date2)
report.main()