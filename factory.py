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
import logging
import time
from abc import ABC, abstractmethod


class Factory(ABC):
    def __init__(self):
        self.attachment_list = []  # Excel 附件清單

    @abstractmethod
    def generate_main_df(self, sql_query, connection_string):
        pass

    @abstractmethod
    def fix_main_df(self, df):
        pass

    @abstractmethod
    def sorting_data(self, df):
        pass

    @abstractmethod
    def validate_data(self, df):
        pass

    @abstractmethod
    def generate_excel(self, df, filename):
        pass

    @abstractmethod
    def generate_chart(self, df, filename):
        pass

    @abstractmethod
    def send_email(self, config, subject, file_list, image_buffers, error_msg, normal_msg):
        logging.info(f"Start to send Email")

        # SMTP Sever config setting
        smtp_server = config.smtp_config.get('smtp_server')
        smtp_port = int(config.smtp_config.get('smtp_port', 587))
        smtp_user = config.smtp_config.get('smtp_user')
        smtp_password = config.smtp_config.get('smtp_password')
        sender_alias = f"{config.location} Report"
        sender_email = smtp_user
        # Mail Info
        msg = MIMEMultipart()
        msg['From'] = f"{sender_alias} <{sender_email}>"
        msg['To'] = ', '.join(config.to_emails)
        msg['Subject'] = subject

        if len(error_msg) > 0:
            msg['Subject'] = msg['Subject'] + ' 資料異常, 取消派送'

        # Mail Content
        html = f"""\
                <html>
                  <body>
                  {normal_msg}
                  {error_msg}
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
        try:
            # server = smtplib.SMTP(smtp_server, smtp_port)
            # server.starttls()  # 启用 TLS 加密
            # server.login(smtp_user, smtp_password)  # 登录到 SMTP 服务器
            # server.sendmail(smtp_user, to_emails, msg.as_string())
            # server.quit()

            # 發送郵件（不進行密碼驗證）
            server = smtplib.SMTP(smtp_server, smtp_port)
            server.ehlo()  # 啟動與伺服器的對話
            if len(error_msg) > 0:
                server.sendmail(smtp_user, config.admin_emails, msg.as_string())
            else:
                server.sendmail(smtp_user, config.to_emails, msg.as_string())
            print("Sent Email Successfully")
        except Exception as e:
            print(f"Sent Email Fail: {e}")
            logging.info(f"Sent Email Fail: {e}")
        finally:
            attachment.close()
