import os
import smtplib
from husfort.qutility import SFG
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.header import Header
from loguru import logger


class CAttachmentText(object):
    def __init__(self, attachment_file: str, attachment_dir: str, alias: str = ""):
        self.attachment_file = attachment_file
        self.attachment_dir = attachment_dir
        self.attachment_path = os.path.join(attachment_dir, attachment_file)
        self.attachment_alias = alias

    def to_mime_app(self):
        attachment_app = MIMEApplication(open(self.attachment_path, "rb").read())
        if self.attachment_alias:
            attachment_app.add_header("Content-Disposition", "attachment", filename=self.attachment_alias)
        else:
            attachment_app.add_header("Content-Disposition", "attachment", filename=self.attachment_file)
        return attachment_app


class CAgentEmail(object):
    def __init__(self, mail_host: str, mail_port: int, mail_sender: str, mail_sender_pwd: str):
        self.mail_host = mail_host
        self.mail_port = mail_port
        self.mail_sender = mail_sender
        self.mail_sender_pwd = mail_sender_pwd
        self.message = MIMEMultipart()

    def reinit(self):
        self.message = MIMEMultipart()
        return 0

    def __write_text(self, msg_body: str):
        self.message.attach(MIMEText(msg_body))
        return 0

    def __add_attachments(self, attachments: list[CAttachmentText]):
        for attachment in attachments:
            self.message.attach(attachment.to_mime_app())
        return 0

    def write(self, receivers: list[str], msg_subject: str, msg_body: str, attachments: list[CAttachmentText]):
        self.message["From"] = f"<{self.mail_sender}>"
        self.message["To"] = ",".join(receivers)
        self.message["Subject"] = Header(msg_subject, "utf-8")
        self.__write_text(msg_body=msg_body)
        self.__add_attachments(attachments=attachments)
        return 0

    def send(self):
        try:
            # smtp_app = smtplib.SMTP_SSL(self.mail_host, self.mail_port)
            smtp_app = smtplib.SMTP(self.mail_host, self.mail_port)
            logger.info(f"{SFG('connected')}")

            smtp_app.login(self.mail_sender, self.mail_sender_pwd)
            logger.info(f"{SFG('logged in')}")

            smtp_app.sendmail(self.mail_sender, self.message["To"].split(","), self.message.as_string())
            logger.info(f"{SFG('email sent')}")
            logger.info(
                f"email with subject = [{SFG(str(self.message['Subject']))}] has been sent to:{SFG(self.message['To'])}\n"
            )
        except smtplib.SMTPException as e:
            logger.error("fails when sending email")
            logger.error(e)
        return 0
