import os
import ast
import csv
import smtplib
import logging
import ConfigParser
from logging.config import dictConfig
from email.mime.multipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email.mime.text import MIMEText
from email import Encoders


class Utility(object):
    """
    Class that performs utility actions like sending an email, settings,
    API stuff etc
    """

    def __init__(self):
        dictConfig({
            "version": 1,
            "formatters": {
                "f": {"format": "%(asctime)s %(name)-12s %(levelname)-8s %(message)s"}
            },
            "handlers": {
                "h": {"class": "logging.StreamHandler", "formatter": "f", "level": logging.DEBUG}
            },
            "root": {
             "handlers": ["h"],
             "level": logging.INFO,
            }
        })

        self.logger = logging.getLogger()
        self._config_parser = ConfigParser.ConfigParser()
        self._config_parser.read(
            os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "settings.ini"))
        )

    def send_email(self, filename, group):
        try:
            message = MIMEMultipart("alternative")
            message["From"] = "Copia Report Automation"
            toaddr = ast.literal_eval(self._config_parser.get(group, "recipients"))
            message["Subject"] = self._config_parser.get(group, "subject")
            message.attach(
                MIMEText("Dear Team,\nAttached is today's %s report." % self._config_parser.get(group, "subject"))
            )
            attachment = MIMEBase("application", "octet-stream")
            attachment.set_payload(file(filename).read())
            Encoders.encode_base64(attachment)

            attachment.add_header(
                "Content-Disposition",
                "attachment",
                filename=filename
            )

            message.attach(attachment)
            password = self._config_parser.get("email", "password")
            server = smtplib.SMTP(
                self._config_parser.get("email", "server"),
                int(self._config_parser.get("email", "port"))
            )

            server.ehlo()
            server.starttls()
            server.login(self._config_parser.get("email", "username"), password)
            server.sendmail(self._config_parser.get("email", "username"), toaddr, message.as_string())
            server.close()
        except smtplib.SMTPException:
            raise

    def make_format_csv(self, result, filename, headers):
        """
        Turn the received list
        """

        result_file = csv.writer(open(filename, 'wb'))
        result.insert(0, headers)
        for row in result:
            result_file.writerow(row)
