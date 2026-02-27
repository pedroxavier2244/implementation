import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from notifier.strategies.base import NotificationStrategy


class EmailSMTPStrategy(NotificationStrategy):
    def __init__(self, host: str, port: int, user: str, password: str, recipient: str = ""):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.recipient = recipient or user

    def send(self, event_type: str, severity: str, message: str, metadata: dict) -> None:
        email = MIMEMultipart("alternative")
        email["Subject"] = f"[{severity}] ETL Alert - {event_type}"
        email["From"] = self.user
        email["To"] = self.recipient

        body = f"<h2>{severity}: {event_type}</h2><p>{message}</p>"
        if metadata:
            rows = "".join(f"<tr><td><b>{k}</b></td><td>{v}</td></tr>" for k, v in metadata.items())
            body += f"<table>{rows}</table>"

        email.attach(MIMEText(body, "html"))

        with smtplib.SMTP(self.host, self.port) as smtp:
            smtp.starttls()
            smtp.login(self.user, self.password)
            smtp.sendmail(self.user, self.recipient, email.as_string())
