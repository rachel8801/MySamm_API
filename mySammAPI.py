from email.message import EmailMessage
import ssl
import smtplib
from product_data_cust import update_data_cust_table
from site_price_history import update_price_history_table

# For sending the email error alert
port = 465   # For SSL
smtp_server = "smtp.gmail.com"
sender_email = "bi.nourison@gmail.com"
receiver_email = ["rachel.lin@nourison.com", "jordan.peykar@nourison.com"]
# This is the gmail app pwd, not the login pwd
email_password = 'fake_one'
subject = 'This is an error message from MySamm API extract'


def send_email(message):
    em = EmailMessage()
    em['From'] = sender_email
    em['To'] = receiver_email
    em['subject'] = subject
    em.set_content(message)
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL('smtp.gmail.com', port, context=context) as smtp:
        smtp.login(sender_email, email_password)
        smtp.sendmail(sender_email, receiver_email, em.as_string())

try:
    update_data_cust_table()
    update_price_history_table()
except Exception as e:
    error = str(e)
    if len(error) > 0:
        send_email(error)
    else:
        pass
