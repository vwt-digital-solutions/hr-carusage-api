import config
import logging
import operator
import base64
import google.auth
import googleapiclient.discovery

from functools import reduce
from datetime import datetime, timedelta, timezone
from google.cloud import firestore
from apiclient import errors
from google.auth import iam
from google.auth.transport import requests
from google.oauth2 import service_account
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

logging.basicConfig(level=logging.INFO)
TOKEN_URI = 'https://accounts.google.com/o/oauth2/token'  # nosec


def get_from_dict(data_dict, map_list):
    """Returns a dictionary based on a mapping"""
    return reduce(operator.getitem, map_list, data_dict)


class MailProcessor(object):
    def __init__(self):
        credentials, project_id = google.auth.default(scopes=['https://www.googleapis.com/auth/iam'])
        delegated_credentials = self.get_delegated_credentials(credentials)
        self.mail_service = googleapiclient.discovery.build(
            'gmail', 'v1', credentials=delegated_credentials, cache_discovery=False)

    @staticmethod
    def get_delegated_credentials(credentials):
        try:
            request = requests.Request()
            credentials.refresh(request)

            signer = iam.Signer(request, credentials, config.GMAIL_SERVICE_ACCOUNT)
            creds = service_account.Credentials(
                signer=signer,
                service_account_email=config.GMAIL_SERVICE_ACCOUNT,
                token_uri=TOKEN_URI,
                scopes=config.GMAIL_SCOPES,
                subject=config.GMAIL_SUBJECT_ADDRESS)
        except Exception:
            raise

        return creds

    @staticmethod
    def generate_mail(mail_addresses):
        msg = MIMEMultipart('alternative')
        msg['From'] = config.GMAIL_REPLYTO_ADDRESS
        msg['Subject'] = 'Wekelijkse beoordeling ritten'
        msg['To'] = mail_addresses[0]

        if len(mail_addresses) > 1:
            msg['Bcc'] = ','.join(mail_addresses[1:])

        msg.attach(MIMEText(open('mail_template.html', 'r').read(), 'html'))
        raw = base64.urlsafe_b64encode(msg.as_bytes())
        raw = raw.decode()

        return {'raw': raw}

    def send_mails(self, mail_addresses):
        try:
            mail_body = self.generate_mail(mail_addresses)
            message = (self.mail_service.users().messages().send(userId="me", body=mail_body).execute())
            logging.info(f"Email '{message['id']}' has been sent to {len(mail_addresses)} recipients")
        except errors.HttpError as e:
            logging.error('An exception occurred when sending an email: {}'.format(e))


class FirestoreProcessor(object):
    def __init__(self):
        self.db_client = firestore.Client()

        today = datetime.now(timezone.utc)
        last_week_start = today - timedelta(days=today.weekday() + 7)
        last_week_end = last_week_start + timedelta(days=7)

        self.start_date = datetime(last_week_start.year, last_week_start.month, last_week_start.day)
        self.end_date = datetime(last_week_end.year, last_week_end.month, last_week_end.day)

    def get_email_addresses(self):
        batch_limit = 500
        batch_has_new_entities = True
        batch_last_reference = None

        email_addresses = []

        while batch_has_new_entities:
            query = self.db_client.collection(config.DB_COLLECTION)

            query = query.where("ended_at", ">=", self.start_date)
            query = query.where("ended_at", "<", self.end_date)
            query = query.where("outside_time_window", "==", True)
            query = query.order_by("ended_at", "ASCENDING")
            query = query.limit(batch_limit)

            if batch_last_reference:
                query = query.start_after(batch_last_reference)

            docs = query.stream()

            if docs:
                docs_list = list(docs)

                if len(docs_list) < batch_limit:
                    batch_has_new_entities = False
                else:
                    batch_last_reference = docs_list[-1]

                for doc in docs_list:
                    try:
                        email_address = get_from_dict(doc.to_dict(), ['department', 'manager_mail'])
                    except KeyError:
                        pass
                    else:
                        email_addresses.append(email_address)
            else:
                batch_has_new_entities = False

        return list(set(email_addresses))  # Removing double values


def mail_to_managers(request):
    for key in ['DB_COLLECTION', 'GMAIL_ACTIVE', 'GMAIL_SERVICE_ACCOUNT', 'GMAIL_SUBJECT_ADDRESS',
                'GMAIL_REPLYTO_ADDRESS', 'GMAIL_SCOPES']:
        if not hasattr(config, key):
            raise ValueError(f"Function is missing the essential configuration key '{key}'")

    if not config.GMAIL_ACTIVE:
        logging.info('Gmail functionality is disabled, finishing execution')
        return 'OK', 200

    try:
        email_addresses = FirestoreProcessor().get_email_addresses()

        if len(email_addresses) > 0:
            logging.info(f"Found {len(email_addresses)} email addresses")
            MailProcessor().send_mails(email_addresses)
        else:
            logging.info("No email addresses have been found, email sending will be skipped")
    except Exception as e:
        logging.error('An error occurred: {}'.format(e))
        return 'Bad Request', 400


if __name__ == '__main__':
    mail_to_managers(None)
