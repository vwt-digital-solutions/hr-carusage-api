import logging
import os
import connexion

from openapi_server import encoder
from Flask_AuditLog import AuditLog
from Flask_No_Cache import CacheControl
from flask_sslify import SSLify


def main():
    app = connexion.App(__name__, specification_dir='./openapi/')
    app.app.json_encoder = encoder.JSONEncoder
    app.add_api('openapi.yaml',
                arguments={'title': 'Expenses API'},
                pythonic_params=True)
    app.run(port=8080)

    logging.basicConfig(level=logging.INFO)

    AuditLog(app)
    CacheControl(app)
    if 'GAE_INSTANCE' in os.environ:
        SSLify(app, permanent=True)


if __name__ == '__main__':
    main()
