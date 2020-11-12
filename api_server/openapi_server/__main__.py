import logging
import os

from . import get_app
from Flask_AuditLog import AuditLog
from Flask_No_Cache import CacheControl
from flask_sslify import SSLify


def main():
    app = get_app()
    app.run(port=8080)

    logging.basicConfig(level=logging.INFO)

    AuditLog(app)
    CacheControl(app)
    if 'GAE_INSTANCE' in os.environ:
        SSLify(app, permanent=True)


if __name__ == '__main__':
    main()
