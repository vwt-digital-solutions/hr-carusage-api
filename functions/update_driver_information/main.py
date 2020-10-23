import logging
import json
import base64
from driverprocessor import DriverProcessor

parser = DriverProcessor()

logging.basicConfig(level=logging.INFO)


# First json to postgis, then postgis to database
def update_drivers(request):
    # Extract data from request
    envelope = json.loads(request.data.decode('utf-8'))
    # log request
    logging.info(f"envelope is: {json.dumps(envelope)}")
    payload = base64.b64decode(envelope['message']['data'])

    # Extract subscription from subscription string
    try:
        subscription = envelope['subscription'].split('/')[-1]
        logging.info(f'Message received from {subscription} [{payload}]')

        parser.process(json.loads(payload))

    except Exception as e:
        logging.info('Extract of subscription failed')
        logging.debug(e)
        raise e

    # Returning any 2xx status indicates successful receipt of the message.
    # 204: no content, delivery successfull, no further actions needed
    return 'OK', 204


if __name__ == '__main__':
    logging.info("Hallo")
