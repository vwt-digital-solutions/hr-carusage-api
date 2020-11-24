import logging
import json
import base64

from stgprocessor import process


def topic_to_storage(request):
    # Extract data from request
    envelope = json.loads(request.data.decode('utf-8'))
    payload = base64.b64decode(envelope['message']['data'])

    # Extract subscription from subscription string
    try:
        subscription = envelope['subscription'].split('/')[-1]
        logging.info(f'Message received from {subscription} [{payload}]')

        process(json.loads(payload))

    except Exception as e:
        logging.info('Extract of subscription failed')
        logging.debug(e)
        raise e

    # Returning any 2xx status indicates successful receipt of the message.
    # 204: no content, delivery successfull, no further actions needed
    return 'OK', 204


# Test code for local testing (won't be executed when deployed as Cloud Function)
if __name__ == '__main__':
    payload = json.load(open('payload.json'))
    try:
        process(payload)
    except Exception:
        logging.exception('Extract of subscription failed')
