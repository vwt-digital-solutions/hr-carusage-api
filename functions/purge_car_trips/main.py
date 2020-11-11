import logging

from datetime import datetime, timedelta, timezone
from google.cloud import firestore

logging.basicConfig(level=logging.INFO)


class FirestoreProcessor(object):
    def __init__(self, collection, delta):
        self.collection = collection
        self.db_client = firestore.Client()

        today = datetime.now(timezone.utc)
        date_delta = today - timedelta(weeks=delta)  # Get timedelta week
        date_week_start = date_delta - timedelta(days=date_delta.weekday())  # Get start of week
        date_week_start = date_week_start + timedelta(days=7)  # Get end of week

        self.start_date = datetime(today.year, today.month, today.day)
        self.start_date = datetime(date_week_start.year, date_week_start.month, date_week_start.day)
        self.end_date = datetime(date_week_start.year, date_week_start.month, date_week_start.day)

    def delete_entities(self):
        batch_limit = 500
        batch_has_new_entities = True
        batch_last_reference = None

        count_entities = 0

        while batch_has_new_entities:
            query = self.db_client.collection(self.collection)

            query = query.where("ended_at", ">=", self.start_date)
            query = query.where("ended_at", "<", self.end_date)
            query = query.where("exported.exported_at", "<", self.today)
            query = query.order_by("ended_at", "ASCENDING")
            query = query.limit(batch_limit)

            if batch_last_reference:
                query = query.start_after(batch_last_reference)

            docs = query.stream()

            if docs:
                batch = self.db_client.batch()  # Creating new batch
                docs_list = list(docs)

                if len(docs_list) < batch_limit:
                    batch_has_new_entities = False
                else:
                    batch_last_reference = docs_list[-1]

                for doc in docs_list:
                    batch.delete(doc.reference)  # Delete entity
                    count_entities += 1

                batch.commit()  # Committing changes within batch
            else:
                batch_has_new_entities = False

        logging.info(f"Purged total of {count_entities} entities from collection '{self.collection}'")


def purge_entities(request):
    for key in ['collection', 'timedelta']:
        if key not in request.args:
            raise ValueError(f"Request is missing the essential configuration key '{key}'")

    try:
        FirestoreProcessor(request.args['collection'], request.args['timedelta']).delete_entities()
    except Exception as e:
        logging.error('An error occurred: {}'.format(e))
        return 400


if __name__ == '__main__':
    class R:
        def __init__(self):
            self.args = {'collection': 'Trips', 'timedelta': 3}
    r = R()
    purge_entities(r)
