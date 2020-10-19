from google.cloud import firestore
import datetime
import logging
from pytz import timezone
import config

logging.basicConfig(level=logging.INFO)


class MarkFirestoreEntities(object):
    def __init__(self):
        self.db = firestore.Client()
        self.collection = self.db.collection(config.collection)
        self.entities = self.get_entities()

    def get_entities(self):
        entities = []
        # Query for documents
        for doc in self.collection.stream():
            # dict of entity:
            doc_dict = doc.to_dict()
            doc_dict['id'] = doc.id
            entities.append(doc_dict)
        return entities

    def update_firestore_entity(self, entity_id, field, value):
        doc_ref = self.collection.document(entity_id)
        doc_ref.update({field: value})
        # doc_ref.update({field: firestore.DELETE_FIELD})

    def mark_collection(self):
        for entity in self.entities:
            # Mark entity if its start date are before a certain time window
            # The mark is "outside_time_window"
            self.mark_entity_outside_time_window(entity, "outside_time_window")

    def mark_entity_outside_time_window(self, entity, field):
        hour_before = config.time_window['start_time_hour']
        minutes_before = config.time_window['start_time_minutes']
        hour_after = config.time_window['end_time_hour']
        minutes_after = config.time_window['end_time_minutes']
        # time window is in Europe/Amsterdam time
        time_before = datetime.time(hour_before, minutes_before, tzinfo=timezone('Europe/Amsterdam'))
        time_after = datetime.time(hour_after, minutes_after, tzinfo=timezone('Europe/Amsterdam'))
        # Convert started_at and ended_at of trip to Europe/Amsterdam timezone
        started_at = entity['started_at']
        started_at = started_at.astimezone(timezone('Europe/Amsterdam')).time()
        ended_at = entity['ended_at']
        ended_at = ended_at.astimezone(timezone('Europe/Amsterdam')).time()
        if started_at < time_before or started_at > time_after:
            # Entity started before begin time of time window or after end time of time window
            self.update_firestore_entity(entity['id'], field, True)
