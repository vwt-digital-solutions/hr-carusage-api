import mark_firestore_entities
import logging

logging.basicConfig(level=logging.INFO)


def mark_trips(request):
    mark_firestore_entities_obj = mark_firestore_entities.MarkFirestoreEntities()
    # Mark trips outside time window
    mark_firestore_entities_obj.mark_collection()


if __name__ == '__main__':
    mark_trips(None)
