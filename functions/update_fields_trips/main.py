import logging
from add_fields_firestore_entities import AddFieldsToFirestoreEntities

logging.basicConfig(level=logging.INFO)


def fields_to_trips(request):
    AddFieldsToFirestoreEntities().add_fields_to_collection()


if __name__ == '__main__':
    fields_to_trips(None)
