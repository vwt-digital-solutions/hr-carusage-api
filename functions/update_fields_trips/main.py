import add_fields_firestore_entities
import logging

logging.basicConfig(level=logging.INFO)


def fields_to_trips(request):
    add_fields_to_firestore_entities_obj = add_fields_firestore_entities.AddFieldsToFirestoreEntities()
    # Add fields to firestore collection
    add_fields_to_firestore_entities_obj.add_fields_to_collection()


if __name__ == '__main__':
    fields_to_trips(None)
