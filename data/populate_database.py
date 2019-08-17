import click
import csv
import os
import pymongo.errors
from data import logger
from data import models

LOGGER = logger.get_logger(__name__)

def populate(ctx: click.core.Context):
    first_row = True
    with models.Connection(ctx.obj.get("connection_string")) as connection:
        for doc in connection.input_domains.all():
            connection.input_domains.delete_one(doc)
        domain_path = str(os.getcwd()) + '/csv/domains.csv'
        with open(domain_path, 'r') as file:
            curReader = csv.reader(file, delimiter=',')
            for curRow in curReader:
                row_dict = {'domain': curRow[0], 'organization_en': curRow[2], 'organization_fr': curRow[3]}
                if len(list(connection.domain_input.find(row_dict))) is 0 and not first_row:
                    try:
                        connection.domain_input.create(row_dict)
                    except pymongo.errors.DocumentTooLarge:
                        LOGGER.exception("An error was encountered while inserting domains into the input database. "
                                         "Document exceeds PyMongo maximum document size.")
                    except pymongo.errors.WriteConcernError as exc:
                        LOGGER.exception("An error was encountered while inserting domains into the input database"
                                         " (Write Concern Error). Exception details: %s", str(exc.details))
                    except pymongo.errors.WriteError as exc:
                        LOGGER.exception("An error was encountered while inserting domains into the input database"
                                         " (Write Error). Exception details: %s", str(exc.details))
                    except pymongo.errors.OperationFailure as exc:
                        LOGGER.exception("An error was encountered while inserting domains into the input database"
                                         " (Operation Failure). Exception details: %s", str(exc.details))
                    except pymongo.errors.PyMongoError:
                        LOGGER.exception("An error was encountered while inserting domains into the input database"
                                         " (PyMongoError).")
                    except Exception as exc:
                        LOGGER.exception(
                            "An unknown error was encountered while inserting domains into the input database."
                            " Exception details: %s", str(exc))
                first_row = False
        connection.close()
