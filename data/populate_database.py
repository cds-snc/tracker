import click
import csv
import os
import pymongo.errors
from data import logger
from data import models

LOGGER = logger.get_logger(__name__)

# This method populates the domain_input database collection with all domains found within domains.csv
def populate(ctx: click.core.Context):
    first_row = True
    with models.Connection(ctx.obj.get("connection_string")) as connection:
        # Drop the collection ahead of domain insertions
        connection.domain_input.drop_collection()
        # Path to domains.csv
        domain_path = str(os.getcwd()) + '/csv/domains.csv'
        with open(domain_path, 'r') as file:
            # For each domain in domains.csv, create a corresponding document within the domain_input collection
            curReader = csv.reader(file, delimiter=',')
            for curRow in curReader:
                row_dict = {'domain': curRow[0], 'organization_en': curRow[2], 'organization_fr': curRow[3]}
                if len(list(connection.domain_input.find(row_dict))) is 0 and not first_row:
                    # Attempt to create the document
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
