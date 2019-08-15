import click
import csv
import os
from data import models

def populate(ctx: click.core.Context):
    with models.Connection(ctx.obj.get("connection_string")) as connection:
        connection.input_domains.clear()
        domain_path = str(os.getcwd()) + '/csv/domains.csv'
        with open(domain_path, 'r') as file:
            curReader = csv.reader(file, delimiter=',')
            for curRow in curReader:
                row_dict = {'domain': curRow[0]}
                connection.domain_input.create(row_dict)