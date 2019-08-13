import click
import csv
from data import models

def populate(ctx: click.core.Context):
    with models.Connection(ctx.obj.get("connection_string")) as connection:
        connection.input_domains.clear()
        file = open('data/domains.csv', 'r')
        curReader = csv.reader(file, delimiter=',')
        for curRow in curReader:
            row_dict = {0: curRow[0]}
            connection.domains_input.create(row_dict)