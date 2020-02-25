"""Provides connections to the Postgres database and implements utilities
for loading new rows into the database"""
from copy import deepcopy
import os

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

MATERIALIZED_VIEWS = ['event_aggregates', 'members_view', 'participants']

PG_SCHEMA = os.environ['PG_SCHEMA']
PG_DATABASE = os.environ['PG_DATABASE']
PG_HOST = os.environ['PG_HOST']
PG_USER = os.environ['PG_USER']
PG_PASS = os.environ['PG_PASS']

class Database:
    """Connects to the database and implements utility functions
    for loading new rows into the tables."""
    def __init__(self, database=None, schema=None, user=None):
        # This is used to cache the names of the columns in a database
        # table so we don't need to make multiple calls to PG
        self.columns = {}

        # Database connection and configurations
        self.connection = psycopg2.connect(user=PG_USER,
                                           password=PG_PASS,
                                           host=PG_HOST,
                                           dbname=PG_DATABASE)

    def run_query(self, sql, commit=True):
        """ Runs a query against the postgres database """
        with self.connection.cursor() as cursor:
            cursor.execute(sql)
        if commit:
            self.connection.commit()

    def refresh_view(self, view):
        """ Refreshes a materialized view """
        sql = """REFRESH MATERIALIZED VIEW
                 {}.{}""".format(PG_SCHEMA, view)
        self.run_query(sql)

    def refresh_views(self, test=False):
        """ Refreshes all materialized views """
        for view in MATERIALIZED_VIEWS:
            self.refresh_view(view)

    def backup_table(self, table):
        """ Creates a backup of the specified table """
        sql = """
            DROP TABLE IF EXISTS {schema}.{table}_backup;
            CREATE TABLE {schema}.{table}_backup
            AS SELECT *
            FROM {schema}.{table}
        """.format(schema=PG_SCHEMA, table=table)
        self.run_query(sql)

    def revert_table(self, table):
        """ Reverts a table to the backup """
        sql = """
            DROP TABLE IF EXISTS {schema}.{table};
            CREATE TABLE {schema}.{table}
            AS SELECT *
            FROM {schema}.{table}
        """.format(schema=PG_SCHEMA, table=table)
        self.run_query(sql)

    def truncate_table(self, table):
        """ Truncates a table """
        sql = "TRUNCATE {}.{}".format(PG_SCHEMA, table)
        self.run_query(sql)

    def get_columns(self, table):
        """ Pulls the column names for a table """
        sql = """
            SELECT DISTINCT column_name
            FROM information_schema.columns
            WHERE table_schema='{schema}'
            AND table_name='{table}'
        """.format(schema=PG_SCHEMA, table=table)
        df = pd.read_sql(sql, self.connection)
        columns = [x for x in df['column_name']]
        return columns

    def load_item(self, item, table):
        """ Load items from a dictionary into a Postgres table """
        # Find the columns for the table
        if table not in self.columns:
            self.columns[table] = self.get_columns(table)
        columns = self.columns[table]

        # Determine which columns in the item are valid
        item_ = deepcopy(item)
        for key in item:
            if key not in columns:
                del item_[key]

        # Construct the insert statement
        n = len(item_)
        row = "(" + ', '.join(['%s' for i in range(n)]) + ")"
        cols = "(" + ', '.join([x for x in item_]) + ")"
        sql = """
            INSERT INTO {schema}.{table}
            {cols}
            VALUES
            {row}
        """.format(schema=PG_SCHEMA, table=table, cols=cols, row=row)

        # Insert the data
        values = tuple([item_[x] for x in item_])
        with self.connection.cursor() as cursor:
            cursor.execute(sql, values)
        self.connection.commit()

    def load_items(self, items, table):
        """
        Loads a list of items into the database
        This is faster than running load_item in a loop
        because it reduces the number of server calls
        """
        # Find the columns for the table
        if table not in self.columns:
            self.columns[table] = self.get_columns(table)
        columns = self.columns[table]

        # Determine which columns in the item are valid
        item_ = deepcopy(items[0])
        for key in items[0]:
            if key not in columns:
                del item_[key]

        # Construct the insert statement
        n = len(item_)
        cols = "(" + ', '.join([x for x in item_]) + ")"
        sql = """
            INSERT INTO {schema}.{table}
            {cols}
            VALUES
            %s
        """.format(schema=PG_SCHEMA, table=table, cols=cols)

        # Insert the data
        all_values = []
        for item in items:
            item_ = deepcopy(item)
            for key in item:
                if key not in columns:
                    del item_[key]
            values = tuple([item_[x] for x in item_])
            all_values.append(values)

        with self.connection.cursor() as cursor:
            execute_values(cursor, sql, all_values)
        self.connection.commit()

    def delete_item(self, table, item_id, secondary=None):
        """ Deletes an item from a table """
        sql = "DELETE FROM {schema}.{table} WHERE id='{item_id}'".format(
            schema=PG_SCHEMA,
            table=table,
            item_id=item_id
        )
        if secondary:
            for key in secondary:
                sql += " AND %s='%s'"%(key, secondary[key])
        self.run_query(sql)

    def get_item(self, table, item_id, secondary=None):
        """ Fetches an item from the database """
        sql = "SELECT * FROM {schema}.{table} WHERE id='{item_id}'".format(
            schema=PG_SCHEMA,
            table=table,
            item_id=item_id
        )
        df = pd.read_sql(sql, self.connection)
        if secondary:
            for key in secondary:
                sql += " AND {}='{}'".format(key, secondary[key])

        if len(df) > 0:
            return dict(df.loc[0])
        else:
            return None

    def update_column(self, table, item_id, column, value):
        """ Updates the value of the specified column """
        sql = """
            UPDATE {schema}.{table}
            SET {column} = {value}
            WHERE id = '{item_id}'
        """.format(schema=PG_SCHEMA, table=table, column=column,
                   value=value, item_id=item_id)
        self.run_query(sql)

    def last_event_load_date(self):
        """ Pulls the most recent event start date from the database """
        sql = """
            SELECT max(load_datetime) as max_start
            FROM {schema}.events
            WHERE start_datetime IS NOT NULL
        """.format(schema=PG_SCHEMA)
        df = pd.read_sql(sql, self.connection)

        if len(df) > 0:
            time = df.loc[0]['max_start']
            if time:
                return time.to_pydatetime()
            else:
                return None
