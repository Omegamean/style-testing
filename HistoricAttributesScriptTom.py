# -*- coding: utf-8 -*-
"""
Created on Tue Aug 20 11:10:31 2019

@author: Conor Glasman
"""

import psycopg2
import datetime
from dateutil.relativedelta import relativedelta

# Defining massive fuck off strings is far better to be done outside a function, as then they don't need to be
# initialised every time the function is called.
from sql_functions import SQL_TABLE_INSERT, SQL_TABLE_UPDATE, SQL_LOG_UPDATE


# Two blank lines between new functions (and imports) if outside a class
# No spaces between function name and parentheses
# Also, use snake_case for functions and CamelCase for classes
def redshift_insert(attribute_date):
    # kwargs arranged like so
    redshift = psycopg2.connect(
        database='*',  # Personal preference, but single quotes look better
        user='*',  # Also, no whitespace either side of equals sign when declaring kwargs
        password='*',
        host='*',
        port='*'
    )
    cur = redshift.cursor()

    # attribute_date is formatted twice here, you can jsut define it twice.
    cur.execute('TRUNCATE TABLE di_staging.di_wrk_user_attributes_old')
    cur.execute(SQL_TABLE_INSERT.format(attribute_insert_date=attribute_date.strftime('%Y-%m-%d')))
    cur.execute(SQL_TABLE_UPDATE)
    cur.execute(SQL_LOG_UPDATE.format(attribute_valid_date=attribute_date.strftime('%Y-%m-%d')))

    redshift.commit()
    cur.close()  # I presume these are functions that need to be executed
    redshift.close()


def redshift_log_between_dates(start_date, end_date):
    while start_date < end_date:
        redshift_insert(start_date)
        start_date = start_date + relativedelta(months=1)


if __name__ == '__main__':
    # This is generally accepted practice when running a script
    attribute_start_date = datetime.datetime(2014, 1, 3)  # Spaces between args here.
    attribute_end_date = datetime.datetime(2014, 2, 4)
    redshift_log_between_dates(attribute_start_date, attribute_end_date)
    # Don't forget a blank line at the end of the file:)
