#!/usr/bin/env python
import snowflake.connector

def test_connection():
    # Gets the version
    ctx = snowflake.connector.connect(
        user='lucasrosario',
        password='L48m48r*11',
        account='xfmrxfj-rj96218'
        )
    cs = ctx.cursor()
    try:
        cs.execute("SELECT current_version()")
        one_row = cs.fetchone()
        print(one_row[0])
    finally:
        cs.close()
    ctx.close()