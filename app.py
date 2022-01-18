"""
This script transfers data from S3 to PostgreSQL table
by using AWS Data Wrangler
"""

import os
import awswrangler as wr
import pg8000


def s3_to_df():
    """
    Read csv file from S3
    """
    # get envirtonment variables related to S3
    s3_uri = os.environ.get("S3_URI")
    # read csv file from s3
    data_df = wr.s3.read_csv(s3_uri)
    return data_df


def df_to_sql(data_df):
    """
    Transfer data from DataFrame to PostgreSQL table
    """
    # get envirtonment variables related to PostgreSQL
    sql_host = os.environ.get("SQL_HOST")
    sql_user = os.environ.get("SQL_USER")
    sql_password = os.environ.get("SQL_PASSWORD")
    sql_db = os.environ.get("SQL_DB")
    sql_table = os.environ.get("SQL_TABLE")
    # connect to Postgresql database
    con = pg8000.connect(
        user=sql_user,
        host=sql_host,
        database=sql_db,
        port=5432,
        password=sql_password
    )
    # transfer data from DataFrame to PostgreSQL table
    wr.postgresql.to_sql(
        df=data_df,
        table=sql_table,
        schema="public",
        con=con
    )


def main():
    """
    Main function which transfers the data from S3 to PostgreSQL table
    """
    data_df = s3_to_df()
    df_to_sql(data_df)


if __name__=="__main__":
    main()
