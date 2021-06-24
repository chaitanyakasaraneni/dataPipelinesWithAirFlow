from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 conn_id   = 'redshift',
                 table     = '',
                 sql_query = '',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.conn_id   = conn_id
        self.table     = table
        self.sql_query = sql_query

    def execute(self, context):
        '''
        Insert data into fact table from staging tables.
        Parameters:
        ----------
        conn_id   (string) : Airflow connection id to redshift cluster
        table     (string) : Target table located in redshift cluster
        sql_query (string) : SQL query to select the insert data
        '''
        redshift = PostgresHook(postgres_conn_id = self.conn_id)
        sql_insert = f'INSERT INTO {self.table} ({str(self.sql_query)})'
        redshift.run(sql_insert)   
