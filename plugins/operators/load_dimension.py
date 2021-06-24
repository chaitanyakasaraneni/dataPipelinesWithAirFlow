from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 conn_id   = 'redshift',
                 table     = '',
                 sql_query = '',
                 truncate  = True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.table = table
        self.sql_query = sql_query
        self.truncate = truncate

    def execute(self, context):
        '''
        Insert data into dimensional tables from staging tables.
        Parameters:
        ----------
        conn_id   (string) : Airflow connection to redshift cluster
        table     (string) : Target dimension table
        sql_query (string) : SQL query to select the insert data
        truncate    (bool) : Truncate table before insert
        '''
        redshift = PostgresHook(postgres_conn_id = self.conn_id)

        if self.truncate:
            self.log.info(f'Truncating table {self.table}')
            redshift.run(f'TRUNCATE TABLE {self.table}')
        
        sql_insert = f'INSERT INTO {self.table} ({str(self.sql_query)})'
        redshift.run(sql_insert)        