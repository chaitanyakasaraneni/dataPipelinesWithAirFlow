from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 conn_id = 'redshift',
                 queries = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.queries = queries

    def execute(self, context):
        '''
        Perform data quality checks by running a list of queries.
        Parameters:
        ----------
        conn_id (string) : Airflow connection to redshift cluster
        queries   (list) : List of check queries, specified as {'sql':'SELECT COUNT(*) FROM time WHERE hour < 0', 'expect':0}           
        '''
        redshift = PostgresHook(postgres_conn_id = self.conn_id)
            
        for query in self.queries:
            
            sql = query.get('sql')
            if sql is None: 
                self.log.error('Data quality check: no SQL expression specified.')
                break

            expect = query.get('expect')
            if expect is None:
                expect = 0
             
            count = redshift.get_first(sql)[0] #https://stackoverflow.com/a/59420411
            if (count != expect):
                self.log.error(f'Check failed: {sql} returns {count}, expected: {expect}')
            else:
                self.log.info(f'Check passed: {sql} returns {count}.')
