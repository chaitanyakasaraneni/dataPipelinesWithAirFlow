from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    
    ui_color = '#358140'

    template_fields = ('s3_key',)
    
    copy_sql = """
        COPY {table}
        FROM '{s3_path}'
        ACCESS_KEY_ID '{access_key}'
        SECRET_ACCESS_KEY '{secret_key}'
        FORMAT AS {file_format}
        REGION '{region}'
        TIMEFORMAT AS 'epochmillisecs'
    """    
    
    @apply_defaults
    def __init__(self,
                 conn_id     = 'redshift',
                 table       = '',
                 s3_bucket   = 'udacity-dend',
                 s3_key      = '',
                 region      = 'us-west-2',
                 file_format = '',
                 aws_cred_id = 'aws_credentials',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.conn_id     = conn_id
        self.table       = table
        self.s3_bucket   = s3_bucket
        self.s3_key      = s3_key
        self.region      = region
        self.file_format = file_format
        self.aws_cred_id = aws_cred_id


    def execute(self, context):
        '''
        Copy data into the staging table from S3.
        Parameters:
        ----------
        conn_id     (string) : Airflow connection id to redshift cluster
        table       (string) : Target table located in redshift cluster
        s3_bucket   (string) : Bucket name on S3
        s3_key      (string) : Key in S3 busket
        region      (string) : S3 region, default 'us-west-2'
        file_format (string) : Data file format, e.g. "JSON 'auto'"
        aws_cred_id (string) : Airflow connection id to AWS, for S3 access
        '''        
        aws_credentials = AwsHook(self.aws_cred_id).get_credentials()
        redshift = PostgresHook(postgres_conn_id = self.conn_id)
            
        self.log.info(f'Clear table {self.table}')
        redshift.run(f'DELETE FROM {self.table}')
        
        #s3_key format: 'log-data/{execution_date.year}/{execution_date.month:02d}'
        s3_path = f's3://{self.s3_bucket}/{self.s3_key.format(**context)}'
        self.log.info(f'Copy from {s3_path} to Redshift')
            
        query = self.copy_sql.format(
            table       = self.table,
            s3_path     = s3_path,
            access_key  = aws_credentials.access_key,
            secret_key  = aws_credentials.secret_key,
            file_format = self.file_format,
            region      = self.region
        )
        redshift.run(query)