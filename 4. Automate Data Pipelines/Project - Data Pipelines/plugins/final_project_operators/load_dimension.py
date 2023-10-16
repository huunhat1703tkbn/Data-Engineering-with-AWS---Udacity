from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',
                 table = '',
                 query = '',
                 truncate = False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.query = query
        self.truncate = truncate

    def execute(self, context):
        
        redshift = PostgresHook(self.redshift_conn_id )
        if self.truncate:
            redshift.run(f"\nTRUNCATE TABLE {self.table}")
        final_query = f"\nINSERT INTO {self.table} ({self.query})"
        redshift.run(final_query)
        self.log.info("Success")