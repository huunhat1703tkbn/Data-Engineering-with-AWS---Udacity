from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',
                 tables = [],
                 checks = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        self.checks = checks

    def execute(self, context):
        
        redshift = PostgresHook(self.redshift_conn_id )
        for table in self.tables:
            records = redshift.get_records("SELECT COUNT(*) FROM {}".format(table)
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
            self.log.info(f"Data quality on table {table} check passed with {records[0][0]} records")
        errores = 0
        for chek in self.checks:
            check_query = check.get('check_sql')
            result = check.get('expected_result')
            try:
                records = redshift_hook.get_records(check_query)[0][0]
            except:
                self.log.info("Error")
            if records != result:
            errores = errores + 1
        if errores > 0:
            raise ValueError("Errors in tables")
