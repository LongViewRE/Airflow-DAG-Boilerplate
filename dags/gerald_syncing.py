import json
from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import PythonVirtualenvOperator

@dag(schedule_interval=None, start_date=datetime(2021, 12, 16), catchup=False, tags=['gerald'])
def gerald_syncing():
    """
    This DAG handles syncing data to Gerald.
    """
    
    @task.virtualenv(
        use_dill=False,
        system_site_packages=True,
        requirements=['thefuzz[speedup]', 'python-dotenv', 
                        'git+ssh://git@github.com/LongViewRE/LV_db_connection',
                        'git+ssh://git@github.com/LongViewRE/LV_external_services',
                        'git+ssh://git@github.com/LongViewRE/LV_general_functions'],
    )
    def pull_from_RPS():
        # extremely hacky way to add packages to the python path, should be changed in future
        import sys
        sys.path.insert(0, "/home/geraldadmin/airflow/dags")

        import logging

        from gerald_syncing.PullFromRPS.utils import contact_comparison
        from gerald_syncing.PullFromRPS.sync import sync_test
        
        from LV_db_connection import GremlinClient
        from LV_external_services import RPSClient

        logging.info("created DAG")
        try:
            g = GremlinClient()
        except Exception as e:
            logging.error("Exception gremlin", exc_info=True)

        try: 
            r = RPSClient()
        except Exception as e:
            logging.error("Exception rps", exc_info=True)
        
        try:
            contact_comparison()
        except Exception as e:
            logging.error("Exception contact", exc_info=True)


    pull_from_RPS()

gerald_syncing = gerald_syncing()