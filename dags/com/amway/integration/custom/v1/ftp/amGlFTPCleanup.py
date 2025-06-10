from typing import Any
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.ftp.hooks.ftp import FTPHook
from com.amway.integration.custom.v1.AmGlCommonV1 import logInfo, logDebug, logError, filter_files, rename_files
from com.amway.integration.custom.v1.pvf.amGlTranLog import logToPVF
import os, shutil, ftplib, json
from ftplib import FTP
from datetime import timedelta, datetime
    
class AmGlFTPCleanup(BaseOperator):
    
    template_fields = ('conn_id' , 'regex', 'remote_path', 'instance_id', 'local_path')

    def __init__(
        self,
        *,
        conn_id,
        regex = '*',
        remote_path,
        instance_id,        
        local_path,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.regex = regex
        self.remote_path=remote_path
        self.instance_id = instance_id  
        self.local_path=local_path                    

    def execute(self, context: Any) -> str:
        error = None
        status = 'FAILED'
        try:
            is_deleted = False
            logDebug(f'Operator : called module with {self.conn_id}, {self.regex} and {self.remote_path}')
            ftp_connection = FTPHook.get_connection(conn_id=self.conn_id)
            ftp = FTP()
            extra = {}
            if ftp_connection.get_extra() is not None and ftp_connection.get_extra() != '':
                extra = json.loads(ftp_connection.get_extra())
            else:
                extra['passive'] = 'true'
            logDebug(f'Operator : FTP Server Connection Mode {extra}')
            if 'passive' in extra:
                passive = extra['passive']
            else:
                passive = 'true'
            ftp.set_pasv(bool(passive))
            ftp.connect(host=ftp_connection.host, port=ftp_connection.port)
            ftp.login(ftp_connection.login, ftp_connection.get_password())
            ftp.cwd(f'{self.remote_path}')
            logDebug(f'Operator : FTP Server Connection successful')
            list_of_files = os.listdir(self.local_path)
            logInfo(f'Operator : list_of_files : {list_of_files}')
            for file in list_of_files:
                try:
                    ftp.delete(self.remote_path+str(file))
                    is_deleted = True
                except ftplib.error_perm:
                    logError(f'File - {self.remote_path}{file} not found.')
                    pass
            if is_deleted:
                ftp.quit()
            logDebug(f'Operator: {list_of_files} removed from '+ self.remote_path)
            logToPVF(os.environ.get('AIRFLOW_CTX_DAG_RUN_ID'), #key
                     os.environ.get('AIRFLOW_CTX_DAG_RUN_ID'), #transactionEventID
                     os.environ.get('AIRFLOW_CTX_DAG_ID'), #transactionSourceObject
                     os.environ.get('AIRFLOW_CTX_DAG_ID'), #dag_id
                     self.instance_id, #dag_run_id or instance id,
                     '', #sourceApp
                     'ACTIVITY',
                     self.conn_id,#targetApplicationCode
                     f'Operator: AmGlFTPCleanup - {list_of_files} removed from '+ self.remote_path #ActivityMessage
                     )
            status = 'SUCCESS'
        except Exception as e:
            logError(f'Operator : exception while cleanup {e}')
            error = e
            logToPVF(os.environ.get('AIRFLOW_CTX_DAG_RUN_ID'), #key
                     os.environ.get('AIRFLOW_CTX_DAG_RUN_ID'), #transactionEventID
                     os.environ.get('AIRFLOW_CTX_DAG_ID'), #transactionSourceObject
                     os.environ.get('AIRFLOW_CTX_DAG_ID'), #dag_id
                     self.instance_id, #dag_run_id or instance id,
                     '', #sourceApp
                     'ERROR',
                     self.conn_id,#targetApplicationCode
                     f'Operator: Failure in AmGlFTPCleanup, reason {e}' #ActivityMessage
                     )
            raise AirflowException(f"exception while cleanup , error: {e}")
        return status, error
