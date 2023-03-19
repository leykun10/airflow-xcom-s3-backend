from typing import Any
from airflow.models.xcom import BaseXCom
import os
import pandas as pd
import uuid

class S3XComBackend(BaseXCom):
    PREFIX = "xcom_s3://"
    BUCKET_NAME = os.getenv('BUCKET_NAME')
    AWS_ACCESS_KEY_ID=os.getenv('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY=os.getenv('AWS_SECRET_ACCESS_KEY')
    CLEAR_AT_COMPLETION = (os.getenv('CLEAR_AT_COMPLETION','False')=='True')
    @staticmethod
    def serialize_value(value: Any):
        if isinstance(value, pd.DataFrame):
            
            key = "data_" + str(uuid.uuid4())
            value.to_csv(
                f"s3://{S3XComBackend.BUCKET_NAME}/{key}",
                index=False,
                storage_options={
                    "key": S3XComBackend.AWS_ACCESS_KEY_ID,
                    "secret": S3XComBackend.AWS_SECRET_ACCESS_KEY,
                },
            )
            value = S3XComBackend.PREFIX + key
        return BaseXCom.serialize_value(value)



    @staticmethod
    def deserialize_value(result) -> Any:
        result = BaseXCom.deserialize_value(result)
        if isinstance(result, str) and result.startswith(S3XComBackend.PREFIX):
            key     = result.replace(S3XComBackend.PREFIX, "")
            result = pd.read_csv(f"s3://{S3XComBackend.BUCKET_NAME}/{key}",
                    storage_options={
                        "key": S3XComBackend.AWS_ACCESS_KEY_ID,
                        "secret": S3XComBackend.AWS_SECRET_ACCESS_KEY,
                    },)
            # todo: check if CLEAR_AT_COMPLETION is true or not and delete the object after task completion
            if S3XComBackend.CLEAR_AT_COMPLETION:
                fs = s3fs.S3FileSystem(key=S3XComBackend.AWS_ACCESS_KEY_ID,secret=S3XComBackend.AWS_SECRET_ACCESS_KEY)
                fs.rm_file(key)

             
        return result

