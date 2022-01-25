import uuid
from pyspark.sql.functions import udf

                

class pythonFunctions:
    @udf
    def generate_uuid():
        return str(uuid.uuid4().hex)
    