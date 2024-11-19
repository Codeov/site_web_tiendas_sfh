from google.cloud import bigquery as bq
import pandas as pd

class bigQuery:

    def query_return(json,query):
        client=bq.Client.from_service_account_json(json)
        request_venta_cero=client.query(query)
        request_venta_cero.result()
        df_venta_cero=pd.DataFrame(data=request_venta_cero.to_dataframe())
        return df_venta_cero
    


