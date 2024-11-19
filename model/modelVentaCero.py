from fuentes.sql import SQLServer
from fuentes.bigQuery import bigQuery

import pandas as pd

#variable
ip = '10.20.1.5'
bd= 'Retail_DW'
us= 'operador'
pw= 'operador'

class modelVentaCero:
    @classmethod
    def filtro_formato(self):
        sql_op=SQLServer(ip,bd,us,pw)
        sql_op.connect_to_sql_server()
        query_filtro_formato="""
        select distinct a.id,a.nombre formato from dim_formato a
        order by 1 desc
        """
        ls=sql_op.query_return(query_filtro_formato)
        sql_op.cerrar()
        return ls
    
    @classmethod
    def filtro_local(self,formato):
        sql_op=SQLServer(ip,bd,us,pw)
        sql_op.connect_to_sql_server()
        query_filtro_local=f"""
        select distinct a.codigo_sap+'-'+a.nombre local 
        from dim_local a
        inner join dim_formato b
        on a.formato=b.nombre
        where
        estado = 'A' and Tipo_Local = 'L'
		and b.nombre='{formato}'
        """
        print(query_filtro_local)
        ls= sql_op.query_return(query_filtro_local)#df.values.tolist() 
        sql_op.cerrar()
        return ls

    @classmethod
    def filtro_area(self):
        sql_op=SQLServer(ip,bd,us,pw)
        sql_op.connect_to_sql_server()
        query_filtro_area=f"""
        select distinct Nombre_Area nombre_area
        from dbo.Dim_Jerarquia_PMM
        where Nombre_Area in ('BAZAR','HOGAR','TEXTIL')
        order by  1 asc
        """
        ls= sql_op.query_return(query_filtro_area)
        sql_op.cerrar()
        return ls
    
    @classmethod
    def select_venta_cero(self,local,area):
      ruta_json_sbi= r'E:\bst\credenciales\sistemas-bi-7a46b3894448.json'
      #ruta_json_sbi= r'D:\python\credenciales biq query\sistemas-bi-7a46b3894448.json'
      query=f"""
        select 
        a.nombre_linea,a.nombre_familia,a.codigo_producto,a.ean,a.nombre_producto nombre_producto,cast(cast(a.Stock_Unidad as integer) as string) Stock,cast(cast(a.Stock_Valorizado as integer) as string) Valorizado
        from `sistemas-bi.SPSA_Tiendas.vc_consolidado` a
        where a.local = '{local}'
        and a.nombre_area= '{area}'
        order by 1,2,5 asc;
        """
      bq=bigQuery.query_return(ruta_json_sbi,query)
      insertObject = []
      ls=[]
      if bq.empty!=True:
          ls=bq.to_dict(orient='records')
          return ls,True
      else:
          return ls,False
      
    @classmethod
    def select_venta_cero_group(self,local,area):
      ruta_json_sbi= r'E:\bst\credenciales\sistemas-bi-7a46b3894448.json'
      #ruta_json_sbi= r'D:\python\credenciales biq query\sistemas-bi-7a46b3894448.json'
      query=f"""
         select  CONCAT('Stock_Valorizado: S/.',FORMAT("%'.2f",cast(cast(sum(round(a.Stock_Valorizado,2)) as int64) as float64))) stock_valorizado,
       concat('Stock_Unidad: ',cast(cast(sum(round(a.Stock_Unidad,2)) as int64) as string)," Und.") Stock_Unidad
        from `sistemas-bi.SPSA_Tiendas.vc_consolidado` a 
        where a.local = '{local}'
        and a.nombre_area= '{area}'
        ;
        """
      bq=bigQuery.query_return(ruta_json_sbi,query)
      insertObject = []
      ls=[]
      if bq.empty!=True:
          ls=bq.to_dict(orient='records')
          return ls,True
      else:
          return ls,False
       
       

