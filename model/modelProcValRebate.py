from fuentes.ora import oracle
from fuentes.bigQuery import bigQuery

from dateutil.relativedelta import relativedelta
import datetime as dt
import pandas as pd
from google.cloud import bigquery
from sys import path

import pandas as pd

#variable
usuario= "SINTERFACE"
contraseña= "SF5590X"
ip_server_name= "10.20.11.20/SPT01"

ruta_file_json= r"E:\bst\credenciales\sistemas-bi-7a46b3894448.json"
#ruta_file_json= r"D:\python\credenciales biq query\sistemas-bi-7a46b3894448.json"

def validar_fechas_pmm(fecha_ini,fecha_fin):
    query_fechas=f"""
        SELECT distinct TO_CHAR(A.RBM_ACU_DATE,'YYYY-MM-DD') fecha_acumulacion
        FROM RBMACDEE A,
            RBMCCLEE C,
            RBMHDREE H,
            TBLFLCEE S,
            VPCMSTEE V,
            VPCALWEE D,
            VPCEVNEE E,
            ORGMSTEE O,
            VPCDELEE P
        WHERE A.RBM_TECH_KEY = H.RBM_TECH_KEY
        AND A.RBM_ACU_TECH_KEY = C.RBM_ACU_TECH_KEY
        AND S.TBL_FIELD = 'RBM_CLAIM_STATUS'
        AND A.RBM_CLAIM_STATUS = TO_NUMBER(S.TBL_CODE)
        AND H.VPC_TECH_KEY = V.VPC_TECH_KEY
        AND H.VPC_DEAL_ID = P.VPC_DEAL_ID
        AND D.RBM_TECH_KEY = H.RBM_TECH_KEY
        AND A.VPC_ALW_KEY = D.VPC_ALW_KEY
        AND D.VPC_EVNT_KEY = E.VPC_EVNT_KEY
        AND O.ORG_LVL_CHILD = C.ORG_LVL_CHILD
        AND A.RBM_ACU_DATE BETWEEN TO_DATE('{fecha_ini}', 'YYYYMMDD') AND
            TO_DATE('{fecha_fin}', 'YYYYMMDD')
        AND TRIM(H.VPC_DEAL_ID) IN ('COM-O', 'COM')
        order by  TO_CHAR(A.RBM_ACU_DATE,'YYYY-MM-DD')
        """
    return query_fechas

def query_pmm(fecha_ini,fecha_fin):
    query=f"""
        select  h.rbm_id rbm_id,
        h.rbm_desc rbm_desc,
        v.vendor_number codigo_proveedor,
        v.vendor_name descripcion_proveedor,
        e.vpc_evnt_type vpc_evnt_type,
        o.org_lvl_number codigo_tienda,
        o.org_name_full descripcion_tienda,
        sum(c.rbm_calc_claim) acumulado,
        s.tbl_code_name estado
        ,P.VPC_DEAL_DESC tipo_rebate
        ,h.vpc_deal_id vpc_deal_id
    from rbmacdee a,
        RBMCCLEE c,
        rbmhdree h,
        tblflcee s,
        vpcmstee v,
        vpcalwee d,
        vpcevnee e,
        orgmstee o
        ,VPCDELEE P
    where a.rbm_tech_key = h.rbm_tech_key
    and a.rbm_acu_tech_key = c.rbm_acu_tech_key
    and s.tbl_field = 'RBM_CLAIM_STATUS'
    and a.rbm_claim_status = to_number(s.tbl_code)
    and h.vpc_tech_key = v.vpc_tech_key
    and h.vpc_deal_id=P.VPC_DEAL_ID
    and d.rbm_tech_key = h.rbm_tech_key
    and a.vpc_alw_key = d.vpc_alw_key
    and d.vpc_evnt_key = e.vpc_evnt_key
    and o.org_lvl_child = c.org_lvl_child
    and a.rbm_acu_date  BETWEEN TO_DATE('{fecha_ini}','YYYYMMDD') AND TO_DATE('{fecha_fin}','YYYYMMDD')
    and trim(h.vpc_deal_id) IN ('COM-O','COM','LOG','LOG-O')
    group by h.rbm_id,
            h.rbm_desc,
            v.vendor_number,
            e.vpc_evnt_type,
            v.vendor_name,
            o.org_lvl_number,
            o.org_name_full,
            s.tbl_code_name
            , P.VPC_DEAL_DESC,h.vpc_deal_id
    order by h.rbm_id
    """
    return query

def consultar_rebate_pmm(obj_ora,query,id_bloque):
    ls=obj_ora.ejecutar_query(query)
    df=pd.DataFrame(data=ls,columns=["RBM_ID", "RBM_DESC", "CODIGO_PROVEEDOR", "DESCRIPCION_PROVEEDOR", "VPC_EVNT_TYPE", "CODIGO_TIENDA", "DESCRIPCION_TIENDA", "ACUMULADO", "ESTADO", "TIPO_REBATE","VPC_DEAL_ID"])
    df['ID_BLOQUE']=id_bloque
    df['RBM_ID']=df['RBM_ID'].map(str)
    df['RBM_DESC']=df['RBM_DESC'].map(str)
    df['CODIGO_PROVEEDOR']=df['CODIGO_PROVEEDOR'].map(str)
    df['DESCRIPCION_PROVEEDOR']=df['DESCRIPCION_PROVEEDOR'].map(str)
    df['VPC_EVNT_TYPE']=df['VPC_EVNT_TYPE'].map(str)
    df['CODIGO_TIENDA']=df['CODIGO_TIENDA'].map(str)
    df['DESCRIPCION_TIENDA']=df['DESCRIPCION_TIENDA'].map(str)
    df['ACUMULADO']=df['ACUMULADO'].map(str)
    df['ESTADO']=df['ESTADO'].map(str)
    df['TIPO_REBATE']=df['TIPO_REBATE'].map(str)
    df['VPC_DEAL_ID']=df['VPC_DEAL_ID'].map(str)
    return df

def conectar_bq(file_json_sbi):
    lg_bq=bigquery.Client.from_service_account_json(file_json_sbi)
    return lg_bq

def clear_table_bq(tabla,project,data_set,file_json_sbi):
    lg_bq=conectar_bq(file_json_sbi)
    query=f"""
    truncate table `{project}.{data_set}.{tabla}`
    """
    request_bq=lg_bq.query(query)
    request_bq.result()

def ejecutar_query_bq(query,file_json_sbi):
    lg_bq=conectar_bq(file_json_sbi)
    request_bq=lg_bq.query(query)
    request_bq.result()


def cargar_table_bq(tabla,project,data_set,ruta_file_json,df):
    obj_bq=conectar_bq(ruta_file_json)
    print("cargando tmp...")
    resp_ins_tg=obj_bq.load_table_from_dataframe(df,f"""{project}.{data_set}.{tabla}""")
    resp_ins_tg.result()
    print("tmp procesado...")

def cargar_pmm_to_bq(tp,obj_ora,ruta_file_json):

    flag_one=0
    tp=tp
    ls=[]
    for i in tp:
        fecha_i_v="".join(i)
        ls.append(fecha_i_v)
    dff=pd.DataFrame(data=[],columns=["ID_BLOQUE","RBM_ID", "RBM_DESC", "CODIGO_PROVEEDOR", "DESCRIPCION_PROVEEDOR", "VPC_EVNT_TYPE", "CODIGO_TIENDA", "DESCRIPCION_TIENDA", "ACUMULADO", "ESTADO", "TIPO_REBATE","VPC_DEAL_ID"])
    
    for i in range(0,len(ls)):
        if flag_one==0:
            fecha_i_v=ls[i][0:8]+"01"
            fecha_f_v=ls[i]
            flag_one=1
        else:
            fecha_i_v=dt.datetime.strftime(((dt.datetime.strptime(ls[(i-1)],"%Y-%m-%d"))+dt.timedelta(days=1)),"%Y-%m-%d")
            fecha_f_v=ls[i]
        
        fecha_i_v=dt.datetime.strftime((dt.datetime.strptime(fecha_i_v,"%Y-%m-%d")),"%Y%m%d")
        fecha_f_v=dt.datetime.strftime((dt.datetime.strptime(fecha_f_v,"%Y-%m-%d")),"%Y%m%d")

        id_bloque_v=str(i+1)+"_"+fecha_i_v+"_"+fecha_f_v

        print(id_bloque_v+" pmm")
        dfv=consultar_rebate_pmm(obj_ora,query_pmm(fecha_i_v,fecha_f_v),id_bloque_v)
        dff=pd.concat([dff,dfv])
    obj_ora.cerrar
    if dff.empty !=True:
        clear_table_bq('tmp_rebate_acum_pmm','sistemas-bi','SPSA',ruta_file_json)
        cargar_table_bq('tmp_rebate_acum_pmm','sistemas-bi','SPSA',ruta_file_json,dff)

def consultar_cupo_op (start_month,end_month,id_bloque_v):
  path.append(r'E:\bst\sales_validate\dll\130')
  from pyadomd import Pyadomd

  connection_string=r'Data Source=irprdf0017;Initial Catalog=SPSA_Cubo_Rebate;Provider=MSOLAP.5;Integrated Security=SSPI;Impersonation Level=Impersonate;'
  

  con=Pyadomd(connection_string)
  con.open()

  fecha_format_i=dt.datetime.strftime(start_month,"%d/%m/%Y")
  fecha_format_f=dt.datetime.strftime(end_month,"%d/%m/%Y")

  day_i=start_month.weekday()
  day_f=end_month.weekday()

  if day_i==0:
    day_i="Lunes"
  elif day_i==1 :
    day_i="Martes"
  elif day_i==2 :
    day_i="Miércoles"
  elif day_i==3 :
    day_i="Jueves"
  elif day_i==4 :
    day_i="Viernes"
  elif day_i==5 :
    day_i="Sábado"
  elif day_i==6 :
    day_i="Domingo"
  
  if day_f==0:
    day_f="Lunes"
  elif day_f==1 :
    day_f="Martes"
  elif day_f==2 :
    day_f="Miércoles"
  elif day_f==3 :
    day_f="Jueves"
  elif day_f==4 :
    day_f="Viernes"
  elif day_f==5 :
    day_f="Sábado"
  elif day_f==6 :
    day_f="Domingo"
  
  fecha_request_i=(str(day_i)+" "+str(fecha_format_i))
  fecha_request_f=(str(day_f)+" "+str(fecha_format_f))

  dax_query= """
      select 
              {
          [Measures].[Aporte GIC],
          [Measures].[Aporte GOC],
          [Measures].[Aporte Logistico],
          [Measures].[Aporte No Devolucion],
          [Measures].[Aporte Promocional],
          [Measures].[Aporte Rebate]
              } on columns,
              non empty
              {
              [Local].[Codigo SAP].[Codigo SAP]*
      [Proveedor].[Codigo Proveedor].[Codigo Proveedor]
              } on rows
              from [SPSA_Cubo_Rebate]
            where (
              [Tiempo].[Calendario Mensual].[Dia].&["""+f"""{fecha_request_i}"""+"""]
              :
              [Tiempo].[Calendario Mensual].[Dia].&["""+f"""{fecha_request_f}"""+"""]
              )
      """
  result=con.cursor().execute(dax_query)
  df=pd.DataFrame(result.fetchone(),columns=["codigo_sap","codigo_proveedor","aporte_gic","aporte_goc","aporte_logistico","aporte_no_devolucion","aporte_promocional","aporte_rebate"])
  df['id_bloque']=str(id_bloque_v)
  df['codigo_sap'] = df["codigo_sap"].map(str)
  df['codigo_proveedor'] = df["codigo_proveedor"].map(str)
  df['aporte_gic'] = df["aporte_gic"].map(str)
  df['aporte_goc'] = df["aporte_goc"].map(str)
  df['aporte_logistico'] = df["aporte_logistico"].map(str)
  df['aporte_no_devolucion'] = df["aporte_no_devolucion"].map(str)
  df['aporte_promocional'] = df["aporte_promocional"].map(str)
  df['aporte_rebate'] = df["aporte_rebate"].map(str)
  con.close()
  return df
  
  
def cargar_cupo_to_bq (tp):
  tp=tp
  ls=[]
  for i in tp:
      fecha_i_v="".join(i)
      ls.append(fecha_i_v)
  dff=pd.DataFrame(data=[],columns=["id_bloque","codigo_sap","codigo_proveedor","aporte_gic","aporte_goc","aporte_logistico","aporte_no_devolucion","aporte_promocional","aporte_rebate"])
  
  flag_one=0
  for i in range(0,len(ls)):
      if flag_one==0:
            fecha_i_v=ls[i][0:8]+"01"
            fecha_f_v=ls[i]
            flag_one=1
      else:
          fecha_i_v=dt.datetime.strftime(((dt.datetime.strptime(ls[(i-1)],"%Y-%m-%d"))+dt.timedelta(days=1)),"%Y-%m-%d")
          fecha_f_v=ls[i]
        
      fecha_i_v=dt.datetime.strptime((dt.datetime.strftime((dt.datetime.strptime(fecha_i_v,"%Y-%m-%d")),'%d/%m/%Y')),'%d/%m/%Y')
      fecha_f_v=dt.datetime.strptime((dt.datetime.strftime((dt.datetime.strptime(fecha_f_v,"%Y-%m-%d")),'%d/%m/%Y')),'%d/%m/%Y')
      id_bloque_v=str(i+1)+"_"+dt.datetime.strftime(fecha_i_v,"%Y%m%d")+"_"+dt.datetime.strftime(fecha_f_v,"%Y%m%d")
      print(id_bloque_v+" bi")
      dfv=consultar_cupo_op (fecha_i_v,fecha_f_v,id_bloque_v)
      dff=pd.concat([dff,dfv])
  if dff.empty != True:
    clear_table_bq('tmp_rebate_acum_bi','sistemas-bi','SPSA',ruta_file_json)
    cargar_table_bq('tmp_rebate_acum_bi','sistemas-bi','SPSA',ruta_file_json,dff)

def procesar_val_reb_pmm_bi(año_mes):
  año_mes=año_mes
  fecha_ini=año_mes+'-01'
  fecha_ini_f= dt.datetime.strptime(fecha_ini,'%Y-%m-%d')

  carry, new_month=divmod(fecha_ini_f.month-1+1, 12)
  new_month+=1
  fecha_fin_f=fecha_ini_f.replace(year=fecha_ini_f.year+carry, month=new_month)+dt.timedelta(days=-1)

  obj_ora=oracle(usuario,contraseña,ip_server_name)
  obj_ora.inicializar_oracle()
  obj_ora.conectar_oracle()
  fecha_ini_fp=dt.datetime.strftime(fecha_ini_f,'%Y%m%d')
  fecha_fin_fp=dt.datetime.strftime(fecha_fin_f,'%Y%m%d')

  tp=obj_ora.ejecutar_query(validar_fechas_pmm(fecha_ini_fp,fecha_fin_fp))
  if tp:
      cargar_pmm_to_bq(tp,obj_ora,ruta_file_json)
      cargar_cupo_to_bq (tp)
      print("cargando sp...")
      query="""
      call `sistemas-bi.SPSA.val_rebate_pmm_bi` ()
      """
      print("sp procesado...")

      ejecutar_query_bq(query,ruta_file_json)
      obj_ora.cerrar()
      return 1
  else:
      print('No hay Data')
      obj_ora.cerrar()
      return 0

class modelProcValRebate:
    @classmethod
    def procesar_val_reb(self, periodo):
        return procesar_val_reb_pmm_bi(periodo)
    
    @classmethod
    def actualizar_cierre_validacion(self,periodo):
      acv_bq=conectar_bq(ruta_file_json)
      q_acv=f"""
        update `sistemas-bi.SPSA.fact_val_reb_pmm_bi` a
        set a.id_cierre=1
        where a.fecha ='{periodo}'
      """
      request_acv_bq=acv_bq.query(q_acv)
      request_acv_bq.result()

    @classmethod
    def validar_data_rebate_acumulado(self,periodo):
          vdra=conectar_bq(ruta_file_json)
          q_vdra=f"""
          select distinct id_cierre
          from `sistemas-bi.SPSA.fact_val_reb_pmm_bi` a
          where a.fecha ='{periodo}'
"""
          request_vd=vdra.query(q_vdra)
          result=request_vd.result()
          total_filas=result.total_rows
          if total_filas==0:
                return False
          else:
                return True
    @classmethod
    def validar_upd_rebate_acumulado(self,periodo):
          vura=conectar_bq(ruta_file_json)
          qura=f"""
          select distinct ifnull(id_cierre,0) id_cierre
          from `sistemas-bi.SPSA.fact_val_reb_pmm_bi` a
          where a.fecha ='{periodo}'
          limit 1
"""
          request_vura=vura.query(qura)
          result=request_vura.result()
          total_filas=result.total_rows
          if total_filas==0:
                id_cierre=0
          else:
            for row in result:
                  id_cierre=row[0]
          return id_cierre
            



