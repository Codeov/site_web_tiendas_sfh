from fuentes.sql import SQLServer
import pandas as pd

#variable
ip = '10.20.1.5'
bd= 'Retail_DW'
us= 'operador'
pw= 'operador'

class modelPollos:
    @classmethod
    def listar_plan_horneado(self,semana,local):
        sql_op=SQLServer(ip,bd,us,pw)
        sql_op.connect_to_sql_server()
        query_plan_horneado_semana_local_clear=f"""
        select a.Fecha_Inicio FechaInicio,a.Fecha_Fin FechaFin, a.Codigo_Sap CodLocal, a.Local, cast(a.turno as varchar(55)) Turno, a.actividad Actividad,
        a.horario Horario,
        a.lunes Lunes,a.martes Martes,a.miercoles Miercoles,a.jueves Jueves,a.viernes Viernes,a.sabado Sabado,a.domingo Domingo
        from pollo_rst_bi a 
        where a.Codigo_SAP=rtrim(ltrim(substring('{local}',1,charindex('-','{local}',0)-1))) and
        replace(ltrim(rtrim(a.first_day_week)),'_','-') in
        (select cast(min(fecha) as date) fecha
                    from Dim_Tiempo 
                    where replace(CONCAT(Semana,'-',Descripcion_Semana),'/','') = '{semana}')
        """
        ls=sql_op.query_return(query_plan_horneado_semana_local_clear)
        sql_op.cerrar()
        return ls

    @classmethod
    def filtro_semana(self):
        sql_op=SQLServer(ip,bd,us,pw)
        sql_op.connect_to_sql_server()
        query_filtro_semana="""
        SELECT distinct 
        CONCAT(Semana,'-',Descripcion_Semana) semana
        FROM DIM_TIEMPO 
        WHERE fecha between 
        (
        select min(fecha)
        from Dim_Tiempo
        WHERE Semana_Unica in (select distinct semana_unica-1 from Dim_Tiempo WHERE fecha = cast(GETDATE() as date))
        )
        and
        (
        select max(fecha)
        from Dim_Tiempo
        WHERE Semana_Unica in (select distinct semana_unica +1 from Dim_Tiempo WHERE fecha = cast(GETDATE() as date)) 
        ) 
        order by semana
        """
        ls=sql_op.query_return(query_filtro_semana)
        sql_op.cerrar()
        return ls
    @classmethod
    def filtro_local(self):
        sql_op=SQLServer(ip,bd,us,pw)
        sql_op.connect_to_sql_server()
        query_filtro_local="""
        SELECT Codigo_SAP, Codigo_SAP + ' - ' + Nombre AS Nombre_Local
        from Dim_Local
        where Formato in ('PLAZA VEA','PLAZA VEA SUPER','PLAZA VEA EXPRESS','VIVANDA')
        and estado = 'A' and Tipo_Local = 'L' and codigo_sap  not in ('P-1')
        or Codigo_SAP in ('Y448')
        ORDER BY 1
        """
        ls=sql_op.query_return(query_filtro_local)
        sql_op.cerrar()
        return ls
    

  

