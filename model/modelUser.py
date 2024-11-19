from .entidad.user import User
from fuentes.sql import SQLServer

ip = '10.20.1.5'
bd= 'Retail_DW'
us= 'operador'
pw= 'operador'

class modelUser:
    @classmethod
    def login(self,user_form):
        try:
            _sql=SQLServer(ip,bd,us,pw)
            sql=f"""
            select a.id,a.usuario,ltrim(rtrim(a.contraseña)),a.nombre from bi_user_bst a
            where a.usuario='{user_form.username}'
            """
            _sql.connect_to_sql_server()
            dict_user=_sql.query_return(sql)
            _sql.cerrar()
            if dict_user :
                ls_user=list((dict(dict_user[0])).values())
                id=ls_user[0]
                usuario=ls_user[1]
                contraseña=ls_user[2]
                nombre=ls_user[3]
                user=User(id,usuario,User.check_password(contraseña,user_form.password),nombre)
                return user
            else :
                return None
        except Exception as ex:
            raise Exception(ex)
    @classmethod
    def get_by_id(self,id):
        try:
            _sql=SQLServer(ip,bd,us,pw)
            sql=f"""
                 select a.id,a.usuario,a.nombre from bi_user_bst a
            where a.id='{id}'
                """
            _sql.connect_to_sql_server()
            dict_user=_sql.query_return(sql)
            _sql.cerrar()
            if dict_user :
                ls_user=list((dict(dict_user[0])).values())
                id=ls_user[0]
                usuario=ls_user[1]
                nombre=ls_user[2]
                user=User(id,usuario,None,nombre)
                return user
            else :
                return None
            
        except Exception as ex:
            raise Exception(ex)
    @classmethod
    def val_roles(self,id,name_area,name_endpoint):
        try:
            _sql=SQLServer(ip,bd,us,pw)
            sql=f"""
                 select count(1) flag_rol
                    from fact_roles_bst a
                    inner join bi_user_bst b
                    on a.id_user=b.id
                    inner join bi_area_bst c
                    on a.id_area=c.id
                    inner join bi_endpoint_bst d
                    on a.id_endpoint=d.id
                    where a.id_user= {id}
                    and c.nombre= '{name_area}'
                    and d.nombre='{name_endpoint}'
                """
            print(sql)
            _sql.connect_to_sql_server()
            fr=_sql.query_return(sql)
            fr=list((dict(fr[0])).values())
            flag_rol=int(str(fr[0]))
            if flag_rol>0:
                return True
            else:
                return False
        
        except Exception as ex:
            raise Exception(ex)




