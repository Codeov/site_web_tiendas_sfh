import oracledb

directory_co=r"E:\bst\validador_prorrateo\bc\instantclient_11_2"
#directory_co=r"D:\instaladores\biblioteca cliente oracle\instantclient_11_2"

class oracle :
    def __init__(self,usuario,contraseña,dsn):
        self.usuario=usuario
        self.contraseña= contraseña
        self.dsn=dsn

    def inicializar_oracle(self):
        oracledb.init_oracle_client(directory_co)
        return True
 
    def conectar_oracle(self):
        try:
            self.cnxo=oracledb.connect(user=self.usuario,password=self.contraseña,dsn=self.dsn)
            print("conexion exitosa to oracle")
            return self.cnxo
        except Exception as e:
            print(e)
            return None
    def ejecutar_query(self,query):
        try:
            cursor=self.cnxo.cursor()
            cursor.execute(query)
            self.data=cursor.fetchall()
            return(self.data)
        except Exception as e:
            print(e)
            return None
        
    def ejecutar_query_cab(self,query):
        try:
            cursor=self.cnxo.cursor()
            cursor.execute(query)
            self.cab = [col[0] for col in cursor.description]
            return(self.cab)
        except Exception as e:
            print(e)
            return None
        
    def cerrar(self):
        try:
            self.close= self.cnxo.close()
            print("conexion to oracle cerrada")
            return self.close
        except Exception as e:
            print(e)
            return None
    
    def query_insert_tupla(self,sql_statement,tupla):
        if self.cnxo is None:
            print("conexion fallida")
            return
        try:
            cursor=self.cnxo.cursor()
            #cursor.fast_executemany=True
            cursor.executemany(sql_statement,tupla,
                   batcherrors=True)
            
        except Exception as e:
            #for error in cursor.getbatcherrors():
             #print("Error", error.message, "at row offset", error.offset)
            print(e)

    def commit(self):
        if self.cnxo is None:
            print("no hay conexion")
            return
        try:
            self.cnxo.commit()
  
        except Exception as e:
            raise Exception ("error de commit sql, detalle :" + str(e))

