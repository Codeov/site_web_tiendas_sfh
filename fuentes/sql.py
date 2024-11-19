import pyodbc as odbc

class SQLServer:
    autocommit= True

    def __init__(self,SERVER,DATABASE,UID,PWD):
        self.SERVER=SERVER
        self.DATABASE=DATABASE
        self.UID=UID
        self.PWD=PWD

    def connect_to_sql_server(self):
        try:
            self.conn= odbc.connect(self._connection_string())
            print('conexion exitosa al server 18 - sql')
            return self.conn

        except Exception as e:
            print(e)
            return None
    
    def _connection_string(self):

        SERVER= self.SERVER
        DATABASE=self.DATABASE
        UID=self.UID
        PWD=self.PWD
        autocommit= self.autocommit

        conn_string = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER='+SERVER+';DATABASE='+DATABASE+';UID='+UID+';PWD='+PWD+';autocommit='+str(autocommit)+''
        return conn_string
    
    def query(self,sql_statement):
        if self.conn is None:
            print("conexion fallida")
            return
        try:
            self.cursor=self.conn.cursor()
            self.cursor.execute(sql_statement)
            return True

        except Exception as e:
            print(e)

    def query_return(self,sql_statement):
        if self.conn is None:
            print("conexion fallida")
            return
        try:
            self.cursor=self.conn.cursor()
            self.lst=self.cursor.execute(sql_statement).fetchall()
            self.columnNames=[column[0] for column in self.cursor.description]
            self.insertObject = []
            
            for record in self.lst:
                self.insertObject.append(dict(zip(self.columnNames, record)))
            return self.insertObject

        except Exception as e:
            print(e)

    def sp_return(self,sql_statement):
        if self.conn is None:
            print("conexion fallida")
            return
        try:
            self.cursor=self.conn.cursor()
            self.lst=self.cursor.execute(sql_statement).commit()
            print(self.lst)
            self.columnNames=[column[0] for column in self.cursor.description]
            self.insertObject = []
            
            for record in self.lst:
                self.insertObject.append(dict(zip(self.columnNames, record)))
            return self.insertObject

        except Exception as e:
            print(e)
    
    def query_insert_tupla(self,sql_statement,tupla):
        if self.conn is None:
            print("conexion fallida")
            return
        try:
            cursor=self.conn.cursor()
            cursor.fast_executemany=True
            cursor.executemany(sql_statement,tupla)

        except Exception as e:
            print(e)

    def commit(self):
        if self.conn is None:
            print("no hay conexion")
            return
        try:
            self.conn.commit()
  
        except Exception as e:
            print(e)
    
    def cerrar(self):
        if self.conn is None:
            print("no hay conexion")
            return
        try:
            self.conn.close()
  
        except Exception as e:
            print(e)