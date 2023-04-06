import pyodbc

class AzureSQLConnector:
    def __init__(self, servername, databasename, username, password):
        self.servername = servername
        self.databasename = databasename
        self.username = username
        self.password = password
        self.connection = None
        self.cursor = None

    def connect(self):
        try:
            self.connection = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                                              f'Server={self.servername}.database.windows.net;'
                                              f'Database={self.databasename};'
                                              f'Uid={self.username};'
                                              f'Pwd={self.password};'
                                              'Encrypt=yes;'
                                              'TrustServerCertificate=no;'
                                              'Connection Timeout=30;')
            self.cursor = self.connection.cursor()
            print("Connection successful")
        except pyodbc.Error as e:
            print(f"Error connecting to Azure SQL: {e}")

    def close(self):
        if self.cursor is not None:
            self.cursor.close()
        if self.connection is not None:
            self.connection.close()

    def execute_proc(self, query):
        try:
            self.cursor.execute(query)
            return None
        except pyodbc.Error as e:
            print(f"Error executing query: {e}")
            return None
        
    def executemany_ddl(self, query, params):
        try:
            self.cursor.executemany(query, params)
            return None
        except pyodbc.Error as e:
            print(f"Error executing query: {e}")
            return None
    
    def commit(self):
        self.connection.commit()
