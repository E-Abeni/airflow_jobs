from sqlalchemy import create_engine, text
import pandas as pd


class Database_Connector:
    def __init__(self):
        self.DATABASE_URL = "postgresql+psycopg2://postgres:admin@localhost:5432/postgres"
        self.connection, self.engine = self.get_database_connection(self.DATABASE_URL)


    def get_database_connection(self, db_url: str):
        engine = create_engine(db_url)
        connection = engine.connect()
        return connection, engine

    def get_engine(self):
        return self.engine
    

    def get_table_data(self, connection=None, table_name: str = None, limit: int = 10000):
        if connection is None:
            connection = self.connection
        if table_name is None:
            raise ValueError("table_name must be provided")
        
        query = text(f"SELECT * FROM {table_name} LIMIT {limit}")
        result = connection.execute(query)
        data = result.fetchall()
        return data


    def get_table_columns(self, connection=None, table_name: str = None):
        if connection is None:
            connection = self.connection
        if table_name is None:
            raise ValueError("table_name must be provided")
        
        query = text(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'")
        result = connection.execute(query)
        columns = [row[0] for row in result.fetchall()]
        return columns


    def get_table_data_selected(self, connection=None, table_name: str = None, selected_columns: list = None, limit: int = 10000):
        if connection is None:
            connection = self.connection
        if selected_columns is None:
            selected_columns = ['*']
        if table_name is None:
            raise ValueError("table_name must be provided")
        
        query = text(f"SELECT {', '.join(selected_columns)} FROM {table_name} LIMIT {limit}")
        result = connection.execute(query)
        data = result.fetchall()
        return data

    def get_filtered_data(self, connection=None, table_name: str = None, filter_conditions: dict = None, selected_columns: list = None, limit: int = 10000):
        if connection is None:
            connection = self.connection
        if table_name is None:
            raise ValueError("table_name must be provided")
        if selected_columns is None:
            selected_columns = ['*']
        if filter_conditions is None:
            filter_conditions = "1=1"
        
        fiter_text = " AND ".join([f"{key} = '{value}'" for key, value in filter_conditions.items()])
        
        query = text(f"SELECT {', '.join(selected_columns)} FROM {table_name} WHERE {fiter_text} LIMIT {limit}")
        result = connection.execute(query)
        data = result.fetchall()
        return data

    def data_to_pandas_df(self, data, columns):
        df = pd.DataFrame(data, columns=columns)
        df = df.convert_dtypes()
        return df


if __name__ == "__main__":
    db_connector = Database_Connector()
    conn = db_connector.connection
    data = db_connector.get_table_data(connection=conn, table_name="test_transactions")
    columns = db_connector.get_table_columns(connection=conn, table_name="test_transactions")
    selected_data = db_connector.get_table_data_selected(connection=conn, table_name="test_transactions", selected_columns=['TRANSACTIONID', 'AMOUNTINBIRR'])
    filtered_data = db_connector.get_filtered_data(connection=conn, table_name="test_transactions", filter_conditions={'CURRENCYTYPE': 'USD'}, selected_columns=['TRANSACTIONID', 'AMOUNTINBIRR', 'CURRENCYTYPE'])
    
    print("All Data:", data)
    print("Columns:", columns)
    print("Selected Data:", selected_data)

    print("Filtered Data: ", len(filtered_data))
