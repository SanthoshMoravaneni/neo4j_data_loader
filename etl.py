import pandas as pd
from neo4j import GraphDatabase
import yaml

class load_data_to_neo4j:
    def __init__(self, URI, USERNAME, PASSWORD, FILE_PATH):
        """
        Initialize a load_data_to_neo4j instance with database credentials and the path to the CSV file.

        :param URI: The URI for connecting to the Neo4j database.
        :param USERNAME: The USERNAME for authentication.
        :param PASSWORD: The PASSWORD for authentication.
        :param FILE_PATH: The file path of the CSV to be loaded.
        """
        try:
            self.driver = GraphDatabase.driver(URI, auth=(USERNAME, PASSWORD))
            self.FILE_PATH = FILE_PATH
            print("Neo4j driver initialized successfully.")
        except Exception as e:
            print(f"Failed to initialize Neo4j driver: {e}")

    def load_data(self):
        """
        Load data from a specified CSV file.

        :return: A pandas DataFrame if successful, None otherwise.
        """
        try:
            data = pd.read_csv(self.FILE_PATH)
            print("CSV data loaded successfully.")
            return data
        except FileNotFoundError:
            print("File not found. Please check the file path.")
        except pd.errors.ParserError:
            print("Error parsing the CSV file.")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
        return None

    def create_product_nodes(self, tx, products):
        """
        Create product nodes in the Neo4j database using a Cypher query.

        :param tx: The transaction function handler for Neo4j.
        :param products: A list of dictionaries, each representing a product to be created as a node.
        """
        try:
            query = """
            UNWIND $products AS product
            CREATE (p:Product) SET p = product
            """
            tx.run(query, products=products)
        except Exception as e:
            print(f"Failed to create product nodes: {e}")  # Keeping this for error logging

    def load_csv_to_neo4j(self, data, batch_size=1000):
        """
        Load CSV data into the Neo4j database in batches.

        :param data: A pandas DataFrame containing the data to be loaded.
        :param batch_size: The number of records to process in each batch.
        """
        try:
            with self.driver.session() as session:
                batch = []
                for index, row in data.iterrows():
                    batch.append(row.to_dict())
                    if len(batch) >= batch_size:
                        session.write_transaction(self.create_product_nodes, batch)
                        batch = []
                if batch:
                    session.write_transaction(self.create_product_nodes, batch)
                print("All data have been loaded into Neo4j.")
        except Exception as e:
            print(f"Failed to load data to Neo4j: {e}")

    def close(self):
        """
        Close the Neo4j driver connection.
        """
        try:
            self.driver.close()
            print("Neo4j connection closed successfully.")
        except Exception as e:
            print(f"Failed to close Neo4j connection: {e}")

    def run(self):
        """
        Execute the workflow to load data from the CSV file to the Neo4j database.
        """
        try:
            data = self.load_data()
            if data is not None:
                self.load_csv_to_neo4j(data)
        except Exception as e:
            print(f"Failed to run loading process: {e}")


if __name__ == "__main__":
    with open("config.yaml", "r") as file:
        config = yaml.safe_load(file)
    URI = config['CREDS']['URI']
    USERNAME = config['CREDS']['USERNAME']
    PASSWORD = config['CREDS']['PASSWORD']
    FILE_PATH = config['CREDS']['FILE_PATH']

    loader = load_data_to_neo4j(URI, USERNAME, PASSWORD, FILE_PATH)
    loader.run()
    loader.close()
