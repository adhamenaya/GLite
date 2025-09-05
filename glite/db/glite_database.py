import pandas as pd
from .conn import Conn  # replace with your actual DBConn import

class GLiteDatabase:
    def __init__(self, schema_name: str, dbname: str = None, env: str = "dev"):
        """
        Initialize database connection and ensure schema exists.

        :param schema_name: Name of the schema (mandatory)
        :param dbname: Name of the database (optional)
        :param env: Environment, 'dev' or 'prod' (default 'dev')
        """
        if not schema_name or not isinstance(schema_name, str):
            raise ValueError("Schema name must be provided as a non-empty string.")
        self.schema = schema_name

        # Database name
        if dbname is None:
            self.dbname = f"ai4ci-db-{env}" if env == "dev" else "ai4ci-db"
        else:
            self.dbname = dbname

        # Table names
        self.table_name_node = "node"
        self.table_name_edge = "edge"
        self.table_name_node_attr = "node_attr"
        self.table_name_edge_attr = "edge_attr"

        # Connect to the database
        self.conn = Conn(
            dbname=self.dbname,
            user="postgres",
            password="ci4ai",
            host="128.40.193.10",
            port="5433"
        )
        self.connection, self.cursor = self.conn.connect()

        # Ensure the schema exists
        try:
            self.cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema};")
            self.connection.commit()
            print(f"Schema '{self.schema}' ensured in database '{self.dbname}'.")

            self.create_all_tables()

        except Exception as e:
            print(f"Error creating schema '{self.schema}': {e}")
            if self.connection:
                self.connection.rollback()

    def create_all_tables(self):
        """
        Create all tables in the schema if they do not already exist.
        Order matters due to foreign key dependencies.
        """
        try:
            # Node table first (referenced by edge and node_attr)
            self.create_table_node()
            # Edge table (references node)
            self.create_table_edge()
            # Node attributes table
            self.create_table_node_attr()
            # Edge attributes table
            self.create_table_edge_attr()
            print(f"All tables created successfully in schema '{self.schema}'.")
        except Exception as e:
            print(f"Error creating all tables: {e}")
            if self.connection:
                self.connection.rollback()

    # --- Table creation methods ---
    def create_table_node(self):
        stmt = f"""
        CREATE TABLE IF NOT EXISTS {self.schema}.{self.table_name_node} (
            id SERIAL PRIMARY KEY,
            label TEXT,
            type TEXT
        );
        """
        self._execute_statement(stmt, self.table_name_node)

    def create_table_edge(self):
        stmt = f"""
        CREATE TABLE IF NOT EXISTS {self.schema}.{self.table_name_edge} (
            id SERIAL PRIMARY KEY,
            label TEXT,
            source_id INTEGER REFERENCES {self.schema}.{self.table_name_node} (id)
                ON DELETE CASCADE
                ON UPDATE CASCADE,
            target_id INTEGER REFERENCES {self.schema}.{self.table_name_node} (id)
                ON DELETE CASCADE
                ON UPDATE CASCADE
        );
        """
        self._execute_statement(stmt, self.table_name_edge)

    def create_table_node_attr(self):
        stmt = f"""
        CREATE TABLE IF NOT EXISTS {self.schema}.{self.table_name_node_attr} (
            id SERIAL PRIMARY KEY,
            node_id INTEGER REFERENCES {self.schema}.{self.table_name_node} (id)
                ON DELETE CASCADE
                ON UPDATE CASCADE,
            key TEXT NOT NULL,
            value TEXT NOT NULL
        );
        """
        self._execute_statement(stmt, self.table_name_node_attr)

    def create_table_edge_attr(self):
        stmt = f"""
        CREATE TABLE IF NOT EXISTS {self.schema}.{self.table_name_edge_attr} (
            id SERIAL PRIMARY KEY,
            edge_id INTEGER REFERENCES {self.schema}.{self.table_name_edge} (id)
                ON DELETE CASCADE
                ON UPDATE CASCADE,
            key TEXT NOT NULL,
            value TEXT NOT NULL
        );
        """
        self._execute_statement(stmt, self.table_name_edge_attr)

    # --- Private helper ---
    def _execute_statement(self, statement, table_name=""):
        try:
            if self.connection and self.cursor:
                self.cursor.execute(statement)
                self.connection.commit()
                print(f"Table '{table_name}' created successfully in schema '{self.schema}'.")
            else:
                print("Database connection not established.")
        except Exception as e:
            print(f"Error creating table '{table_name}': {e}")
            if self.connection:
                self.connection.rollback()

    # --- Insert methods ---
    def insert_node(self, label, node_type, verbose=True):
        """
        Insert a new node or return existing id, with detailed debug info.

        :param label: Node label
        :param node_type: Node type
        :param verbose: Whether to print informative messages (default True)
        :return: Node id or None if failed
        """
        if not isinstance(label, str) or pd.isna(label):
            if verbose:
                print(f"Skipping invalid label: {label}")
            return None

        select_stmt = f"""
        SELECT id FROM {self.schema}.{self.table_name_node} WHERE label=%s AND type=%s;
        """
        insert_stmt = f"""
        INSERT INTO {self.schema}.{self.table_name_node} (label, type)
        VALUES (%s, %s) RETURNING id;
        """

        try:
            if verbose:
                print(f"Checking if node exists: label='{label}', type='{node_type}'")
                print(f"Executing SELECT: {select_stmt.strip()}")

            self.cursor.execute(select_stmt, (label, node_type))
            existing = self.cursor.fetchone()
            if existing:
                # Always access 'id' key if dictionary, else index 0
                node_id = existing.get('id') if isinstance(existing, dict) else existing[0]
                if verbose:
                    print(f"Node already exists: '{label}' (type: {node_type}) with ID {node_id}")
                return node_id

            if verbose:
                print(f"Inserting new node: label='{label}', type='{node_type}'")
                print(f"Executing INSERT: {insert_stmt.strip()}")

            self.cursor.execute(insert_stmt, (label, node_type))
            new_node = self.cursor.fetchone()

            if new_node is None:
                if verbose:
                    print("Insert returned no ID (fetchone() is None). Rolling back.")
                if self.connection:
                    self.connection.rollback()
                return None

            # Handle dict or tuple consistently
            node_id = new_node.get('id') if isinstance(new_node, dict) else new_node[0]

            if self.connection:
                self.connection.commit()

            if verbose:
                print(f"New node created: '{label}' (type: {node_type}) with ID {node_id}")

            return node_id

        except Exception as e:
            if verbose:
                print(f"Error inserting node '{label}': {type(e).__name__}: {e}")
                import traceback
                traceback.print_exc()
            if self.connection:
                self.connection.rollback()
            return None

    def insert_edge(self, label, source_id, target_id, verbose=True):
        """
        Insert a new edge with given label, source node id, and target node id.

        :param label: Edge label
        :param source_id: Source node ID
        :param target_id: Target node ID
        :param verbose: Whether to print informative messages (default True)
        :return: Edge id or None if failed
        """
        if not isinstance(label, str) or pd.isna(label):
            if verbose:
                print(f"Skipping invalid edge label: {label}")
            return None
        if not isinstance(source_id, int) or not isinstance(target_id, int):
            if verbose:
                print(f"Invalid source_id ({source_id}) or target_id ({target_id})")
            return None

        select_stmt = f"""
        SELECT id FROM {self.schema}.{self.table_name_edge}
        WHERE label=%s AND source_id=%s AND target_id=%s;
        """
        insert_stmt = f"""
        INSERT INTO {self.schema}.{self.table_name_edge} (label, source_id, target_id)
        VALUES (%s, %s, %s) RETURNING id;
        """

        try:
            # Check if edge already exists
            self.cursor.execute(select_stmt, (label, source_id, target_id))
            existing = self.cursor.fetchone()
            if existing:
                edge_id = existing.get('id') if isinstance(existing, dict) else existing[0]
                if verbose:
                    print(f"Edge already exists: '{label}' from node {source_id} to node {target_id} with ID {edge_id}")
                return edge_id

            # Insert new edge
            self.cursor.execute(insert_stmt, (label, source_id, target_id))
            new_edge = self.cursor.fetchone()
            if new_edge is None:
                if verbose:
                    print("Insert returned no ID (fetchone() is None). Rolling back.")
                if self.connection:
                    self.connection.rollback()
                return None

            edge_id = new_edge.get('id') if isinstance(new_edge, dict) else new_edge[0]

            if self.connection:
                self.connection.commit()

            if verbose:
                print(f"New edge created: '{label}' from node {source_id} to node {target_id} with ID {edge_id}")

            return edge_id

        except Exception as e:
            if verbose:
                print(f"Error inserting edge '{label}': {type(e).__name__}: {e}")
                import traceback
                traceback.print_exc()
            if self.connection:
                self.connection.rollback()
            return None

    def insert_node_attribute(self, node_id, key, value, verbose=True):
        """
        Insert a node attribute.

        :param node_id: Node ID
        :param key: Attribute key
        :param value: Attribute value
        :param verbose: Whether to print informative messages (default True)
        :return: Attribute id or None if failed
        """
        if not isinstance(node_id, int):
            if verbose:
                print(f"Invalid node_id: {node_id}")
            return None

        if not isinstance(key, str) or not isinstance(value, str):
            if verbose:
                print(f"Invalid key ({key}) or value ({value}) - both must be strings")
            return None

        select_stmt = f"""
        SELECT id FROM {self.schema}.{self.table_name_node_attr}
        WHERE node_id=%s AND key=%s AND value=%s;
        """
        insert_stmt = f"""
        INSERT INTO {self.schema}.{self.table_name_node_attr} (node_id, key, value)
        VALUES (%s, %s, %s) RETURNING id;
        """

        try:
            self.cursor.execute(select_stmt, (node_id, key, value))
            existing = self.cursor.fetchone()
            if existing:
                attr_id = existing.get('id') if isinstance(existing, dict) else existing[0]
                if verbose:
                    print(f"Node attribute already exists: node {node_id}, key '{key}', value '{value}' with ID {attr_id}")
                return attr_id

            self.cursor.execute(insert_stmt, (node_id, key, value))
            new_attr = self.cursor.fetchone()
            if new_attr is None:
                if verbose:
                    print("Insert returned no ID (fetchone() is None). Rolling back.")
                if self.connection:
                    self.connection.rollback()
                return None

            attr_id = new_attr.get('id') if isinstance(new_attr, dict) else new_attr[0]

            if self.connection:
                self.connection.commit()

            if verbose:
                print(f"New node attribute created: node {node_id}, key '{key}', value '{value}' with ID {attr_id}")

            return attr_id

        except Exception as e:
            if verbose:
                print(f"Error inserting node attribute: {type(e).__name__}: {e}")
                import traceback
                traceback.print_exc()
            if self.connection:
                self.connection.rollback()
            return None

    def insert_edge_attribute(self, edge_id, key, value, verbose=True):
        """
        Insert an edge attribute.

        :param edge_id: Edge ID
        :param key: Attribute key
        :param value: Attribute value
        :param verbose: Whether to print informative messages (default True)
        :return: Attribute id or None if failed
        """
        if not isinstance(edge_id, int):
            if verbose:
                print(f"Invalid edge_id: {edge_id}")
            return None

        if not isinstance(key, str) or not isinstance(value, str):
            if verbose:
                print(f"Invalid key ({key}) or value ({value}) - both must be strings")
            return None

        select_stmt = f"""
        SELECT id FROM {self.schema}.{self.table_name_edge_attr}
        WHERE edge_id=%s AND key=%s AND value=%s;
        """
        insert_stmt = f"""
        INSERT INTO {self.schema}.{self.table_name_edge_attr} (edge_id, key, value)
        VALUES (%s, %s, %s) RETURNING id;
        """

        try:
            self.cursor.execute(select_stmt, (edge_id, key, value))
            existing = self.cursor.fetchone()
            if existing:
                attr_id = existing.get('id') if isinstance(existing, dict) else existing[0]
                if verbose:
                    print(f"Edge attribute already exists: edge {edge_id}, key '{key}', value '{value}' with ID {attr_id}")
                return attr_id

            self.cursor.execute(insert_stmt, (edge_id, key, value))
            new_attr = self.cursor.fetchone()
            if new_attr is None:
                if verbose:
                    print("Insert returned no ID (fetchone() is None). Rolling back.")
                if self.connection:
                    self.connection.rollback()
                return None

            attr_id = new_attr.get('id') if isinstance(new_attr, dict) else new_attr[0]

            if self.connection:
                self.connection.commit()

            if verbose:
                print(f"New edge attribute created: edge {edge_id}, key '{key}', value '{value}' with ID {attr_id}")

            return attr_id

        except Exception as e:
            if verbose:
                print(f"Error inserting edge attribute: {type(e).__name__}: {e}")
                import traceback
                traceback.print_exc()
            if self.connection:
                self.connection.rollback()
            return None
    # --- Read methods ---
    def read_all(self, table_name, output_format='dataframe'):
        """ Fetch and return all rows from the specified table in the requested format."""
        select_statement = f"SELECT * FROM {self.schema}.{table_name};"
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(select_statement)
                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]

                if output_format == 'dataframe':
                    return pd.DataFrame(rows, columns=columns)
                elif output_format == 'list':
                    return rows
                else:
                    raise ValueError("Invalid output_format. Choose 'dataframe' or 'list'.")
        except Exception as e:
            print(f"Error retrieving data from table '{table_name}': {e}")
            return None

    def read_by_id(self, table_name, record_id, output_format='dataframe'):
        """Fetch and return a single record by its id from the specified table."""
        select_statement = f"SELECT * FROM {self.schema}.{table_name} WHERE id = %s;"

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(select_statement, (record_id,))
                row = cursor.fetchone()
                if not row:
                    print(f"No record found with id={record_id} in table '{table_name}'.")
                    return None

                columns = [desc[0] for desc in cursor.description]

                if output_format == 'dataframe':
                    return pd.DataFrame([row], columns=columns)
                elif output_format == 'dict':
                    return dict(zip(columns, row))
                else:
                    raise ValueError("Invalid output_format. Choose 'dataframe' or 'dict'.")

        except Exception as e:
            print(f"Error retrieving record id={record_id} from table '{table_name}': {e}")
            return None

    def read_by_label(self, table_name, label, output_format='dataframe'):
        """Fetch and return a single record by its label from the specified table."""
        select_statement = f"SELECT * FROM {self.schema}.{table_name} WHERE label = %s;"

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(select_statement, (label,))
                row = cursor.fetchone()

                if not row:
                    print(f"No record found with label={label!r} in table '{table_name}'.")
                    return None

                columns = [desc[0] for desc in cursor.description]

                if output_format == 'dataframe':
                    return pd.DataFrame([row], columns=columns)
                elif output_format == 'dict':
                    return dict(zip(columns, row))
                else:
                    raise ValueError("Invalid output_format. Choose 'dataframe' or 'dict'.")

        except Exception as e:
            self.connection.rollback()
            print(f"Error retrieving record label={label!r} from table '{table_name}': {e}")
            return None


    # --- Delete and drop methods ---
    def delete_by_id(self, table_name, record_id):
        """Delete a record by its ID with user confirmation."""
        user_input = self.delete_confirmation(type=f"record from {table_name} with ID {record_id}")
        if user_input.lower() == "yes":
            delete_statement = f"DELETE FROM {self.schema}.{table_name} WHERE id = %s;"
            try:
                if self.connection and self.cursor:
                    self.cursor.execute(delete_statement, (record_id,))
                    self.connection.commit()
                    print(f"Record with ID {record_id} has been deleted from '{table_name}'.")
                else:
                    print("Database connection not established.")
            except Exception as e:
                print(f"Error deleting record with ID {record_id} from '{table_name}': {e}")
        else:
            print(f"No record was deleted from '{table_name}'.")

    def drop_table(self, table_name):
        """Drop a table from the database with user confirmation."""
        user_input = self.delete_confirmation(type=f"table '{table_name}'")
        if user_input.lower() == "yes":
            drop_statement = f"DROP TABLE IF EXISTS {self.schema}.{table_name} CASCADE;"
            try:
                if self.connection and self.cursor:
                    self.cursor.execute(drop_statement)
                    self.connection.commit()
                    print(f"Table '{table_name}' dropped successfully.")
                else:
                    print("Database connection not established.")
            except Exception as e:
                print(f"Error dropping table '{table_name}': {e}")
        else:
            print(f"Table '{table_name}' was not dropped.")

    # --- Finish connection ---
    def finish(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()