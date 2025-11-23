database = {}

def handle_query(query):
    query = query.strip().rstrip(";")
    if query.upper().startswith("CREATE TABLE"):
        create_table(query)
    elif query.upper().startswith("INSERT INTO"):
        insert_into(query)
    elif "," in query.upper() and "WHERE" in query.upper():
        select_join(query)
    elif query.upper().startswith("SELECT * FROM") and "WHERE" in query.upper():
        select_all_where(query)
    elif query.upper().startswith("SELECT DISTINCT"):
        select_distinct(query)
    elif query.upper().startswith("SELECT * FROM"):
        select_all(query)
    elif query.upper().startswith("SELECT") and any(func in query.upper() for func in ["MAX", "MIN", "SUM", "AVG"]):
        select_aggregate(query)
    elif query.upper().startswith("SELECT"):
        select_columns(query)
    elif query.upper().startswith("DROP TABLE"):
        drop_table(query)
    elif query.upper().startswith("DELETE FROM"):
        delete_from(query)
    elif query.upper().startswith("UPDATE"):
        update_table(query)

    else:
        print("Unsupported query.")

def create_table(query):
    try:
        query = query[len("CREATE TABLE"):].strip()
        table_name, rest = query.split("(", 1)
        table_name = table_name.strip()
        columns_definitions = rest.rstrip(")").split(",")

        columns = []
        types = []
        not_null_flags = []

        for col_def in columns_definitions:
            parts = col_def.strip().split()
            col_name = parts[0]
            col_type = parts[1].upper() if len(parts) > 1 else "TEXT"
            not_null = "NOT NULL" in col_def.upper()

            columns.append(col_name)
            types.append(col_type)
            not_null_flags.append(not_null)

        database[table_name] = {
            "columns": columns,
            "types": types,
            "not_null": not_null_flags,
            "rows": []
        }
        print(f"Table '{table_name}' created with columns {columns}")
    except Exception as e:
        print("Error creating table:", e)

def delete_from(query):
    try:
        if "WHERE" not in query.upper():
            print("DELETE without WHERE not supported.")
            return

        before_where, condition = query.upper().split("WHERE")
        table_name = before_where[len("DELETE FROM"):].strip().lower()
        condition = condition.strip().lower()

        if table_name not in database:
            print(f"Table '{table_name}' does not exist.")
            return

        table = database[table_name]

        operators = [">=", "<=", "!=", "=", ">", "<"]
        operator_used = None
        for op in operators:
            if op in condition:
                operator_used = op
                break

        if not operator_used:
            print("Invalid WHERE condition.")
            return

        column, value = condition.split(operator_used)
        column = column.strip()
        value = value.strip()
        if value.startswith("'") and value.endswith("'"):
            value = value[1:-1]
        elif value.isdigit():
            value = int(value)
        else:
            try:
                value = float(value)
            except:
                pass

        if column not in table["columns"]:
            print(f"Column '{column}' does not exist.")
            return

        col_index = table["columns"].index(column)
        original_len = len(table["rows"])
        table["rows"] = [row for row in table["rows"] if not compare(row[col_index], value, operator_used)]
        deleted = original_len - len(table["rows"])
        print(f"{deleted} row(s) deleted from '{table_name}'")

    except Exception as e:
        print("Error deleting rows:", e)

def drop_table(query):
    try:
        table_name = query[len("DROP TABLE"):].strip().lower()
        if table_name in database:
            del database[table_name]
            print(f"Table '{table_name}' dropped.")
        else:
            print(f"Table '{table_name}' does not exist.")
    except Exception as e:
        print("Error dropping table:", e)

def update_table(query):
    try:
        if "SET" not in query.upper() or "WHERE" not in query.upper():
            print("Invalid UPDATE syntax.")
            return

        query = query[len("UPDATE"):].strip()
        table_name, rest = query.split("SET", 1)
        table_name = table_name.strip().lower()
        set_part, where_part = rest.split("WHERE", 1)
        set_col, set_val = [x.strip() for x in set_part.strip().split("=")]
        cond_col, cond_val = None, None

        for op in ["=", "!=", ">", "<", ">=", "<="]:
            if op in where_part:
                cond_col, cond_val = [x.strip() for x in where_part.strip().split(op)]
                condition_op = op
                break

        if set_val.startswith("'") and set_val.endswith("'"):
            set_val = set_val[1:-1]
        elif set_val.isdigit():
            set_val = int(set_val)
        else:
            try:
                set_val = float(set_val)
            except:
                pass

        if cond_val.startswith("'") and cond_val.endswith("'"):
            cond_val = cond_val[1:-1]
        elif cond_val.isdigit():
            cond_val = int(cond_val)
        else:
            try:
                cond_val = float(cond_val)
            except:
                pass

        if table_name not in database:
            print(f"Table '{table_name}' does not exist.")
            return

        table = database[table_name]
        if set_col not in table["columns"] or cond_col not in table["columns"]:
            print("Invalid column name.")
            return

        set_idx = table["columns"].index(set_col)
        cond_idx = table["columns"].index(cond_col)
        updated = 0
        for row in table["rows"]:
            if compare(row[cond_idx], cond_val, condition_op):
                row[set_idx] = set_val
                updated += 1

        print(f"{updated} row(s) updated in '{table_name}'")

    except Exception as e:
        print("Error updating rows:", e)

def insert_into(query):
    try:
        query = query[len("INSERT INTO"):].strip()
        table_part, values_part = query.split("VALUES")
        table_name = table_part.strip()
        values_str = values_part.strip().lstrip("(").rstrip(")")
        raw_values = values_str.split(",")

        values = []
        for val in raw_values:
            val = val.strip()
            if val.upper() == "NULL":
                values.append(None)
            elif val.startswith("'") and val.endswith("'"):
                values.append(val[1:-1])
            elif val.isdigit():
                values.append(int(val))
            else:
                try:
                    values.append(float(val))
                except:
                    values.append(val)

        if table_name not in database:
            print(f"Table '{table_name}' does not exist.")
            return

        table = database[table_name]
        if len(values) != len(table["columns"]):
            print(f"Column count mismatch. Expected {len(table['columns'])}, got {len(values)})")
            return

        for i in range(len(values)):
            if table["not_null"][i] and values[i] is None:
                print(f"NOT NULL constraint violated for column '{table['columns'][i]}'")
                return
            expected_type = table["types"][i]
            if expected_type == "INT" and not isinstance(values[i], int):
                print(f"Type mismatch: Expected INT for '{table['columns'][i]}', got {type(values[i]).__name__}")
                return
            if expected_type == "FLOAT" and not isinstance(values[i], (int, float)):
                print(f"Type mismatch: Expected FLOAT for '{table['columns'][i]}', got {type(values[i]).__name__}")
                return
            if expected_type == "TEXT" and not isinstance(values[i], str):
                print(f"Type mismatch: Expected TEXT for '{table['columns'][i]}', got {type(values[i]).__name__}")
                return

        table["rows"].append(values)
        print(f"1 row inserted into '{table_name}'")

    except Exception as e:
        print("Error during insert:", e)

def select_all(query):
    try:
        table_name = query[len("SELECT * FROM"):].strip()
        if table_name not in database:
            print(f"Table '{table_name}' does not exist.")
            return
        table = database[table_name]
        print(" | ".join(table["columns"]))
        print("-" * (len(table["columns"]) * 10))
        for row in table["rows"]:
            print(" | ".join(str(x) for x in row))
    except Exception as e:
        print("Error during SELECT *:", e)

def select_columns(query):
    try:
        query = query[len("SELECT"):].strip()
        if "FROM" not in query.upper():
            print("Invalid SELECT query: missing FROM")
            return
        parts = query.upper().split("FROM")
        columns_part = query[:query.upper().find("FROM")].strip()
        table_name = query[query.upper().find("FROM") + len("FROM"):].strip()
        selected_columns = [col.strip() for col in columns_part.split(",")]
        if table_name not in database:
            print(f"Table '{table_name}' does not exist.")
            return
        table = database[table_name]
        table_columns = table["columns"]
        rows = table["rows"]
        for col in selected_columns:
            if col not in table_columns:
                print(f"Column '{col}' not found in table '{table_name}'")
                return
        print(" | ".join(selected_columns))
        print("-" * (len(selected_columns) * 10))
        for row in rows:
            output = []
            for col in selected_columns:
                index = table_columns.index(col)
                output.append(str(row[index]))
            print(" | ".join(output))
    except Exception as e:
        print("Error during SELECT with columns:", e)

def select_aggregate(query):
    try:
        query = query.strip()
        if "FROM" not in query.upper():
            print("Invalid aggregate query.")
            return
        select_part, table_name = query.split("FROM")
        select_part = select_part[len("SELECT"):].strip()
        table_name = table_name.strip()
        if table_name not in database:
            print(f"Table '{table_name}' does not exist.")
            return
        table = database[table_name]
        col_start = select_part.find("(") + 1
        col_end = select_part.find(")")
        func = select_part[:col_start - 1].strip().upper()
        column = select_part[col_start:col_end].strip()
        if column not in table["columns"]:
            print(f"Column '{column}' not found in table '{table_name}'")
            return
        col_index = table["columns"].index(column)
        values = [row[col_index] for row in table["rows"] if isinstance(row[col_index], (int, float))]
        if not values:
            print("No numeric data to aggregate.")
            return
        if func == "MAX":
            print(max(values))
        elif func == "MIN":
            print(min(values))
        elif func == "SUM":
            print(sum(values))
        elif func == "AVG":
            print(sum(values) / len(values))
        else:
            print(f"Unknown aggregate function '{func}'")
    except Exception as e:
        print("Error during aggregate SELECT:", e)

def select_all_where(query):
    try:
        query = query.strip().rstrip(";")

        if "WHERE" not in query.upper():
            print("Missing WHERE clause.")
            return

        where_index = query.upper().find("WHERE")
        before_where = query[:where_index].lower()
        where_clause = query[where_index + len("WHERE"):].strip()

        table_name = before_where[len("SELECT * FROM"):].strip()

        if table_name not in database:
            print(f"Table '{table_name}' does not exist.")
            return

        condition = where_clause
        operators = [">=", "<=", "!=", "=", ">", "<"]
        operator_used = None
        for op in operators:
            if op in condition:
                operator_used = op
                break

        if not operator_used:
            print("Invalid WHERE condition.")
            return

        column, value = condition.split(operator_used)
        column = column.strip()
        value = value.strip()

        if value.startswith("'") and value.endswith("'"):
            value = value[1:-1]
        elif value.isdigit():
            value = int(value)
        else:
            try:
                value = float(value)
            except:
                pass

        table = database[table_name]

        if column not in table["columns"]:
            print(f"Column '{column}' does not exist in table '{table_name}'.")
            return

        col_index = table["columns"].index(column)

        print(" | ".join(table["columns"]))
        print("-" * (len(table["columns"]) * 10))

        for row in table["rows"]:
            row_val = row[col_index]
            if compare(row_val, value, operator_used):
                print(" | ".join(str(x) for x in row))

    except Exception as e:
        print("Error in SELECT with WHERE:", e)


def compare(a, b, operator):
    if operator == "=":
        return a == b
    elif operator == "!=":
        return a != b
    elif operator == ">":
        return a > b
    elif operator == "<":
        return a < b
    elif operator == ">=":
        return a >= b
    elif operator == "<=":
        return a <= b
    return False

def select_distinct(query):
    try:
        query = query[len("SELECT DISTINCT"):].strip()
        if "FROM" not in query.upper():
            print("Missing FROM clause in DISTINCT query.")
            return
        parts = query.upper().split("FROM")
        columns_part = query[:query.upper().find("FROM")].strip()
        table_name = query[query.upper().find("FROM") + len("FROM"):].strip()
        selected_columns = [col.strip() for col in columns_part.split(",")]
        if table_name not in database:
            print(f"Table '{table_name}' does not exist.")
            return
        table = database[table_name]
        table_columns = table["columns"]
        rows = table["rows"]
        indexes = []
        for col in selected_columns:
            if col not in table_columns:
                print(f"Column '{col}' not found in table '{table_name}'")
                return
            indexes.append(table_columns.index(col))
        seen = set()
        print(" | ".join(selected_columns))
        print("-" * (len(selected_columns) * 10))
        for row in rows:
            values = tuple(row[i] for i in indexes)
            if values not in seen:
                seen.add(values)
                print(" | ".join(str(v) for v in values))
    except Exception as e:
        print("Error during SELECT DISTINCT:", e)

def select_join(query):
    try:
        # Do not uppercase full query — preserve table/column names
        select_part, where_clause = query.split("WHERE")
        tables_part = select_part[len("SELECT * FROM"):].strip()
        table1, table2 = [t.strip() for t in tables_part.split(",")]

        if table1 not in database or table2 not in database:
            print(f"One or both tables '{table1}', '{table2}' do not exist.")
            return

        cond = where_clause.strip()
        if "=" not in cond:
            print("Invalid JOIN condition.")
            return

        left, right = [x.strip() for x in cond.split("=")]
        t1_name, t1_col = left.split(".")
        t2_name, t2_col = right.split(".")

        if t1_name not in database or t2_name not in database:
            print(f"One or both tables '{t1_name}', '{t2_name}' do not exist.")
            return

        table1_data = database[t1_name]
        table2_data = database[t2_name]

        if t1_col not in table1_data["columns"] or t2_col not in table2_data["columns"]:
            print("One or both columns not found in respective tables.")
            return

        t1_idx = table1_data["columns"].index(t1_col)
        t2_idx = table2_data["columns"].index(t2_col)

        headers = [f"{table1}.{col}" for col in table1_data["columns"]] + [f"{table2}.{col}" for col in table2_data["columns"]]
        print(" | ".join(headers))
        print("-" * (len(headers) * 17))

        for row1 in table1_data["rows"]:
            for row2 in table2_data["rows"]:
                if row1[t1_idx] == row2[t2_idx]:
                    combined = row1 + row2
                    print(" | ".join(str(x) for x in combined))

    except Exception as e:
        print("Error in JOIN SELECT:", e)


def main():
    print("Welcome to Mini SQL Engine (type 'exit' to quit)")
    while True:
        query = input("sql> ")
        if query.lower() == "exit":
            break
        handle_query(query)

if __name__ == "__main__":
    main()