import sqlite3, json, yaml

# queries
LIST_TABLE_NAMES = "SELECT tbl_name FROM sqlite_master where type='table' AND tbl_name NOT IN ('sqlite_sequence');"

# read config
config = {}
with open("./config.yaml", "r") as stream:
  try:
    config = yaml.safe_load(stream)
  except yaml.YAMLError as error:
    print(error)


# connect to sql
conn = sqlite3.connect(config["database"])

def exclude_table(tableName, config):
  for pattern in config["exclude"]["tables"]:
    if pattern in tableName:
      return True
  return False

def exclude_column(columnName, config):
  for pattern in config["exclude"]["columns"]:
    if pattern in columnName:
      return True
  return False


# build data object from queries
data = {}
cursor = conn.cursor()
for row in cursor.execute(LIST_TABLE_NAMES):
  table = row[0]
  if exclude_table(table, config):
    continue
  records = []
  cursor2 = conn.cursor()
  # print("querying " + table)  
  for row2 in cursor2.execute("SELECT * FROM " + table):
    cols = list(map(lambda x: x[0], cursor2.description))
    record = {}
    for i, col in enumerate(cols):
      if not exclude_column(col, config):
        record[col] = row2[i]
    records.append(record)
  data[table] = records

# write data to json
with open(config["output"], "w") as f:
  f.write(json.dumps(data))


join_queries = []
with open("./joins.yaml", "r") as stream:
  try:
    join_query = ""
    joins = yaml.safe_load(stream)
    tables = []
    cols = []
    for join in joins:
      for side in joins[join]:
        table = next(iter(side))
        tables.append(table)
        cols.append(side[table])
    join_query += "SELECT * FROM " + tables[0] + " INNER JOIN " + tables[1] + " ON " + tables[0] + "." + cols[0] + " = " + tables[1] + "." + cols[1]
    join_queries.append(join_query)
  except yaml.YAMLError as error:
    print(error)

for query in join_queries:
  cursor4 = conn.cursor()
  # print(query)
  for row in cursor4.execute(query):
    cols = list(map(lambda x: x[0], cursor4.description))
    # print(cols)


