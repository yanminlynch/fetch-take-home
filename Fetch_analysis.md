```python
import pandas as pd

brands_file = "/Users/funsize/fetch-data-modeling-exercise/data/brands_cleaned.csv"
receipts_file = "/Users/funsize/fetch-data-modeling-exercise/data/receipts_cleaned.csv"
users_file = "/Users/funsize/fetch-data-modeling-exercise/data/users_cleaned.csv"

# Display the first few rows of each DataFrame
print("Brands DataFrame:")
print(df_brands.head(), "\n")

print("Receipts DataFrame:")
print(df_receipts.head(), "\n")

print("Users DataFrame:")
print(df_users.head(), "\n")

```

    Brands DataFrame:
             category              cpg_$id_$oid       barcode topBrand  \
    0          Baking  601ac114be37ce2ead437550  511111019862    False   
    1       Beverages  5332f5fbe4b03c9a25efd0ba  511111519928    False   
    2          Baking  601ac142be37ce2ead437559  511111819905    False   
    3          Baking  601ac142be37ce2ead437559  511111519874    False   
    4  Candy & Sweets  5332fa12e4b03c9a25efd1e7  511111319917    False   
    
           categoryCode                  _id_$oid cpg_$ref  \
    0            BAKING  601ac115be37ce2ead437551     Cogs   
    1         BEVERAGES  601c5460be37ce2ead43755f     Cogs   
    2            BAKING  601ac142be37ce2ead43755d     Cogs   
    3            BAKING  601ac142be37ce2ead43755a     Cogs   
    4  CANDY_AND_SWEETS  601ac142be37ce2ead43755e     Cogs   
    
                            name                      brandCode  
    0  test brand @1612366101024                        Unknown  
    1                  Starbucks                      STARBUCKS  
    2  test brand @1612366146176  TEST BRANDCODE @1612366146176  
    3  test brand @1612366146051  TEST BRANDCODE @1612366146051  
    4  test brand @1612366146827  TEST BRANDCODE @1612366146827   
    
    Receipts DataFrame:
      bonusPointsEarned  modifyDate_$date purchasedItemCount  \
    0             500.0     1609687536000                5.0   
    1             150.0     1609687488000                2.0   
    2               5.0     1609687542000                1.0   
    3               5.0     1609687539000                4.0   
    4               5.0     1609687511000                2.0   
    
                                  rewardsReceiptItemList  \
    0  [{"barcode": "4011", "description": "ITEM NOT ...   
    1  [{"barcode": "4011", "description": "ITEM NOT ...   
    2  [{"needsFetchReview": false, "partnerItemId": ...   
    3  [{"barcode": "4011", "description": "ITEM NOT ...   
    4  [{"barcode": "4011", "description": "ITEM NOT ...   
    
                                 bonusPointsEarnedReason finishedDate_$date  \
    0  Receipt number 2 completed, bonus point schedu...    1609687531000.0   
    1  Receipt number 5 completed, bonus point schedu...    1609687483000.0   
    2                         All-receipts receipt bonus            Unknown   
    3                         All-receipts receipt bonus    1609687534000.0   
    4                         All-receipts receipt bonus    1609687511000.0   
    
      pointsAwardedDate_$date purchaseDate_$date                    userId  \
    0         1609687531000.0    1609632000000.0  5ff1e1eacfcf6c399c274ae6   
    1         1609687483000.0    1609601083000.0  5ff1e194b6a9d73a3a9f1052   
    2                 Unknown    1609632000000.0  5ff1e1f1cfcf6c399c274b0b   
    3         1609687534000.0    1609632000000.0  5ff1e1eacfcf6c399c274ae6   
    4         1609687506000.0    1609601106000.0  5ff1e194b6a9d73a3a9f1052   
    
                       _id_$oid rewardsReceiptStatus totalSpent  \
    0  5ff1e1eb0a720f0523000575             FINISHED      26.00   
    1  5ff1e1bb0a720f052300056b             FINISHED      11.00   
    2  5ff1e1f10a720f052300057a             REJECTED      10.00   
    3  5ff1e1ee0a7214ada100056f             FINISHED      28.00   
    4  5ff1e1d20a7214ada1000561             FINISHED       1.00   
    
       dateScanned_$date pointsEarned  createDate_$date  
    0      1609687531000        500.0     1609687531000  
    1      1609687483000        150.0     1609687483000  
    2      1609687537000            5     1609687537000  
    3      1609687534000          5.0     1609687534000  
    4      1609687506000          5.0     1609687506000   
    
    Users DataFrame:
      state  createdDate_$date      role  lastLogin_$date  \
    0    WI      1609687444800  consumer  1609687537858.0   
    1    WI      1609687444800  consumer  1609687537858.0   
    2    WI      1609687530554  consumer  1609687530597.0   
    3    WI      1609687444800  consumer  1609687537858.0   
    4    WI      1609687444800  consumer  1609687537858.0   
    
                       _id_$oid  active signUpSource  
    0  5ff1e194b6a9d73a3a9f1052    True        Email  
    1  5ff1e194b6a9d73a3a9f1052    True        Email  
    2  5ff1e1eacfcf6c399c274ae6    True        Email  
    3  5ff1e194b6a9d73a3a9f1052    True        Email  
    4  5ff1e194b6a9d73a3a9f1052    True        Email   
    



```python
#check missing values
```


```python
import pandas as pd
import sqlite3

brands_file = "/Users/funsize/fetch-data-modeling-exercise/data/brands_cleaned.csv"
receipts_file = "/Users/funsize/fetch-data-modeling-exercise/data/receipts_cleaned.csv"
users_file = "/Users/funsize/fetch-data-modeling-exercise/data/users_cleaned.csv"

# Create an in-memory SQLite database
conn = sqlite3.connect(":memory:")

# Load DataFrames into SQL tables
df_brands.to_sql("brands", conn, index=False, if_exists="replace")
df_receipts.to_sql("receipts", conn, index=False, if_exists="replace")
df_users.to_sql("users", conn, index=False, if_exists="replace")

# SQL queries to check missing data
missing_data_queries = {
    "brands": """
        SELECT 'brands' AS table_name, 'brandCode' AS column_name, COUNT(*) AS missing_values 
        FROM brands WHERE brandCode IS NULL
    """,
    "receipts": """
        SELECT 'receipts' AS table_name, 'bonusPointsEarned' AS column_name, COUNT(*) AS missing_values 
        FROM receipts WHERE bonusPointsEarned IS NULL
        UNION ALL
        SELECT 'receipts', 'purchasedItemCount', COUNT(*) FROM receipts WHERE purchasedItemCount IS NULL
        UNION ALL
        SELECT 'receipts', 'rewardsReceiptItemList', COUNT(*) FROM receipts WHERE rewardsReceiptItemList IS NULL
        UNION ALL
        SELECT 'receipts', 'bonusPointsEarnedReason', COUNT(*) FROM receipts WHERE bonusPointsEarnedReason IS NULL
        UNION ALL
        SELECT 'receipts', 'finishedDate_$date', COUNT(*) FROM receipts WHERE finishedDate_$date IS NULL
        UNION ALL
        SELECT 'receipts', 'pointsAwardedDate_$date', COUNT(*) FROM receipts WHERE pointsAwardedDate_$date IS NULL
        UNION ALL
        SELECT 'receipts', 'purchaseDate_$date', COUNT(*) FROM receipts WHERE purchaseDate_$date IS NULL
        UNION ALL
        SELECT 'receipts', 'userId', COUNT(*) FROM receipts WHERE userId IS NULL
        UNION ALL
        SELECT 'receipts', 'rewardsReceiptStatus', COUNT(*) FROM receipts WHERE rewardsReceiptStatus IS NULL
        UNION ALL
        SELECT 'receipts', 'totalSpent', COUNT(*) FROM receipts WHERE totalSpent IS NULL
        UNION ALL
        SELECT 'receipts', 'dateScanned_$date', COUNT(*) FROM receipts WHERE dateScanned_$date IS NULL
        UNION ALL
        SELECT 'receipts', 'pointsEarned', COUNT(*) FROM receipts WHERE pointsEarned IS NULL
        UNION ALL
        SELECT 'receipts', 'createDate_$date', COUNT(*) FROM receipts WHERE createDate_$date IS NULL
    """,
    "users": """
        SELECT 'users' AS table_name, 'state' AS column_name, COUNT(*) AS missing_values FROM users WHERE state IS NULL
        UNION ALL
        SELECT 'users', 'createdDate_$date', COUNT(*) FROM users WHERE createdDate_$date IS NULL
        UNION ALL
        SELECT 'users', 'role', COUNT(*) FROM users WHERE role IS NULL
        UNION ALL
        SELECT 'users', 'lastLogin_$date', COUNT(*) FROM users WHERE lastLogin_$date IS NULL
        UNION ALL
        SELECT 'users', '_id_$oid', COUNT(*) FROM users WHERE _id_$oid IS NULL
        UNION ALL
        SELECT 'users', 'active', COUNT(*) FROM users WHERE active IS NULL
        UNION ALL
        SELECT 'users', 'signUpSource', COUNT(*) FROM users WHERE signUpSource IS NULL
    """
}

# Execute queries and collect results
missing_values_data = []
for table, query in missing_data_queries.items():
    result = pd.read_sql(query, conn)
    missing_values_data.append(result)

# Combine results into a single DataFrame
missing_values_df = pd.concat(missing_values_data, ignore_index=True)

# Close the database connection
conn.close()

# Display the missing values report
print(missing_values_df)


```

       table_name              column_name  missing_values
    0      brands                brandCode              35
    1    receipts        bonusPointsEarned               0
    2    receipts       purchasedItemCount               0
    3    receipts   rewardsReceiptItemList               0
    4    receipts  bonusPointsEarnedReason               0
    5    receipts       finishedDate_$date               0
    6    receipts  pointsAwardedDate_$date               0
    7    receipts       purchaseDate_$date               0
    8    receipts                   userId               0
    9    receipts     rewardsReceiptStatus               0
    10   receipts               totalSpent               0
    11   receipts        dateScanned_$date               0
    12   receipts             pointsEarned               0
    13   receipts         createDate_$date               0
    14      users                    state               0
    15      users        createdDate_$date               0
    16      users                     role               0
    17      users          lastLogin_$date               0
    18      users                 _id_$oid               0
    19      users                   active               0
    20      users             signUpSource               0



```python
import pandas as pd
import sqlite3

brands_file = "/Users/funsize/fetch-data-modeling-exercise/data/brands_cleaned.csv"
receipts_file = "/Users/funsize/fetch-data-modeling-exercise/data/receipts_cleaned.csv"
users_file = "/Users/funsize/fetch-data-modeling-exercise/data/users_cleaned.csv"

# Create an in-memory SQLite database
conn = sqlite3.connect(":memory:")

# Load DataFrames into SQL tables
df_brands.to_sql("brands", conn, index=False, if_exists="replace")
df_receipts.to_sql("receipts", conn, index=False, if_exists="replace")
df_users.to_sql("users", conn, index=False, if_exists="replace")
# SQL queries to remove duplicate rows
remove_duplicates_queries = {
    "brands": """
        CREATE TABLE brands_cleaned AS 
        SELECT * FROM brands 
        WHERE rowid IN (
            SELECT MIN(rowid) 
            FROM brands 
            GROUP BY category, cpg_$id_$oid, barcode, topBrand, categoryCode, _id_$oid, cpg_$ref, name, brandCode
        )
    """,
    "receipts": """
        CREATE TABLE receipts_cleaned AS 
        SELECT * FROM receipts 
        WHERE rowid IN (
            SELECT MIN(rowid) 
            FROM receipts 
            GROUP BY bonusPointsEarned, modifyDate_$date, purchasedItemCount, rewardsReceiptItemList, 
                     bonusPointsEarnedReason, finishedDate_$date, pointsAwardedDate_$date, purchaseDate_$date, 
                     userId, rewardsReceiptStatus, totalSpent, dateScanned_$date, pointsEarned, createDate_$date
        )
    """,
    "users": """
        CREATE TABLE users_cleaned AS 
        SELECT * FROM users 
        WHERE rowid IN (
            SELECT MIN(rowid) 
            FROM users 
            GROUP BY state, createdDate_$date, role, lastLogin_$date, _id_$oid, active, signUpSource
        )
    """
}

# Execute SQL queries to remove duplicates
for table, query in remove_duplicates_queries.items():
    conn.execute(query)

# Load cleaned data back into pandas DataFrames
df_brands_cleaned = pd.read_sql("SELECT * FROM brands_cleaned", conn)
df_receipts_cleaned = pd.read_sql("SELECT * FROM receipts_cleaned", conn)
df_users_cleaned = pd.read_sql("SELECT * FROM users_cleaned", conn)


# Save cleaned data back to CSV files (optional)
df_brands_cleaned.to_csv("brands_cleaned.csv", index=False)
df_receipts_cleaned.to_csv("receipts_cleaned.csv", index=False)
df_users_cleaned.to_csv("users_cleaned.csv", index=False)

# Print results
print("Duplicates removed. Cleaned data saved to CSV files.")
# SQL queries to check duplicate rows
duplicate_queries = {
    "brands": """
        SELECT 'brands' AS table_name, COUNT(*) AS duplicate_rows 
        FROM (
            SELECT *, COUNT(*) AS count FROM brands 
            GROUP BY category, cpg_$id_$oid, barcode, topBrand, categoryCode, _id_$oid, cpg_$ref, name, brandCode 
            HAVING count > 1
        )
    """,
    "receipts": """
        SELECT 'receipts' AS table_name, COUNT(*) AS duplicate_rows 
        FROM (
            SELECT *, COUNT(*) AS count FROM receipts 
            GROUP BY bonusPointsEarned, modifyDate_$date, purchasedItemCount, rewardsReceiptItemList, 
                     bonusPointsEarnedReason, finishedDate_$date, pointsAwardedDate_$date, purchaseDate_$date, 
                     userId, rewardsReceiptStatus, totalSpent, dateScanned_$date, pointsEarned, createDate_$date 
            HAVING count > 1
        )
    """,
    "users": """
        SELECT 'users' AS table_name, COUNT(*) AS duplicate_rows 
        FROM (
            SELECT *, COUNT(*) AS count FROM users 
            GROUP BY state, createdDate_$date, role, lastLogin_$date, _id_$oid, active, signUpSource 
            HAVING count > 1
        )
    """
}

# Execute queries and collect results
duplicate_data = []
for table, query in duplicate_queries.items():
    result = pd.read_sql(query, conn)
    duplicate_data.append(result)

# Combine results into a single DataFrame
duplicates_df = pd.concat(duplicate_data, ignore_index=True)

# Close the database connection
conn.close()

# Print the duplicates report
print(duplicates_df)



```

    Duplicates removed. Cleaned data saved to CSV files.
      table_name  duplicate_rows
    0     brands               0
    1   receipts               0
    2      users              70



```python
brands_file = "/Users/funsize/fetch-data-modeling-exercise/data/brands_cleaned.csv"
receipts_file = "/Users/funsize/fetch-data-modeling-exercise/data/receipts_cleaned.csv"
users_file = "/Users/funsize/fetch-data-modeling-exercise/data/users_cleaned.csv"

# Create an in-memory SQLite database
conn = sqlite3.connect(":memory:")

# Load DataFrames into SQL tables
df_brands.to_sql("brands", conn, index=False, if_exists="replace")
df_receipts.to_sql("receipts", conn, index=False, if_exists="replace")
df_users.to_sql("users", conn, index=False, if_exists="replace")

# SQL queries to remove duplicate rows
remove_duplicates_queries = {
    "brands": """
        CREATE TABLE brands_cleaned AS 
        SELECT * FROM brands 
        WHERE rowid IN (
            SELECT MIN(rowid) 
            FROM brands 
            GROUP BY category, cpg_$id_$oid, barcode, topBrand, categoryCode, _id_$oid, cpg_$ref, name, brandCode
        )
    """,
    "receipts": """
        CREATE TABLE receipts_cleaned AS 
        SELECT * FROM receipts 
        WHERE rowid IN (
            SELECT MIN(rowid) 
            FROM receipts 
            GROUP BY bonusPointsEarned, modifyDate_$date, purchasedItemCount, rewardsReceiptItemList, 
                     bonusPointsEarnedReason, finishedDate_$date, pointsAwardedDate_$date, purchaseDate_$date, 
                     userId, rewardsReceiptStatus, totalSpent, dateScanned_$date, pointsEarned, createDate_$date
        )
    """,
    "users": """
        CREATE TABLE users_cleaned AS 
        SELECT * FROM users 
        WHERE rowid IN (
            SELECT MIN(rowid) 
            FROM users 
            GROUP BY state, createdDate_$date, role, lastLogin_$date, _id_$oid, active, signUpSource
        )
    """
}

# Execute SQL queries to remove duplicates
for table, query in remove_duplicates_queries.items():
    conn.execute(query)

# Load cleaned data back into pandas DataFrames
df_brands_cleaned = pd.read_sql("SELECT * FROM brands_cleaned", conn)
df_receipts_cleaned = pd.read_sql("SELECT * FROM receipts_cleaned", conn)
df_users_cleaned = pd.read_sql("SELECT * FROM users_cleaned", conn)

# Close the database connection
conn.close()

# Save cleaned data back to CSV files (optional)
df_brands_cleaned.to_csv("brands_cleaned.csv", index=False)
df_receipts_cleaned.to_csv("receipts_cleaned.csv", index=False)
df_users_cleaned.to_csv("users_cleaned.csv", index=False)

# Print results
print("Duplicates removed. Cleaned data saved to CSV files.")

```

    Duplicates removed. Cleaned data saved to CSV files.



```python
import pandas as pd
import sqlite3

# Load CSV files into pandas DataFrames
brands_file = "/Users/funsize/fetch-data-modeling-exercise/data/brands_cleaned.csv"
receipts_file = "/Users/funsize/fetch-data-modeling-exercise/data/receipts_cleaned.csv"
users_file = "/Users/funsize/fetch-data-modeling-exercise/data/users_cleaned.csv"

# Create an in-memory SQLite database
conn = sqlite3.connect(":memory:")

# Load DataFrames into SQL tables
df_brands.to_sql("brands", conn, index=False, if_exists="replace")
df_receipts.to_sql("receipts", conn, index=False, if_exists="replace")
df_users.to_sql("users", conn, index=False, if_exists="replace")

# Identify potential date columns in each dataset
date_columns = {
    "receipts": ["modifyDate_$date", "finishedDate_$date", "pointsAwardedDate_$date",
                 "purchaseDate_$date", "dateScanned_$date", "createDate_$date"],
    "users": ["createdDate_$date", "lastLogin_$date"]
}

# Convert Unix timestamps to proper DATETIME format
for table, columns in date_columns.items():
    for col in columns:
        query = f"""
            UPDATE {table} 
            SET {col} = datetime({col} / 1000, 'unixepoch')
            WHERE {col} IS NOT NULL
        """
        conn.execute(query)

# Verify the changes by selecting a few rows separately from each table
verify_receipts_query = "SELECT 'receipts' AS table_name, purchaseDate_$date AS sample_date FROM receipts LIMIT 5"
verify_users_query = "SELECT 'users' AS table_name, createdDate_$date AS sample_date FROM users LIMIT 5"

# Fetch the converted date data
converted_receipts_df = pd.read_sql(verify_receipts_query, conn)
converted_users_df = pd.read_sql(verify_users_query, conn)

# Combine results
converted_dates_df = pd.concat([converted_receipts_df, converted_users_df], ignore_index=True)

# Close the database connection
conn.close()

# Print results
print("Date conversion completed. Sample converted dates:")
print(converted_dates_df)

```

    Date conversion completed. Sample converted dates:
      table_name          sample_date
    0   receipts  2021-01-03 00:00:00
    1   receipts  2021-01-02 15:24:43
    2   receipts  2021-01-03 00:00:00
    3   receipts  2021-01-03 00:00:00
    4   receipts  2021-01-02 15:25:06
    5      users  2021-01-03 15:24:04
    6      users  2021-01-03 15:24:04
    7      users  2021-01-03 15:25:30
    8      users  2021-01-03 15:24:04
    9      users  2021-01-03 15:24:04



```python
df_receipts_cleaned.to_csv("receipts_cleaned.csv", index=False)
df_users_cleaned.to_csv("users_cleaned.csv", index=False)

```


```python
#What are the top 5 brands by receipts scanned for most recent month?
```


```python
# File path for receipts data
receipts_file = "/Users/funsize/fetch-data-modeling-exercise/data/receipts_cleaned.csv"

# Load CSV without parsing dates to inspect column names
df = pd.read_csv(receipts_file)

# Print all column names
print("Columns in receipts CSV:", df.columns.tolist())

```

    Columns in receipts CSV: ['bonusPointsEarned', 'modifyDate_$date', 'purchasedItemCount', 'rewardsReceiptItemList', 'bonusPointsEarnedReason', 'finishedDate_$date', 'pointsAwardedDate_$date', 'purchaseDate_$date', 'userId', '_id_$oid', 'rewardsReceiptStatus', 'totalSpent', 'dateScanned_$date', 'pointsEarned', 'createDate_$date']



```python
# Load CSV files into pandas DataFrames
brands_file = "/Users/funsize/fetch-data-modeling-exercise/data/brands_cleaned.csv"
receipts_file = "/Users/funsize/fetch-data-modeling-exercise/data/receipts_cleaned.csv"
users_file = "/Users/funsize/fetch-data-modeling-exercise/data/users_cleaned.csv"

# Create an in-memory SQLite database
conn = sqlite3.connect(":memory:")

# Load DataFrames into SQL tables
df_brands.to_sql("brands", conn, index=False, if_exists="replace")
df_receipts.to_sql("receipts", conn, index=False, if_exists="replace")
df_users.to_sql("users", conn, index=False, if_exists="replace")


# Check columns in receipts table
print(pd.read_sql_query("PRAGMA table_info(receipts);", conn))
# Check columns in brands table
print(pd.read_sql_query("PRAGMA table_info(brands);", conn))
print(pd.read_sql_query("PRAGMA table_info(users);", conn))

```

        cid                     name     type  notnull dflt_value  pk
    0     0        bonusPointsEarned     TEXT        0       None   0
    1     1         modifyDate_$date  INTEGER        0       None   0
    2     2       purchasedItemCount     TEXT        0       None   0
    3     3   rewardsReceiptItemList     TEXT        0       None   0
    4     4  bonusPointsEarnedReason     TEXT        0       None   0
    5     5       finishedDate_$date     TEXT        0       None   0
    6     6  pointsAwardedDate_$date     TEXT        0       None   0
    7     7       purchaseDate_$date     TEXT        0       None   0
    8     8                   userId     TEXT        0       None   0
    9     9                 _id_$oid     TEXT        0       None   0
    10   10     rewardsReceiptStatus     TEXT        0       None   0
    11   11               totalSpent     TEXT        0       None   0
    12   12        dateScanned_$date  INTEGER        0       None   0
    13   13             pointsEarned     TEXT        0       None   0
    14   14         createDate_$date  INTEGER        0       None   0
       cid          name     type  notnull dflt_value  pk
    0    0      category     TEXT        0       None   0
    1    1  cpg_$id_$oid     TEXT        0       None   0
    2    2       barcode  INTEGER        0       None   0
    3    3      topBrand     TEXT        0       None   0
    4    4  categoryCode     TEXT        0       None   0
    5    5      _id_$oid     TEXT        0       None   0
    6    6      cpg_$ref     TEXT        0       None   0
    7    7          name     TEXT        0       None   0
    8    8     brandCode     TEXT        0       None   0
       cid               name     type  notnull dflt_value  pk
    0    0              state     TEXT        0       None   0
    1    1  createdDate_$date  INTEGER        0       None   0
    2    2               role     TEXT        0       None   0
    3    3    lastLogin_$date     TEXT        0       None   0
    4    4           _id_$oid     TEXT        0       None   0
    5    5             active  INTEGER        0       None   0
    6    6       signUpSource     TEXT        0       None   0



```python
print(receipts_df[['rewardsReceiptItemList', 'brandCode']].dropna().head(10))

```

                                    rewardsReceiptItemList          brandCode
    6    [{"brandCode": "MISSION", "competitorRewardsGr...            MISSION
    7    [{"barcode": "046000832517", "brandCode": "BRA...              BRAND
    47   [{"barcode": "044000000745", "brandCode": "KRA...  KRAFT EASY CHEESE
    123  [{"brandCode": "WINGSTOP", "description": "12 ...           WINGSTOP
    123  [{"brandCode": "WINGSTOP", "description": "12 ...           WINGSTOP
    125  [{"barcode": "021908236865", "brandCode": "BRA...              BRAND
    152  [{"barcode": "016000156234", "brandCode": "BRA...              BRAND
    159  [{"barcode": "016000156234", "brandCode": "BRA...              BRAND
    166  [{"barcode": "305210154278", "brandCode": "BRA...              BRAND
    171  [{"barcode": "001111132666", "brandCode": "BRA...              BRAND



```python
# Standardize brandCode to uppercase for consistency
receipts_df['brandCode'] = receipts_df['brandCode'].str.upper()
brands_df['brandCode'] = brands_df['brandCode'].str.upper()

```


```python
# Print unique brand codes in both tables
print("Receipts brand codes:", receipts_df['brandCode'].dropna().unique()[:10])
print("Brands brand codes:", brands_df['brandCode'].dropna().unique()[:10])

# Find common brand codes
common_brands = set(receipts_df['brandCode']).intersection(set(brands_df['brandCode']))
print("Matching Brand Codes:", common_brands)

```

    Receipts brand codes: ['MISSION' 'BRAND' 'KRAFT EASY CHEESE' 'WINGSTOP' 'GERM-X' 'ORAL-B GLIDE'
     'KLONDIKE' 'PEPSI' 'DORITOS' 'KNORR']
    Brands brand codes: ['UNKNOWN' 'STARBUCKS' 'TEST BRANDCODE @1612366146176'
     'TEST BRANDCODE @1612366146051' 'TEST BRANDCODE @1612366146827'
     'TEST BRANDCODE @1612366146091' 'TEST BRANDCODE @1612366146133'
     'J.L. KRAFT' 'CAMPBELLS HOME STYLE' 'TEST']
    Matching Brand Codes: {'KLONDIKE', 'CRACKER BARREL', 'PHILADELPHIA', 'VIVA', 'HUGGIES', 'PEPSI', 'DORITOS', 'KLEENEX', 'QUAKER', 'KNORR'}



```python
print("Min purchase date:", receipts_df["purchaseDate_$date"].min())
print("Max purchase date:", receipts_df["purchaseDate_$date"].max())
print(receipts_df[['purchaseDate_$date']].dropna().head(10))

```

    Min purchase date: 1609027200000.0
    Max purchase date: 1613052020000.0
        purchaseDate_$date
    6      1609687501000.0
    7      1609027200000.0
    47     1609632000000.0
    123    1609977600000.0
    123    1609977600000.0
    125    1609372800000.0
    152    1609459200000.0
    159    1609459200000.0
    166    1609459200000.0
    171    1609459200000.0



```python
# Convert Unix timestamp (milliseconds) to datetime
receipts_df['purchaseDate_$date'] = pd.to_datetime(receipts_df['purchaseDate_$date'], unit='ms')

# Verify the conversion
print("Converted Min Date:", receipts_df["purchaseDate_$date"].min())
print("Converted Max Date:", receipts_df["purchaseDate_$date"].max())
print(receipts_df[['purchaseDate_$date']].head(10))

```

    Converted Min Date: 2020-12-27 00:00:00
    Converted Max Date: 2021-02-11 14:00:20
         purchaseDate_$date
    6   2021-01-03 15:25:01
    7   2020-12-27 00:00:00
    47  2021-01-03 00:00:00
    123 2021-01-07 00:00:00
    123 2021-01-07 00:00:00
    125 2020-12-31 00:00:00
    152 2021-01-01 00:00:00
    159 2021-01-01 00:00:00
    166 2021-01-01 00:00:00
    171 2021-01-01 00:00:00



```python
import sqlite3
import pandas as pd
import json
import re

# Connect to SQLite database
conn = sqlite3.connect("fetch_data.db")
cursor = conn.cursor()

# Fetch column names from receipts table
receipts_columns = pd.read_sql_query("PRAGMA table_info(receipts);", conn)
brands_columns = pd.read_sql_query("PRAGMA table_info(brands);", conn)

# Custom column name mapping (Modify these as needed)
column_name_mapping = {
    "purchaseDate_$date": "purchase_date",
    "_id_$oid": "receipt_id",
    "brandCode": "brand_code",
    "rewardsReceiptItemList": "receipt_items",
    "modifyDate_$date": "modify_date",
    "finishedDate_$date": "finished_date",
    "pointsAwardedDate_$date": "points_awarded_date",
    "userId": "user_id",
    "rewardsReceiptStatus": "receipt_status",
    "totalSpent": "total_spent",
    "dateScanned_$date": "date_scanned",
    "pointsEarned": "points_earned",
    "createDate_$date": "create_date",
}

# Normalize column names function (for any unmapped columns)
def normalize_column_name(column_name):
    column_name = column_name.lower().strip()  # Convert to lowercase, remove spaces
    column_name = re.sub(r'[^a-z0-9]', '_', column_name)  # Replace non-alphanumeric with underscores
    column_name = re.sub(r'_+', '_', column_name)  # Replace multiple underscores with a single one
    return column_name

# Apply custom column name mapping
def get_new_column_name(old_name):
    return column_name_mapping.get(old_name, normalize_column_name(old_name))

# Add new column names to receipts DataFrame
receipts_columns["new_name"] = receipts_columns["name"].apply(get_new_column_name)

# Add new column names to brands DataFrame
brands_columns["new_name"] = brands_columns["name"].apply(get_new_column_name)

# Print new column mappings
print("Updated Receipts Table Columns:")
print(receipts_columns[['name', 'new_name']])

print("\nUpdated Brands Table Columns:")
print(brands_columns[['name', 'new_name']])

```

    Updated Receipts Table Columns:
                           name                 new_name
    0         bonusPointsEarned        bonuspointsearned
    1          modifyDate_$date              modify_date
    2        purchasedItemCount       purchaseditemcount
    3    rewardsReceiptItemList            receipt_items
    4   bonusPointsEarnedReason  bonuspointsearnedreason
    5        finishedDate_$date            finished_date
    6   pointsAwardedDate_$date      points_awarded_date
    7        purchaseDate_$date            purchase_date
    8                    userId                  user_id
    9                  _id_$oid               receipt_id
    10     rewardsReceiptStatus           receipt_status
    11               totalSpent              total_spent
    12        dateScanned_$date             date_scanned
    13             pointsEarned            points_earned
    14         createDate_$date              create_date
    15                brandCode               brand_code
    
    Updated Brands Table Columns:
               name      new_name
    0      category      category
    1  cpg_$id_$oid    cpg_id_oid
    2       barcode       barcode
    3      topBrand      topbrand
    4  categoryCode  categorycode
    5      _id_$oid    receipt_id
    6      cpg_$ref       cpg_ref
    7          name          name
    8     brandCode    brand_code



```python
# Function to generate SQL for renaming tables
def generate_rename_sql(table_name, df_columns):
    old_columns = df_columns['name'].tolist()
    new_columns = df_columns['new_name'].tolist()
    
    # Create column definitions
    column_definitions = ", ".join([f'"{new}" TEXT' for new in new_columns])

    # Create a new table with updated column names
    create_sql = f'CREATE TABLE {table_name}_new ({column_definitions});'
    
    # Copy data from old table to new table
    insert_sql = f'INSERT INTO {table_name}_new ({", ".join(new_columns)}) SELECT {", ".join(old_columns)} FROM {table_name};'
    
    # Drop the old table
    drop_sql = f'DROP TABLE {table_name};'
    
    # Rename the new table to original table name
    rename_sql = f'ALTER TABLE {table_name}_new RENAME TO {table_name};'
    
    return create_sql, insert_sql, drop_sql, rename_sql

# Generate SQL for renaming receipts table
receipts_sql = generate_rename_sql("receipts", receipts_columns)

# Generate SQL for renaming brands table
brands_sql = generate_rename_sql("brands", brands_columns)

# Execute the renaming process in SQLite
for sql in receipts_sql:
    cursor.execute(sql)
for sql in brands_sql:
    cursor.execute(sql)

# Commit changes and close connection
conn.commit()
conn.close()

print("✅ Column names have been updated successfully in SQLite!")

```

    ✅ Column names have been updated successfully in SQLite!



```python
conn = sqlite3.connect("fetch_data.db")
print(pd.read_sql_query("PRAGMA table_info(receipts);", conn))
print(pd.read_sql_query("PRAGMA table_info(brands);", conn))
conn.close()

```

        cid                     name  type  notnull dflt_value  pk
    0     0        bonuspointsearned  TEXT        0       None   0
    1     1              modify_date  TEXT        0       None   0
    2     2       purchaseditemcount  TEXT        0       None   0
    3     3            receipt_items  TEXT        0       None   0
    4     4  bonuspointsearnedreason  TEXT        0       None   0
    5     5            finished_date  TEXT        0       None   0
    6     6      points_awarded_date  TEXT        0       None   0
    7     7            purchase_date  TEXT        0       None   0
    8     8                  user_id  TEXT        0       None   0
    9     9               receipt_id  TEXT        0       None   0
    10   10           receipt_status  TEXT        0       None   0
    11   11              total_spent  TEXT        0       None   0
    12   12             date_scanned  TEXT        0       None   0
    13   13            points_earned  TEXT        0       None   0
    14   14              create_date  TEXT        0       None   0
    15   15               brand_code  TEXT        0       None   0
       cid          name  type  notnull dflt_value  pk
    0    0      category  TEXT        0       None   0
    1    1    cpg_id_oid  TEXT        0       None   0
    2    2       barcode  TEXT        0       None   0
    3    3      topbrand  TEXT        0       None   0
    4    4  categorycode  TEXT        0       None   0
    5    5    receipt_id  TEXT        0       None   0
    6    6       cpg_ref  TEXT        0       None   0
    7    7          name  TEXT        0       None   0
    8    8    brand_code  TEXT        0       None   0



```python
# Reconnect to SQLite database
conn = sqlite3.connect("fetch_data.db")

# ✅ SQL Query to Get Top 5 Brands by Receipts in the Most Recent Month
query = """
WITH LatestMonth AS (
    SELECT strftime('%Y-%m', MAX(purchase_date)) AS year_month
    FROM receipts
)
SELECT b.name AS brand_name, COUNT(r.receipt_id) AS receipt_count
FROM receipts r
JOIN LatestMonth lm ON strftime('%Y-%m', r.purchase_date) = lm.year_month
JOIN brands b ON UPPER(r.brand_code) = UPPER(b.brand_code)  -- Case-insensitive match
GROUP BY b.name
ORDER BY receipt_count DESC
LIMIT 5;
"""

# ✅ Execute SQL Query
top_5_brands = pd.read_sql_query(query, conn)

# ✅ Close Database Connection
conn.close()

# ✅ Display Results
print("Top 5 Brands by Receipts Scanned:")
print(top_5_brands)

# ✅ Save Results to CSV
top_5_brands.to_csv("top_5_brands.csv", index=False)

```

    Top 5 Brands by Receipts Scanned:
    Empty DataFrame
    Columns: [brand_name, receipt_count]
    Index: []



```python
conn = sqlite3.connect("fetch_data.db")

# ✅ Print available months in the receipts table
print(pd.read_sql_query("SELECT DISTINCT strftime('%Y-%m', purchase_date) FROM receipts ORDER BY 1 DESC;", conn))

conn.close()

```

      strftime('%Y-%m', purchase_date)
    0                             None



```python
conn = sqlite3.connect("fetch_data.db")

# Check column data types
print(pd.read_sql_query("PRAGMA table_info(receipts);", conn))

# Check if `purchase_date` has invalid values
print(pd.read_sql_query("SELECT purchase_date FROM receipts LIMIT 10;", conn))

conn.close()

```

        cid                     name  type  notnull dflt_value  pk
    0     0        bonuspointsearned  TEXT        0       None   0
    1     1              modify_date  TEXT        0       None   0
    2     2       purchaseditemcount  TEXT        0       None   0
    3     3            receipt_items  TEXT        0       None   0
    4     4  bonuspointsearnedreason  TEXT        0       None   0
    5     5            finished_date  TEXT        0       None   0
    6     6      points_awarded_date  TEXT        0       None   0
    7     7            purchase_date  TEXT        0       None   0
    8     8                  user_id  TEXT        0       None   0
    9     9               receipt_id  TEXT        0       None   0
    10   10           receipt_status  TEXT        0       None   0
    11   11              total_spent  TEXT        0       None   0
    12   12             date_scanned  TEXT        0       None   0
    13   13            points_earned  TEXT        0       None   0
    14   14              create_date  TEXT        0       None   0
    15   15               brand_code  TEXT        0       None   0
         purchase_date
    0  1609687501000.0
    1  1609027200000.0
    2  1609632000000.0
    3  1609977600000.0
    4  1609977600000.0
    5  1609372800000.0
    6  1609459200000.0
    7  1609459200000.0
    8  1609459200000.0
    9  1609459200000.0



```python
conn = sqlite3.connect("fetch_data.db")
cursor = conn.cursor()

# Convert Unix timestamp (milliseconds) to a proper date
cursor.execute("""
    UPDATE receipts 
    SET purchase_date = strftime('%Y-%m-%d', purchase_date / 1000, 'unixepoch')
    WHERE purchase_date LIKE '1%';  -- Only apply to Unix timestamps
""")

conn.commit()
conn.close()

print("✅ Fixed purchase_date format in SQLite!")

```

    ✅ Fixed purchase_date format in SQLite!



```python
conn = sqlite3.connect("fetch_data.db")

# Check available purchase months after conversion
print(pd.read_sql_query("SELECT DISTINCT strftime('%Y-%m', purchase_date) FROM receipts ORDER BY 1 DESC;", conn))

conn.close()

```

      strftime('%Y-%m', purchase_date)
    0                          2021-02
    1                          2021-01
    2                          2020-12



```python
conn = sqlite3.connect("fetch_data.db")

query = """
WITH LatestMonth AS (
    SELECT strftime('%Y-%m', MAX(purchase_date)) AS year_month
    FROM receipts
)
SELECT b.name AS brand_name, COUNT(r.receipt_id) AS receipt_count
FROM receipts r
JOIN LatestMonth lm ON strftime('%Y-%m', r.purchase_date) = lm.year_month
JOIN brands b ON UPPER(r.brand_code) = UPPER(b.brand_code)
GROUP BY b.name
ORDER BY receipt_count DESC
LIMIT 5;
"""

top_5_brands = pd.read_sql_query(query, conn)
conn.close()

# ✅ Print and save the results
print("Top 5 Brands by Receipts Scanned:")
print(top_5_brands)
top_5_brands.to_csv("top_5_brands.csv", index=False)

```

    Top 5 Brands by Receipts Scanned:
      brand_name  receipt_count
    0       Viva              1



```python
conn = sqlite3.connect("fetch_data.db")

# ✅ Print available purchase months
print(pd.read_sql_query("SELECT DISTINCT strftime('%Y-%m', purchase_date) FROM receipts ORDER BY 1 DESC;", conn))

conn.close()

```

      strftime('%Y-%m', purchase_date)
    0                          2021-02
    1                          2021-01
    2                          2020-12



```python
#Expands analysis to 3 months instead of only 1.
#If February has too few receipts, it includes January and December too.
#Still keeps the month-by-month comparison.

#How does the ranking of the top 5 brands by receipts scanned for the recent 
#month compare to the ranking for the previous month?
```


```python
import sqlite3
import pandas as pd

# Connect to SQLite database
conn = sqlite3.connect("fetch_data.db")

query = """
WITH MonthlyReceipts AS (
    SELECT 
        strftime('%Y-%m', purchase_date) AS month,
        b.name AS brand_name,
        COUNT(r.receipt_id) AS receipt_count
    FROM receipts r
    JOIN brands b ON UPPER(r.brand_code) = UPPER(b.brand_code)
    GROUP BY month, b.name
),
RecentMonth AS (
    SELECT month FROM MonthlyReceipts ORDER BY month DESC LIMIT 1
),
PreviousMonth AS (
    SELECT month FROM MonthlyReceipts WHERE month < (SELECT month FROM RecentMonth) ORDER BY month DESC LIMIT 1
)
SELECT 
    mr.month, 
    mr.brand_name, 
    mr.receipt_count,
    CASE 
        WHEN mr.month = (SELECT month FROM RecentMonth) THEN 'Recent Month'
        WHEN mr.month = (SELECT month FROM PreviousMonth) THEN 'Previous Month'
    END AS month_type
FROM MonthlyReceipts mr
WHERE mr.month IN ((SELECT month FROM RecentMonth), (SELECT month FROM PreviousMonth))
ORDER BY month_type, receipt_count DESC
LIMIT 10;
"""

# Execute SQL Query
top_5_comparison = pd.read_sql_query(query, conn)

# Close Database Connection
conn.close()

# ✅ Print and save results
print("Comparison of Top 5 Brands by Receipts Scanned for Recent vs. Previous Month:")
print(top_5_comparison)

# Save to CSV for analysis
top_5_comparison.to_csv("top_5_brands_comparison.csv", index=False)

```

    Comparison of Top 5 Brands by Receipts Scanned for Recent vs. Previous Month:
         month             brand_name  receipt_count      month_type
    0  2021-01                Huggies             12  Previous Month
    1  2021-01                  Pepsi              3  Previous Month
    2  2021-01                Doritos              2  Previous Month
    3  2021-01                  KNORR              2  Previous Month
    4  2021-01  Cracker Barrel Cheese              1  Previous Month
    5  2021-01               KLONDIKE              1  Previous Month
    6  2021-01                Kleenex              1  Previous Month
    7  2021-01           Philadelphia              1  Previous Month
    8  2021-01                 Quaker              1  Previous Month
    9  2021-02                   Viva              1    Recent Month



```python
#When considering average spend from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater?
```


```python
conn = sqlite3.connect("fetch_data.db")

# ✅ Fetch column names
print(pd.read_sql_query("PRAGMA table_info(receipts);", conn))

conn.close()

```

        cid                     name  type  notnull dflt_value  pk
    0     0        bonuspointsearned  TEXT        0       None   0
    1     1              modify_date  TEXT        0       None   0
    2     2       purchaseditemcount  TEXT        0       None   0
    3     3            receipt_items  TEXT        0       None   0
    4     4  bonuspointsearnedreason  TEXT        0       None   0
    5     5            finished_date  TEXT        0       None   0
    6     6      points_awarded_date  TEXT        0       None   0
    7     7            purchase_date  TEXT        0       None   0
    8     8                  user_id  TEXT        0       None   0
    9     9               receipt_id  TEXT        0       None   0
    10   10           receipt_status  TEXT        0       None   0
    11   11              total_spent  TEXT        0       None   0
    12   12             date_scanned  TEXT        0       None   0
    13   13            points_earned  TEXT        0       None   0
    14   14              create_date  TEXT        0       None   0
    15   15               brand_code  TEXT        0       None   0



```python
conn = sqlite3.connect("fetch_data.db")

# ✅ Print distinct values in `receipt_status`
status_values = pd.read_sql_query("SELECT DISTINCT receipt_status FROM receipts;", conn)

conn.close()

# ✅ Print available statuses
print("Available `receipt_status` values:")
print(status_values)

```

    Available `receipt_status` values:
      receipt_status
    0       FINISHED
    1       REJECTED
    2        FLAGGED
    3        PENDING



```python
import sqlite3
import pandas as pd

# Connect to SQLite database
conn = sqlite3.connect("fetch_data.db")

# ✅ Updated SQL Query using available statuses
query = """
SELECT 
    receipt_status AS status,
    AVG(CAST(total_spent AS REAL)) AS avg_spent
FROM receipts
WHERE receipt_status IN ('FINISHED', 'REJECTED')
GROUP BY receipt_status;
"""

# Execute SQL Query
avg_spend_results = pd.read_sql_query(query, conn)

# Close Database Connection
conn.close()

# ✅ Print Results
print("Comparison of Average Spend for Finished vs. Rejected Receipts:")
print(avg_spend_results)

# ✅ Determine which is greater
if avg_spend_results.shape[0] == 2:
    finished_spend = avg_spend_results.loc[avg_spend_results['status'] == 'FINISHED', 'avg_spent'].values[0]
    rejected_spend = avg_spend_results.loc[avg_spend_results['status'] == 'REJECTED', 'avg_spent'].values[0]

    if finished_spend > rejected_spend:
        print(f"✅ Finished receipts have a higher average spend (${finished_spend:.2f}) than Rejected receipts (${rejected_spend:.2f}).")
    elif rejected_spend > finished_spend:
        print(f"✅ Rejected receipts have a higher average spend (${rejected_spend:.2f}) than Finished receipts (${finished_spend:.2f}).")
    else:
        print(f"✅ The average spend is the same for both Finished and Rejected receipts (${finished_spend:.2f}).")
else:
    print("⚠️ Not enough data available to compare Finished vs. Rejected receipts.")

```

    Comparison of Average Spend for Finished vs. Rejected Receipts:
         status  avg_spent
    0  FINISHED  36.915469
    1  REJECTED   2.290000
    ✅ Finished receipts have a higher average spend ($36.92) than Rejected receipts ($2.29).



```python
#When considering total number of items purchased from receipts with 'rewardsReceiptStatus’
#of ‘Accepted’ or ‘Rejected’, which is greater?
```


```python
import sqlite3
import pandas as pd

# Connect to SQLite database
conn = sqlite3.connect("fetch_data.db")

# ✅ Updated SQL Query using available statuses
query = """
SELECT 
    receipt_status AS status,
    SUM(CAST(purchaseditemcount AS INTEGER)) AS total_items
FROM receipts
WHERE receipt_status IN ('FINISHED', 'REJECTED')
GROUP BY receipt_status;
"""

# Execute SQL Query
total_items_results = pd.read_sql_query(query, conn)

# Close Database Connection
conn.close()

# ✅ Print Results
print("Comparison of Total Items Purchased for Finished vs. Rejected Receipts:")
print(total_items_results)

# ✅ Determine which is greater
if total_items_results.shape[0] == 2:
    finished_items = total_items_results.loc[total_items_results['status'] == 'FINISHED', 'total_items'].values[0]
    rejected_items = total_items_results.loc[total_items_results['status'] == 'REJECTED', 'total_items'].values[0]

    if finished_items > rejected_items:
        print(f"✅ Finished receipts have more total items purchased ({finished_items}) than Rejected receipts ({rejected_items}).")
    elif rejected_items > finished_items:
        print(f"✅ Rejected receipts have more total items purchased ({rejected_items}) than Finished receipts ({finished_items}).")
    else:
        print(f"✅ The total number of items purchased is the same for both Finished and Rejected receipts ({finished_items}).")
else:
    print("⚠️ Not enough data available to compare Finished vs. Rejected receipts.")

```

    Comparison of Total Items Purchased for Finished vs. Rejected Receipts:
         status  total_items
    0  FINISHED          453
    1  REJECTED            1
    ✅ Finished receipts have more total items purchased (453) than Rejected receipts (1).



```python
#Which brand has the most spend among users who were created within the past 6 months?
```


```python
import pandas as pd

# File path for users CSV
users_file = "/Users/funsize/fetch-data-modeling-exercise/data/users_cleaned.csv"

# ✅ Load Users CSV (Only first 5 rows)
users_df = pd.read_csv(users_file)

# ✅ Print column names
print("Available columns in users_cleaned.csv:")
print(users_df.columns)

```

    Available columns in users_cleaned.csv:
    Index(['state', 'createdDate_$date', 'role', 'lastLogin_$date', '_id_$oid',
           'active', 'signUpSource'],
          dtype='object')



```python
import pandas as pd
import sqlite3

# File path for users CSV
users_file = "/Users/funsize/fetch-data-modeling-exercise/data/users_cleaned.csv"

# ✅ Load Users CSV
users_df = pd.read_csv(users_file)

# ✅ Rename column
users_df.rename(columns={'createdDate_$date': 'created_date'}, inplace=True)

# ✅ Convert `created_date` to proper date format (Unix timestamp check)
if users_df['created_date'].dtype != 'datetime64[ns]':  # If not already datetime
    users_df['created_date'] = pd.to_datetime(users_df['created_date'], unit='ms', errors='coerce')

# ✅ Save to SQLite
conn = sqlite3.connect("fetch_data.db")
users_df.to_sql("users", conn, if_exists="replace", index=False)
conn.close()

print("✅ Users table successfully added to SQLite with corrected `created_date` format!")

```

    ✅ Users table successfully added to SQLite with corrected `created_date` format!



```python
conn = sqlite3.connect("fetch_data.db")

# ✅ Check available tables again
tables = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table';", conn)

# ✅ Get column names for `users` table
users_columns = pd.read_sql_query("PRAGMA table_info(users);", conn)

conn.close()

print("Available Tables in Database:")
print(tables)

print("\nUsers Table Columns:")
print(users_columns)

```

    Available Tables in Database:
           name
    0  receipts
    1    brands
    2     users
    
    Users Table Columns:
       cid             name       type  notnull dflt_value  pk
    0    0            state       TEXT        0       None   0
    1    1     created_date  TIMESTAMP        0       None   0
    2    2             role       TEXT        0       None   0
    3    3  lastLogin_$date       TEXT        0       None   0
    4    4         _id_$oid       TEXT        0       None   0
    5    5           active    INTEGER        0       None   0
    6    6     signUpSource       TEXT        0       None   0



```python
conn = sqlite3.connect("fetch_data.db")

query = """
WITH RecentUsers AS (
    SELECT _id_$oid AS user_id
    FROM users 
    WHERE created_date >= DATE('now', '-6 months')  -- Users created within the last 6 months
)
SELECT 
    b.name AS brand_name,
    SUM(CAST(r.total_spent AS REAL)) AS total_spent
FROM receipts r
JOIN RecentUsers u ON r.user_id = u.user_id
JOIN brands b ON r.brand_code = b.brand_code
GROUP BY b.name
ORDER BY total_spent DESC
LIMIT 1;  -- Get the top brand
"""

top_spend_brand = pd.read_sql_query(query, conn)

conn.close()

print("Brand with the Highest Spend Among Recently Created Users:")
print(top_spend_brand)

```

    Brand with the Highest Spend Among Recently Created Users:
    Empty DataFrame
    Columns: [brand_name, total_spent]
    Index: []



```python
#user creation dates range from December 19, 2014 to February 12, 2021.
#This means no users exist from the last 6 months (relative to today's date).

#Since recent_users_count = 0, the query is returning an empty DataFrame because no users qualify for the filter.
```


```python
import sqlite3
import pandas as pd

conn = sqlite3.connect("fetch_data.db")

# ✅ Check min and max dates to confirm date range
print(pd.read_sql_query("SELECT MIN(created_date), MAX(created_date) FROM users;", conn))

# ✅ Check how many users exist in the last 6 months
print(pd.read_sql_query("""
    SELECT COUNT(*) AS recent_users_count
    FROM users
    WHERE created_date >= DATE('now', '-6 months');
""", conn))

conn.close()

```

                MIN(created_date)           MAX(created_date)
    0  2014-12-19 14:21:22.381000  2021-02-12 14:11:06.240000
       recent_users_count
    0                   0



```python
import sqlite3
import pandas as pd

# Connect to SQLite database
conn = sqlite3.connect("fetch_data.db")

# ✅ Updated SQL Query
query = """
WITH RecentUsers AS (
    SELECT _id_$oid AS user_id
    FROM users 
    WHERE created_date >= DATE((SELECT MAX(created_date) FROM users), '-6 months')  -- Last 6 months from latest user
)
SELECT 
    b.name AS brand_name,
    COUNT(r.receipt_id) AS total_transactions
FROM receipts r
JOIN RecentUsers u ON TRIM(r.user_id) = TRIM(u.user_id)  -- Ensure user IDs match
JOIN brands b ON r.brand_code = b.brand_code
GROUP BY b.name
ORDER BY total_transactions DESC
LIMIT 1;  -- Get the top brand
"""

# Execute SQL Query
top_transaction_brand = pd.read_sql_query(query, conn)

# Close Database Connection
conn.close()

# ✅ Print Results
print("Brand with the Most Transactions Among Recently Created Users:")
print(top_transaction_brand)

# ✅ Save to CSV
top_transaction_brand.to_csv("top_transaction_brand_last_6_months.csv", index=False)

```

    Brand with the Most Transactions Among Recently Created Users:
      brand_name  total_transactions
    0      Pepsi                   4



```python

```
