```python
import os

directory = "/Users/funsize/fetch-data-modeling-exercise/data"

files = [f for f in os.listdir(directory) if not f.startswith('.')]
print("Files in directory:", files)

```

    Files in directory: ['receipts.json.gz', 'users.json.gz', 'brands.json.gz']



```python
#unzip the gz files
```


```python
import gzip
import shutil

# List only .gz files in the directory
gz_files = [f for f in os.listdir(directory) if f.endswith('.gz')]

# Unzip each file
for gz_file in gz_files:
    gz_path = os.path.join(directory, gz_file)
    output_path = os.path.join(directory, gz_file.replace('.gz', ''))

    with gzip.open(gz_path, 'rb') as f_in:
        with open(output_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    print(f"Unzipped: {gz_file} -> {output_path}")

```

    Unzipped: receipts.json.gz -> /Users/funsize/fetch-data-modeling-exercise/data/receipts.json
    Unzipped: users.json.gz -> /Users/funsize/fetch-data-modeling-exercise/data/users.json
    Unzipped: brands.json.gz -> /Users/funsize/fetch-data-modeling-exercise/data/brands.json



```python
#clean and validate the json files
```


```python
import json

json_files = ["receipts.json", "users.json", "brands.json"]

def clean_json(file_path):
    cleaned_data = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                record = json.loads(line.strip())  # Try parsing each line as JSON
                cleaned_data.append(record)
            except json.JSONDecodeError:
                print(f"Skipping invalid JSON in {file_path}: {line[:100]}...")  # Print partial invalid line for debugging

    return cleaned_data

# Process and clean each JSON file
for json_file in json_files:
    file_path = os.path.join(directory, json_file)
    cleaned_data = clean_json(file_path)
    
    # Save cleaned data to a new file
    clean_file_path = file_path.replace('.json', '_cleaned.json')
    with open(clean_file_path, 'w', encoding='utf-8') as f:
        json.dump(cleaned_data, f, indent=4)

    print(f"Cleaned data saved to: {clean_file_path}")

```

    Cleaned data saved to: /Users/funsize/fetch-data-modeling-exercise/data/receipts_cleaned.json
    Skipping invalid JSON in /Users/funsize/fetch-data-modeling-exercise/data/users.json: users.json                                                                                          ...
    Skipping invalid JSON in /Users/funsize/fetch-data-modeling-exercise/data/users.json:                                                                                                     ...
    Cleaned data saved to: /Users/funsize/fetch-data-modeling-exercise/data/users_cleaned.json
    Cleaned data saved to: /Users/funsize/fetch-data-modeling-exercise/data/brands_cleaned.json



```python
# Checklist Before Converting JSON to DataFrame
Consistent Structure – Ensure all JSON records follow a uniform schema (e.g., same keys in all records).
Nested Data Handling – If there are deeply nested fields, decide whether to flatten them.
Handling Missing/Null Values – Identify missing or malformed values.
File Size Consideration – If the files are too large, consider using chunk-based reading.
```


```python
# Normalize JSON Schema
```


```python
import os
import json
import pandas as pd

directory = "/Users/funsize/fetch-data-modeling-exercise/data"
json_files = ["receipts_cleaned.json", "users_cleaned.json", "brands_cleaned.json"]

def flatten_json(nested_json, parent_key='', sep='_'):
    """Flattens nested dictionaries into a single-level dictionary."""
    flattened_dict = {}
    for key, value in nested_json.items():
        new_key = f"{parent_key}{sep}{key}" if parent_key else key
        if isinstance(value, dict):  # Recursive flattening for dictionaries
            flattened_dict.update(flatten_json(value, new_key, sep=sep))
        elif isinstance(value, list):
            # Convert lists to JSON strings (or handle lists differently if needed)
            flattened_dict[new_key] = json.dumps(value) if len(value) > 0 else None
        else:
            flattened_dict[new_key] = value
    return flattened_dict

def process_json_file(file_path, chunk_size=1000):
    """Processes JSON file while ensuring schema consistency, flattening, and handling missing values."""
    all_keys = set()
    structured_data = []

    # Read file in chunks to handle large files
    with open(file_path, 'r', encoding='utf-8') as f:
        try:
            data = json.load(f)  # Load entire JSON if possible
        except json.JSONDecodeError:
            print(f"Error decoding {file_path}. Skipping malformed file.")
            return pd.DataFrame()  # Return an empty DataFrame if file is corrupt

    # Collect all possible keys (including flattened ones)
    for record in data[:chunk_size]:  # Only scan first chunk to determine schema
        if isinstance(record, dict):  
            flattened_record = flatten_json(record)
            all_keys.update(flattened_record.keys())

    # Normalize records by filling missing keys and handling null values
    for record in data:
        if isinstance(record, dict):
            flattened_record = flatten_json(record)
            # Ensure all keys exist in each record
            structured_record = {key: flattened_record.get(key, None) for key in all_keys}
            structured_data.append(structured_record)

    # Convert structured data into a Pandas DataFrame
    df = pd.DataFrame(structured_data)
    
    # Handling missing values - (Optionally, replace `None` with default values)
    df.fillna(value={"example_column": "Unknown"}, inplace=True)  # Customize for specific columns

    return df

dataframes = {}

# Process each JSON file
for json_file in json_files:
    file_path = os.path.join(directory, json_file)
    
    # Process and load into DataFrame
    df = process_json_file(file_path)
    dataframes[json_file] = df  # Store DataFrame

    print(f"Processed {json_file}: {df.shape[0]} records, {df.shape[1]} columns")
    print(f"Schema Sample: {df.columns.tolist()[:10]}\n")  # Print first 10 columns

# Example: Access a specific DataFrame
receipts_df = dataframes["receipts_cleaned.json"]
users_df = dataframes["users_cleaned.json"]
brands_df = dataframes["brands_cleaned.json"]

```

    Processed receipts_cleaned.json: 1119 records, 15 columns
    Schema Sample: ['bonusPointsEarned', 'modifyDate_$date', 'purchasedItemCount', 'rewardsReceiptItemList', 'bonusPointsEarnedReason', 'finishedDate_$date', 'pointsAwardedDate_$date', 'purchaseDate_$date', 'userId', '_id_$oid']
    
    Processed users_cleaned.json: 494 records, 7 columns
    Schema Sample: ['state', 'createdDate_$date', 'role', 'lastLogin_$date', '_id_$oid', 'active', 'signUpSource']
    
    Processed brands_cleaned.json: 1167 records, 9 columns
    Schema Sample: ['category', 'cpg_$id_$oid', 'barcode', 'topBrand', 'categoryCode', '_id_$oid', 'cpg_$ref', 'name', 'brandCode']
    



```python
# Convert Cleaned JSON to Pandas DataFrames
```


```python
json_files = ["receipts_cleaned.json", "users_cleaned.json", "brands_cleaned.json"]

dataframes = {}

# Convert JSON files to Pandas DataFrames
for json_file in json_files:
    file_path = os.path.join(directory, json_file)

    # Load JSON file
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)  # Load entire JSON data as a list of dictionaries

    # Convert to DataFrame
    df = pd.DataFrame(data)
    dataframes[json_file] = df  # Store in dictionary for easy access

    # Display the first few rows for verification
    print(f"Loaded {json_file} into DataFrame with shape {df.shape}")
    print(df.head())

# Example: Accessing a specific DataFrame
receipts_df = dataframes["receipts_cleaned.json"]
users_df = dataframes["users_cleaned.json"]
brands_df = dataframes["brands_cleaned.json"]

```

    Loaded receipts_cleaned.json into DataFrame with shape (1119, 15)
                                        _id  bonusPointsEarned  \
    0  {'$oid': '5ff1e1eb0a720f0523000575'}              500.0   
    1  {'$oid': '5ff1e1bb0a720f052300056b'}              150.0   
    2  {'$oid': '5ff1e1f10a720f052300057a'}                5.0   
    3  {'$oid': '5ff1e1ee0a7214ada100056f'}                5.0   
    4  {'$oid': '5ff1e1d20a7214ada1000561'}                5.0   
    
                                 bonusPointsEarnedReason  \
    0  Receipt number 2 completed, bonus point schedu...   
    1  Receipt number 5 completed, bonus point schedu...   
    2                         All-receipts receipt bonus   
    3                         All-receipts receipt bonus   
    4                         All-receipts receipt bonus   
    
                     createDate               dateScanned  \
    0  {'$date': 1609687531000}  {'$date': 1609687531000}   
    1  {'$date': 1609687483000}  {'$date': 1609687483000}   
    2  {'$date': 1609687537000}  {'$date': 1609687537000}   
    3  {'$date': 1609687534000}  {'$date': 1609687534000}   
    4  {'$date': 1609687506000}  {'$date': 1609687506000}   
    
                   finishedDate                modifyDate  \
    0  {'$date': 1609687531000}  {'$date': 1609687536000}   
    1  {'$date': 1609687483000}  {'$date': 1609687488000}   
    2                       NaN  {'$date': 1609687542000}   
    3  {'$date': 1609687534000}  {'$date': 1609687539000}   
    4  {'$date': 1609687511000}  {'$date': 1609687511000}   
    
              pointsAwardedDate pointsEarned              purchaseDate  \
    0  {'$date': 1609687531000}        500.0  {'$date': 1609632000000}   
    1  {'$date': 1609687483000}        150.0  {'$date': 1609601083000}   
    2                       NaN            5  {'$date': 1609632000000}   
    3  {'$date': 1609687534000}          5.0  {'$date': 1609632000000}   
    4  {'$date': 1609687506000}          5.0  {'$date': 1609601106000}   
    
       purchasedItemCount                             rewardsReceiptItemList  \
    0                 5.0  [{'barcode': '4011', 'description': 'ITEM NOT ...   
    1                 2.0  [{'barcode': '4011', 'description': 'ITEM NOT ...   
    2                 1.0  [{'needsFetchReview': False, 'partnerItemId': ...   
    3                 4.0  [{'barcode': '4011', 'description': 'ITEM NOT ...   
    4                 2.0  [{'barcode': '4011', 'description': 'ITEM NOT ...   
    
      rewardsReceiptStatus totalSpent                    userId  
    0             FINISHED      26.00  5ff1e1eacfcf6c399c274ae6  
    1             FINISHED      11.00  5ff1e194b6a9d73a3a9f1052  
    2             REJECTED      10.00  5ff1e1f1cfcf6c399c274b0b  
    3             FINISHED      28.00  5ff1e1eacfcf6c399c274ae6  
    4             FINISHED       1.00  5ff1e194b6a9d73a3a9f1052  
    Loaded users_cleaned.json into DataFrame with shape (494, 7)
                                        _id  active               createdDate  \
    0  {'$oid': '5ff1e194b6a9d73a3a9f1052'}    True  {'$date': 1609687444800}   
    1  {'$oid': '5ff1e194b6a9d73a3a9f1052'}    True  {'$date': 1609687444800}   
    2  {'$oid': '5ff1e1eacfcf6c399c274ae6'}    True  {'$date': 1609687530554}   
    3  {'$oid': '5ff1e194b6a9d73a3a9f1052'}    True  {'$date': 1609687444800}   
    4  {'$oid': '5ff1e194b6a9d73a3a9f1052'}    True  {'$date': 1609687444800}   
    
                      lastLogin      role signUpSource state  
    0  {'$date': 1609687537858}  consumer        Email    WI  
    1  {'$date': 1609687537858}  consumer        Email    WI  
    2  {'$date': 1609687530597}  consumer        Email    WI  
    3  {'$date': 1609687537858}  consumer        Email    WI  
    4  {'$date': 1609687537858}  consumer        Email    WI  
    Loaded brands_cleaned.json into DataFrame with shape (1167, 8)
                                        _id       barcode        category  \
    0  {'$oid': '601ac115be37ce2ead437551'}  511111019862          Baking   
    1  {'$oid': '601c5460be37ce2ead43755f'}  511111519928       Beverages   
    2  {'$oid': '601ac142be37ce2ead43755d'}  511111819905          Baking   
    3  {'$oid': '601ac142be37ce2ead43755a'}  511111519874          Baking   
    4  {'$oid': '601ac142be37ce2ead43755e'}  511111319917  Candy & Sweets   
    
           categoryCode                                                cpg  \
    0            BAKING  {'$id': {'$oid': '601ac114be37ce2ead437550'}, ...   
    1         BEVERAGES  {'$id': {'$oid': '5332f5fbe4b03c9a25efd0ba'}, ...   
    2            BAKING  {'$id': {'$oid': '601ac142be37ce2ead437559'}, ...   
    3            BAKING  {'$id': {'$oid': '601ac142be37ce2ead437559'}, ...   
    4  CANDY_AND_SWEETS  {'$id': {'$oid': '5332fa12e4b03c9a25efd1e7'}, ...   
    
                            name topBrand                      brandCode  
    0  test brand @1612366101024    False                            NaN  
    1                  Starbucks    False                      STARBUCKS  
    2  test brand @1612366146176    False  TEST BRANDCODE @1612366146176  
    3  test brand @1612366146051    False  TEST BRANDCODE @1612366146051  
    4  test brand @1612366146827    False  TEST BRANDCODE @1612366146827  



```python
json_files = ["receipts_cleaned.json", "users_cleaned.json", "brands_cleaned.json"]

def flatten_json(nested_json, parent_key='', sep='_'):
    """Flattens nested JSON fields into a single-level dictionary."""
    flattened_dict = {}
    for key, value in nested_json.items():
        new_key = f"{parent_key}{sep}{key}" if parent_key else key
        if isinstance(value, dict):  
            flattened_dict.update(flatten_json(value, new_key, sep=sep))
        elif isinstance(value, list):
            flattened_dict[new_key] = json.dumps(value) if len(value) > 0 else None
        else:
            flattened_dict[new_key] = value
    return flattened_dict

def process_json_file(file_path, chunk_size=1000):
    """Processes JSON file while ensuring schema consistency, flattening, and handling missing values."""
    all_keys = set()
    structured_data = []

    with open(file_path, 'r', encoding='utf-8') as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError:
            print(f"Error decoding {file_path}. Skipping malformed file.")
            return pd.DataFrame()  

    for record in data[:chunk_size]:  
        if isinstance(record, dict):  
            flattened_record = flatten_json(record)
            all_keys.update(flattened_record.keys())

    for record in data:
        if isinstance(record, dict):
            flattened_record = flatten_json(record)
            structured_record = {key: flattened_record.get(key, None) for key in all_keys}
            structured_data.append(structured_record)

    df = pd.DataFrame(structured_data)
    df.fillna("Unknown", inplace=True)  # Replace missing values with "Unknown"
    return df

dataframes = {}

# Process and export each JSON file
for json_file in json_files:
    file_path = os.path.join(directory, json_file)

    df = process_json_file(file_path)
    dataframes[json_file] = df  

    # Export to CSV
    csv_file_path = file_path.replace('.json', '.csv')
    df.to_csv(csv_file_path, index=False, encoding='utf-8')

    print(f"✅ Exported {json_file} to {csv_file_path} (Shape: {df.shape})")

# Example: Accessing a specific DataFrame
receipts_df = dataframes["receipts_cleaned.json"]
users_df = dataframes["users_cleaned.json"]
brands_df = dataframes["brands_cleaned.json"]

```

    ✅ Exported receipts_cleaned.json to /Users/funsize/fetch-data-modeling-exercise/data/receipts_cleaned.csv (Shape: (1119, 15))
    ✅ Exported users_cleaned.json to /Users/funsize/fetch-data-modeling-exercise/data/users_cleaned.csv (Shape: (494, 7))
    ✅ Exported brands_cleaned.json to /Users/funsize/fetch-data-modeling-exercise/data/brands_cleaned.csv (Shape: (1167, 9))



```python
#To check data quality for the cleaned CSV files, we need to:

#1.Check missing values (NaN)
#2.Detect duplicate records
#3.Identify outliers (e.g., extreme values in numeric columns)
#4.Validate column types (e.g., dates, numeric fields)

```


```python
csv_files = ["receipts_cleaned.csv", "users_cleaned.csv", "brands_cleaned.csv"]

def check_data_quality(df, file_name):
    """Perform data quality checks and print insights."""
    print(f"\n🔍 Checking data quality for: {file_name}")
    
    # Check for missing values
    missing_values = df.isnull().sum()
    missing_percentage = (missing_values / len(df)) * 100

    # Check for duplicate records
    duplicate_count = df.duplicated().sum()
    
    # Summary statistics
    summary_stats = df.describe(include='all')

    # Check data types
    data_types = df.dtypes

    # Print results
    print(f"\n🔹 Missing Values (Top 10 Columns):")
    print(missing_percentage[missing_percentage > 0].sort_values(ascending=False).head(10))

    print(f"\n🔹 Duplicate Records: {duplicate_count}")

    print(f"\n🔹 Data Types:\n{data_types}")

    print(f"\n🔹 Summary Statistics (First 5 Columns):\n{summary_stats.iloc[:, :5]}")

    print("\n" + "="*50)

# Process each CSV file
for csv_file in csv_files:
    file_path = os.path.join(directory, csv_file)

    # Load CSV into DataFrame
    df = pd.read_csv(file_path)

    # Perform data quality checks
    check_data_quality(df, csv_file)

```

    
    🔍 Checking data quality for: receipts_cleaned.csv
    
    🔹 Missing Values (Top 10 Columns):
    Series([], dtype: float64)
    
    🔹 Duplicate Records: 0
    
    🔹 Data Types:
    bonusPointsEarned          object
    modifyDate_$date            int64
    purchasedItemCount         object
    rewardsReceiptItemList     object
    bonusPointsEarnedReason    object
    finishedDate_$date         object
    pointsAwardedDate_$date    object
    purchaseDate_$date         object
    userId                     object
    _id_$oid                   object
    rewardsReceiptStatus       object
    totalSpent                 object
    dateScanned_$date           int64
    pointsEarned               object
    createDate_$date            int64
    dtype: object
    
    🔹 Summary Statistics (First 5 Columns):
           bonusPointsEarned  modifyDate_$date purchasedItemCount  \
    count               1119      1.119000e+03               1119   
    unique                13               NaN                 51   
    top              Unknown               NaN            Unknown   
    freq                 575               NaN                484   
    mean                 NaN      1.611847e+12                NaN   
    std                  NaN      1.361576e+09                NaN   
    min                  NaN      1.609687e+12                NaN   
    25%                  NaN      1.610660e+12                NaN   
    50%                  NaN      1.611941e+12                NaN   
    75%                  NaN      1.612704e+12                NaN   
    max                  NaN      1.614641e+12                NaN   
    
           rewardsReceiptItemList bonusPointsEarnedReason  
    count                    1119                    1119  
    unique                    384                      10  
    top                   Unknown                 Unknown  
    freq                      440                     575  
    mean                      NaN                     NaN  
    std                       NaN                     NaN  
    min                       NaN                     NaN  
    25%                       NaN                     NaN  
    50%                       NaN                     NaN  
    75%                       NaN                     NaN  
    max                       NaN                     NaN  
    
    ==================================================
    
    🔍 Checking data quality for: users_cleaned.csv
    
    🔹 Missing Values (Top 10 Columns):
    Series([], dtype: float64)
    
    🔹 Duplicate Records: 282
    
    🔹 Data Types:
    state                object
    createdDate_$date     int64
    role                 object
    lastLogin_$date      object
    _id_$oid             object
    active                 bool
    signUpSource         object
    dtype: object
    
    🔹 Summary Statistics (First 5 Columns):
           state  createdDate_$date      role lastLogin_$date  \
    count    494       4.940000e+02       494             494   
    unique     9                NaN         2             173   
    top       WI                NaN  consumer         Unknown   
    freq     395                NaN       412              62   
    mean     NaN       1.596651e+12       NaN             NaN   
    std      NaN       4.407043e+10       NaN             NaN   
    min      NaN       1.418999e+12       NaN             NaN   
    25%      NaN       1.609789e+12       NaN             NaN   
    50%      NaN       1.610569e+12       NaN             NaN   
    75%      NaN       1.611596e+12       NaN             NaN   
    max      NaN       1.613139e+12       NaN             NaN   
    
                            _id_$oid  
    count                        494  
    unique                       212  
    top     54943462e4b07e684157a532  
    freq                          20  
    mean                         NaN  
    std                          NaN  
    min                          NaN  
    25%                          NaN  
    50%                          NaN  
    75%                          NaN  
    max                          NaN  
    
    ==================================================
    
    🔍 Checking data quality for: brands_cleaned.csv
    
    🔹 Missing Values (Top 10 Columns):
    brandCode    2.999143
    dtype: float64
    
    🔹 Duplicate Records: 0
    
    🔹 Data Types:
    category        object
    cpg_$id_$oid    object
    barcode          int64
    topBrand        object
    categoryCode    object
    _id_$oid        object
    cpg_$ref        object
    name            object
    brandCode       object
    dtype: object
    
    🔹 Summary Statistics (First 5 Columns):
           category              cpg_$id_$oid       barcode topBrand categoryCode
    count      1167                      1167  1.167000e+03     1167         1167
    unique       24                       196           NaN        3           15
    top      Baking  559c2234e4b06aca36af13c6           NaN  Unknown      Unknown
    freq        369                        98           NaN      612          650
    mean        NaN                       NaN  5.111115e+11      NaN          NaN
    std         NaN                       NaN  2.874497e+05      NaN          NaN
    min         NaN                       NaN  5.111110e+11      NaN          NaN
    25%         NaN                       NaN  5.111112e+11      NaN          NaN
    50%         NaN                       NaN  5.111114e+11      NaN          NaN
    75%         NaN                       NaN  5.111117e+11      NaN          NaN
    max         NaN                       NaN  5.111119e+11      NaN          NaN
    
    ==================================================



```python
#Analyze Data Schema
```


```python
csv_files = ["receipts_cleaned.csv", "users_cleaned.csv", "brands_cleaned.csv"]

def analyze_schema(df, file_name):
    """Analyze data schema including column names, types, and uniqueness."""
    print(f"\n📊 Schema Analysis for: {file_name}")

    # Column names and data types
    print("\n🔹 Column Names and Data Types:")
    print(df.dtypes)

    # Check uniqueness to identify primary keys
    unique_counts = df.nunique()
    print("\n🔹 Unique Value Counts (Potential Primary Keys & Categorical Fields):")
    print(unique_counts.sort_values(ascending=False).head(10))

    # Find potential primary keys (columns with all unique values)
    possible_pk = unique_counts[unique_counts == len(df)].index.tolist()
    print("\n🔹 Potential Primary Keys:", possible_pk if possible_pk else "None found")

    # Display sample records
    print("\n🔹 Sample Data (First 5 Rows):")
    print(df.head())

    print("\n" + "="*60)

# Process each CSV file
for csv_file in csv_files:
    file_path = os.path.join(directory, csv_file)

    # Load CSV into DataFrame
    df = pd.read_csv(file_path)

    # Analyze schema
    analyze_schema(df, csv_file)

```

    
    📊 Schema Analysis for: receipts_cleaned.csv
    
    🔹 Column Names and Data Types:
    bonusPointsEarned          object
    modifyDate_$date            int64
    purchasedItemCount         object
    rewardsReceiptItemList     object
    bonusPointsEarnedReason    object
    finishedDate_$date         object
    pointsAwardedDate_$date    object
    purchaseDate_$date         object
    userId                     object
    _id_$oid                   object
    rewardsReceiptStatus       object
    totalSpent                 object
    dateScanned_$date           int64
    pointsEarned               object
    createDate_$date            int64
    dtype: object
    
    🔹 Unique Value Counts (Potential Primary Keys & Categorical Fields):
    _id_$oid                   1119
    dateScanned_$date          1107
    createDate_$date           1107
    modifyDate_$date           1104
    finishedDate_$date          554
    pointsAwardedDate_$date     524
    rewardsReceiptItemList      384
    purchaseDate_$date          359
    userId                      258
    pointsEarned                121
    dtype: int64
    
    🔹 Potential Primary Keys: ['_id_$oid']
    
    🔹 Sample Data (First 5 Rows):
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
    
    ============================================================
    
    📊 Schema Analysis for: users_cleaned.csv
    
    🔹 Column Names and Data Types:
    state                object
    createdDate_$date     int64
    role                 object
    lastLogin_$date      object
    _id_$oid             object
    active                 bool
    signUpSource         object
    dtype: object
    
    🔹 Unique Value Counts (Potential Primary Keys & Categorical Fields):
    createdDate_$date    212
    _id_$oid             212
    lastLogin_$date      173
    state                  9
    signUpSource           3
    role                   2
    active                 2
    dtype: int64
    
    🔹 Potential Primary Keys: None found
    
    🔹 Sample Data (First 5 Rows):
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
    
    ============================================================
    
    📊 Schema Analysis for: brands_cleaned.csv
    
    🔹 Column Names and Data Types:
    category        object
    cpg_$id_$oid    object
    barcode          int64
    topBrand        object
    categoryCode    object
    _id_$oid        object
    cpg_$ref        object
    name            object
    brandCode       object
    dtype: object
    
    🔹 Unique Value Counts (Potential Primary Keys & Categorical Fields):
    _id_$oid        1167
    barcode         1160
    name            1156
    brandCode        897
    cpg_$id_$oid     196
    category          24
    categoryCode      15
    topBrand           3
    cpg_$ref           2
    dtype: int64
    
    🔹 Potential Primary Keys: ['_id_$oid']
    
    🔹 Sample Data (First 5 Rows):
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
    
    ============================================================


Identify the main entities (tables) from your datasets:

1.receipts
2.users
3.brands

Determine primary keys (PK) and foreign keys (FK):

users: userId (PK)
brands: brandId (PK)
receipts: receiptId (PK), userId (FK), brandId (FK - if applicable)
Define relationships:

users ↔ receipts (one user can have many receipts)
brands ↔ receipts (each receipt may contain products from multiple brands)


```python
#Structured Relational Data Model (ER Diagram)
```


```python
#ER Diagram
#Brands-(many-to-many)-receipts-(one-to-many)-Users
```


```python

```
