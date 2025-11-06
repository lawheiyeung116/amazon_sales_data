import pandas as pd
import math
import mysql.connector
from mysql.connector import Error

db_config = {
    'user': 'admin',
    'password': 'Mudrock_1234',
    'host': 'localhost',
    'port': 3307,
    'database': 'sales_data'
}

def create_connection():
    try:
        return mysql.connector.connect(**db_config)
    except Error as e:
        print(f"Error: {e}")
        return None

connection = create_connection()
if connection:
    
    df = pd.read_csv('amazon.csv') # https://www.kaggle.com/datasets/karkavelrajaj/amazon-sales-dataset
    
    cursor = connection.cursor()
    
    query = """
    CREATE TABLE IF NOT EXISTS products (
        product_id CHAR(10) PRIMARY KEY,
        product_name VARCHAR(500), -- maxlen = 485
        category VARCHAR(255), -- maxlen = 119
        about_product VARCHAR(3000) -- maxlen = 2646
    );
    """
    cursor.execute(query)
    
    query = """
    CREATE TABLE IF NOT EXISTS product_details (
        product_id CHAR(10) PRIMARY KEY,
        rating FLOAT,
        rating_count INT,
        img_link VARCHAR(255), -- maxlen = 114
        product_link VARCHAR(255), -- maxlen = 188
        FOREIGN KEY (product_id) REFERENCES products(product_id)
            ON DELETE CASCADE
    );
    """
    cursor.execute(query)
    
    query = """
    CREATE TABLE IF NOT EXISTS prices (
        product_id CHAR(10) PRIMARY KEY,
        discounted_price_in₹ FLOAT, -- change column name to save as INT
        actual_price_in₹ FLOAT, -- same as above
        discount_percentage INT,
        FOREIGN KEY (product_id) REFERENCES products(product_id)
            ON DELETE CASCADE
    );
    """
    cursor.execute(query)
    
    query = """
    CREATE TABLE IF NOT EXISTS users ( -- seperated by ,
        user_id CHAR(28) PRIMARY KEY, -- maxlen = 231
        user_name VARCHAR(50) -- maxlen = 137
    );
    """
    cursor.execute(query)
    
    query = """
    CREATE TABLE IF NOT EXISTS reviews ( -- seperated by ,
        product_id CHAR(10),
        user_id CHAR(28), -- maxlen = 231
        review_id CHAR(14) PRIMARY KEY, -- maxlen = 119
        review_title VARCHAR(100), -- maxlen = 407
        review_content VARCHAR(5000), -- maxlen = 18547
        FOREIGN KEY (product_id) REFERENCES products(product_id)
            ON DELETE CASCADE,
        FOREIGN KEY (user_id) REFERENCES users(user_id)
            ON DELETE CASCADE
    );
    """
    cursor.execute(query)
    
    index_queries = [
        "CREATE INDEX IF NOT EXISTS idx_product_id ON reviews (product_id);",
        "CREATE INDEX IF NOT EXISTS idx_user_id ON reviews (user_id);",
        "CREATE INDEX IF NOT EXISTS idx_category ON products (category(50));",
        # category(50) is category with prefix index 50, only searches top 50 chars to accelerate the process
        "CREATE INDEX IF NOT EXISTS idx_rating ON product_details (rating);",
        "CREATE INDEX IF NOT EXISTS idx_price ON prices (discounted_price_in₹);",
    ]

    for q in index_queries:
        cursor.execute(q)
        print(f"Index added: {q.split('ADD INDEX')[1].strip()}")
    
    for _, row in df.iterrows():
        # Insert into tables (ignore duplicates)
        data = (
            row['product_id'], row['product_name'],
            row['category'], row['about_product']
        )
        query = """
        INSERT IGNORE INTO products 
        (product_id, product_name, category, about_product)
        VALUES (%s, %s, %s, %s)
        """
        cursor.execute(query, data)
        
        if type(row['rating_count']) is str:
            row['rating_count'] = int(row['rating_count'].replace(',', ''))
            # '24,370' -> 24370
        elif math.isnan(row['rating_count']):
            row['rating_count'] = 0
        
        data = (
            row['product_id'], row['rating'], row['rating_count'],
            row['img_link'], row['product_link']
        )
        query = """
        INSERT IGNORE INTO product_details
        (product_id, rating, rating_count, img_link, product_link)
        VALUES (%s, %s, %s, %s, %s)
        """     
        
        cursor.execute(query, data)
        
        # change ₹3,999 into 3999
        row['discounted_price'] = float(row['discounted_price'][1:].replace(',', ''))
        row['actual_price'] = float(row['actual_price'][1:].replace(',', ''))
        
        data = (
            row['product_id'], row['discounted_price'],
            row['actual_price'], row['discount_percentage']
        )
        
        query = """
        INSERT IGNORE INTO prices 
        (product_id, discounted_price_in₹, actual_price_in₹, discount_percentage)
        VALUES (%s, %s, %s, %s)
        """
        cursor.execute(query, data)
        
        # Split reviews (assuming comma-separated; handle escapes if needed)
        user_ids = [uid.strip() for uid in row['user_id'].split(',')]
        user_names = [name.strip() for name in row['user_name'].split(',')]
        review_ids = [rid.strip() for rid in row['review_id'].split(',')]
        review_titles = [title.strip() for title in row['review_title'].split(',')]
        review_contents = [content.strip() for content in row['review_content'].split(',')]
        
        # Insert reviews (assume lists are equal length)
        for i in range(len(review_ids)):
            data = (user_ids[i], user_names[i])
            query = """
            INSERT IGNORE INTO users (user_id, user_name)
            VALUES (%s, %s)
            """
            cursor.execute(query, data)
            
            data = (
                review_ids[i], row['product_id'], user_ids[i],
                review_titles[i], review_contents[i]
            )
            query = """
            INSERT IGNORE INTO reviews 
            (review_id, product_id, user_id, review_title, review_content)
            VALUES (%s, %s, %s, %s, %s)
            """
            cursor.execute(query, data)
    
    connection.commit()
    connection.close()
    print("Data inserted successfully")
    
# mysql -h localhost -P 3307 -u admin -p