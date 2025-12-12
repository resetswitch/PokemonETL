import psycopg2

# Connect to the default 'postgres' database
conn = psycopg2.connect(
    dbname="postgres",
    user="user",
    password="password",
    host="localhost",
    port="5432"
)

conn.autocommit = True  # Required to create a new database
cursor = conn.cursor()

# Create a new database
cursor.execute("CREATE DATABASE my_new_database")

print("✅ Database created successfully!")

cursor.close()
conn.close()