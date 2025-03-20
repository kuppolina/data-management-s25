import psycopg2
from pymongo import MongoClient
import sys
from datetime import datetime  # Add this import at the top of your file
from postgresql.suppliers.config import load_config
from postgresql.suppliers.connect_postgresql import connect_postgresql
from mongodb.suppliers.connect_mongodb import connect_mongodb
from decimal import Decimal

config_data = load_config(filename='database.ini', section='postgresql')  # Load config before passing it

# Connect to PostgreSQL
conn = connect_postgresql(config_data)

if conn: 
    pg_cursor = conn.cursor()
else: 
    raise ConnectionError("Failed to connect to the postgresql")

# Connect to MongoDB
mongo_client = connect_mongodb()

if mongo_client:
    mongo_db = mongo_client["suppliers"]
else: 
    raise ConnectionError("Failed to connect to mongodb")

# Fetch actors from PostgreSQL
pg_cursor.execute("SELECT actor_id, first_name, last_name, last_update FROM actor;")
actors = pg_cursor.fetchall()

for actor in actors:
    doc = {
        "_id": actor[0],
        "first_name": actor[1],
        "last_name": actor[2],
        "last_update": actor[3]
    }
    mongo_db.actors.insert_one(doc)

# Fetch address from PostgreSQL
pg_cursor.execute("SELECT address_id, address, address2, district, city_id, postal_code, phone, last_update FROM address;")
addresses = pg_cursor.fetchall()

for address in addresses:
    doc = {
        "_id": address[0],
        "address": address[1],
        "address2": address[2],
        "district": address[3],
        "city_id": address[4],
        "postal_code": address[5],
        "phone": address[6],
        "last_update": address[7]
    }
    mongo_db.addresses.insert_one(doc)


# Fetch category from PostgreSQL
pg_cursor.execute("SELECT category_id, name, last_update FROM category;")
categories = pg_cursor.fetchall()

for category in categories:
    doc = {
        "_id": category[0],
        "name": category[1],
        "last_update": category[2]
    }
    mongo_db.categories.insert_one(doc)

# Fetch city from PostgreSQL
pg_cursor.execute("SELECT city_id, city, country_id, last_update FROM city;")
cities = pg_cursor.fetchall()

for city in cities:
    doc = {
        "_id": city[0],
        "city": city[1],
        "country_id": city[2],
        "last_update": city[3]
    }
    mongo_db.cities.insert_one(doc)

# Fetch country from PostgreSQL
pg_cursor.execute("SELECT country_id, country, last_update FROM country;")
countries = pg_cursor.fetchall()

for country in countries:
    doc = {
        "_id": country[0],
        "country": country[1],
        "last_update": country[2]
    }
    mongo_db.countries.insert_one(doc)

# Fetch customer from PostgreSQL
pg_cursor.execute("SELECT customer_id, store_id, first_name, last_name, email, address_id, activebool, create_date, last_update, active FROM customer;")
customers = pg_cursor.fetchall()

for customer in customers:
    # have to convert the datatime.date to datetime.datetime 
    # Convert PostgreSQL date/datetime to MongoDB-compatible datetime
    create_date = customer[7]

    # If create_date is a date (not datetime), convert it
    if (create_date is not datetime.date):
        create_date = datetime(create_date.year, create_date.month, create_date.day)

    doc = {
        "_id": customer[0],
        "store_id": customer[1],
        "first_name": customer[2],
        "last_name": customer[3],
        "email": customer[4],
        "address_id": customer[5],
        "active": customer[6],
        "create_date": create_date,
        "last_update": customer[8],
        "is_active": customer[9]
    }
    mongo_db.customers.insert_one(doc)
    

# Fetch film from PostgreSQL
pg_cursor.execute("SELECT film_id, title, description, release_year, language_id, original_language_id, rental_duration, rental_rate, length, replacement_cost, rating, last_update, special_features, fulltext FROM film;")
films = pg_cursor.fetchall()

for film in films:
    
    rental_rate = film[7]
    replacement_cost = film[9]
    
     # If create_date is a date (not datetime), convert it
    if rental_rate is not None and isinstance(rental_rate, Decimal):
        rental_rate = str(rental_rate)

    if replacement_cost is not None and isinstance(replacement_cost, Decimal):
        replacement_cost = str(replacement_cost)
        
    doc = {
        "_id": film[0],
        "title": film[1],
        "description": film[2],
        "release_year": film[3],
        "language_id": film[4],
        "original_language_id": film[5],
        "rental_duration": film[6],
        "rental_rate": rental_rate,
        "length": film[8],
        "replacement_cost": replacement_cost,
        "rating": film[10],
        "last_update": film[11],
        "special_features": film[12]
    }
    mongo_db.films.insert_one(doc)

# Fetch film_actor from PostgreSQL
pg_cursor.execute("SELECT actor_id, film_id, last_update FROM film_actor;")
film_actors = pg_cursor.fetchall()

for film_actor in film_actors:
    doc = {
        "actor_id": film_actor[0],
        "film_id": film_actor[1],
        "last_update": film_actor[2]
    }
    mongo_db.film_actor.insert_one(doc)

# Fetch film_category from PostgreSQL
pg_cursor.execute("SELECT film_id, category_id, last_update FROM film_category;")
film_categories = pg_cursor.fetchall()

for film_category in film_categories:
    doc = {
        "film_id": film_category[0],
        "category_id": film_category[1],
        "last_update": film_category[2]
    }
    mongo_db.film_category.insert_one(doc)

# Fetch inventory from PostgreSQL
pg_cursor.execute("SELECT inventory_id, film_id, store_id, last_update FROM inventory;")
inventories = pg_cursor.fetchall()

for inventory in inventories:
    doc = {
        "_id": inventory[0],
        "film_id": inventory[1],
        "store_id": inventory[2],
        "last_update": inventory[3]
    }
    mongo_db.inventories.insert_one(doc)

# Fetch language from PostgreSQL
pg_cursor.execute("SELECT language_id, name, last_update FROM language;")
languages = pg_cursor.fetchall()

for language in languages:
    doc = {
        "_id": language[0],
        "name": language[1],
        "last_update": language[2]
    }
    mongo_db.languages.insert_one(doc)

# Fetch payment from PostgreSQL
pg_cursor.execute("SELECT payment_id, customer_id, staff_id, rental_id, amount, payment_date FROM payment;")
payments = pg_cursor.fetchall()

for payment in payments:
    amount = payment[4]
    
    if amount is not None and isinstance(amount, Decimal):
        amount = str(amount)
        
    payment_date = payment[5]
    if (payment_date is not datetime.date):
        payment_date = datetime(payment_date.year, payment_date.month, payment_date.day)

    doc = {
        "_id": payment[0],
        "customer_id": payment[1],
        "staff_id": payment[2],
        "rental_id": payment[3],
        "amount": amount,
        "payment_date": payment_date
    }
    mongo_db.payments.insert_one(doc)


# # Fetch payment_p2007_01 from PostgreSQL
# pg_cursor.execute("SELECT payment_id, customer_id, staff_id, rental_id, amount, payment_date FROM payment_p2007_01;")
# payment_p2007_01s = pg_cursor.fetchall()

# # Fetch payment_p2007_02 from PostgreSQL
# pg_cursor.execute("SELECT payment_id, customer_id, staff_id, rental_id, amount, payment_date FROM payment_p2007_02;")
# payment_p2007_02s = pg_cursor.fetchall()

# # Fetch payment_p2007_03 from PostgreSQL
# pg_cursor.execute("SELECT payment_id, customer_id, staff_id, rental_id, amount, payment_date FROM payment_p2007_03;")
# payment_p2007_03s = pg_cursor.fetchall()

# # Fetch payment_p2007_04 from PostgreSQL
# pg_cursor.execute("SELECT payment_id, customer_id, staff_id, rental_id, amount, payment_date FROM payment_p2007_04;")
# payment_p2007_04s = pg_cursor.fetchall()

# # Fetch payment_p2007_05 from PostgreSQL
# pg_cursor.execute("SELECT payment_id, customer_id, staff_id, rental_id, amount, payment_date FROM payment_p2007_05;")
# payment_p2007_05s = pg_cursor.fetchall()

# # Fetch payment_p2007_06 from PostgreSQL
# pg_cursor.execute("SELECT payment_id, customer_id, staff_id, rental_id, amount, payment_date FROM payment_p2007_06;")
# payment_p2007_06s = pg_cursor.fetchall()

# Fetch rental from PostgreSQL
pg_cursor.execute("SELECT rental_id, rental_date, inventory_id, customer_id, return_date, staff_id, last_update FROM rental;")
rentals = pg_cursor.fetchall()

for rental in rentals:
    doc = {
        "_id": rental[0],
        "rental_date": rental[1],
        "inventory_id": rental[2],
        "customer_id": rental[3],
        "return_date": rental[4],
        "staff_id": rental[5],
        "last_update": rental[6]
    }
    mongo_db.rentals.insert_one(doc)


# # Fetch staff from PostgreSQL
pg_cursor.execute("SELECT staff_id, first_name, last_name, address_id, email, store_id, active, username, password, last_update, picture FROM staff;")
staffs = pg_cursor.fetchall()

for staff in staffs:
    doc = {
        "_id": staff[0],
        "first_name": staff[1],
        "last_name": staff[2],
        "address_id": staff[3],
        "email": staff[4],
        "store_id": staff[5],
        "active": staff[6],
        "username": staff[7],
        "password": staff[8],
        "last_update": staff[9]
    }
    mongo_db.staff.insert_one(doc)


# # Fetch store from PostgreSQL
pg_cursor.execute("SELECT store_id, manager_staff_id, address_id, last_update FROM store;")
stores = pg_cursor.fetchall()

for store in stores:
    doc = {
        "_id": store[0],
        "manager_staff_id": store[1],
        "address_id": store[2],
        "last_update": store[3]
    }
    mongo_db.stores.insert_one(doc)

# Close connections
pg_cursor.close()
conn.close()
mongo_client.close()
