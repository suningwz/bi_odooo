"""
Goal: To create Open order MOS Master report (Manufacturing order)
Code: Python ODOO API
Model: Mrp.Production
Database: Postgresql
Author: Harsha_Kantipudi (Vivere Gmbh)

"""

import xmlrpc.client
import pandas as pd         # create dataframes 
import os                   # fetch files
import time                 # timing operations
import json
import io
from configparser import ConfigParser  # Import the 'config' function 
import psycopg2                        # python->psql connection
from psycopg2 import OperationalError, errorcodes, errors  # import the error handling libraries for psycopg2
import psycopg2.extras as extras
import psycopg2.extras
import memory_profiler                  # managing memory usagez
from memory_profiler import memory_usage
from functools import wraps             # decorator/wrapper
from typing import Iterator, Optional, Dict, Any,List  # Create Iterator for One-By-One Loading 
import sys                                # import sys to get more detailed Python exception info
from psycopg2 import OperationalError, errorcodes, errors  # import the error handling libraries for psycopg2

# Create a config object
config_object = ConfigParser()
config_object.read("config.ini")
odoo_user_info = config_object["ODOO_USER_INFO"]
Database_connection=config_object["postgresql"]

# Credentials to connect ODOO
url = odoo_user_info['url']
db = odoo_user_info['db']
username = odoo_user_info['username']
password = odoo_user_info['password']

# Credentials to connect Postgresql
conn_params_dic = {
    "host"      :Database_connection['host'],
    "database"  :Database_connection['database'],
    "user"      :Database_connection['username'],
    "password"  :Database_connection['password'],
    "port"      :Database_connection['port']
}

# Use Cred to Connect
common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(url))
uid = common.authenticate(db, username, password, {})
models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(url))
version=common.version()
print('ODOO VERSION DETAILS',version)


# + `Getting results for product.product model for SKU Master report`:
# 
#     Step-1- Id selection (Primary Key) for Product_product Model for SKU Master
#     Step-2- Print Number of results (This will be the final row count)
#     Step-3- Identify the model, For SKU Master it is Product.Product
#     Step-4- To view the feild names in ODOO the user need to activate developer mode
#     Step-5- ODOO--> Settings-> Activate developer mode
#     Step-6- Select list of Feilds to get data for the final report
#     Step-7- Create a Dataframe using panadas
#     Step-8- Change the Datatypes
#     Step-9- Rename the columns as per requirement  


# Step-1- Id selection (Primary Key) for Product_product Model for SKU Master 
MOS_Master_ids=models.execute_kw(db, uid, password,'mrp.production', 'search',[[]])
print(MOS_Master_ids)

#Step-2- Print Number of results (This will be the final row count)
MOS_Master_Count = models.execute_kw(db, uid, password, 'mrp.production', 'search_count',[[]])
print(MOS_Master_Count)

MOS_Master=models.execute_kw(db, uid, password,
                              'mrp.production', 'read',
                              [MOS_Master_ids], {'fields': ['name',
                              'product_id',
                              'bom_id',
                              'delivery_count']})

for MOS in MOS_Master:
    print(MOS)


# Step-7- Create a Dataframe using panadas
MOS_Master_Mrp_Production = pd.DataFrame.from_dict(MOS_Master)
print(MOS_Master_Mrp_Production.head())


MOS_Master_Mrp_Production['product_id'] = MOS_Master_Mrp_Production['product_id'].astype('str')
MOS_Master_Mrp_Production['bom_id'] = MOS_Master_Mrp_Production['bom_id'].astype('str')

# Step-9- Rename the columns as per requirement  
MOS_Master_Mrp_Production.columns
MOS_Master_Mrp_Production.rename(columns={'name': 'Internal_Reference',
                                           'product_id': 'Product_Name',
                                           'bom_id': 'Bill_of_Materials',
                                           'delivery_count': 'Transfers'},inplace=True)



### Sending data to Postgresql
def show_psycopg2_exception(err):
    # get details about the exception
    err_type, err_obj, traceback = sys.exc_info()
    
    # get the line number when exception occured
    line_n = traceback.tb_lineno

    # print the connect() error
    print ("\npsycopg2 ERROR:", err, "on line number:", line_n)
    print ("psycopg2 traceback:", traceback, "-- type:", err_type)

    # psycopg2 extensions.Diagnostics object attribute
    print ("\nextensions.Diagnostics:", err.diag)

    # print the pgcode and pgerror exceptions
    print ("pgerror:", err.pgerror)
    print ("pgcode:", err.pgcode, "\n")
    
    
def connect(conn_params_dic):
    conn = None
    try:
        print('Connecting to the PostgreSQL...........')
        conn = psycopg2.connect(**conn_params_dic)
        print("Connection successful..................")
        
    except OperationalError as err:
        # pass exception to function
        show_psycopg2_exception(err)

        # set the connection to 'None' in case of error
        conn = None
    
    return conn

conn = connect(conn_params_dic)
conn.autocommit = True

if conn!=None:
    
    try:
        cursor = conn.cursor();
        # Dropping table iris if exists
        cursor.execute("DROP TABLE IF EXISTS Calculated_MOS_Master_Mrp_Production;") 
        sql = '''CREATE TABLE Calculated_MOS_Master_Mrp_Production(
            id                                     integer, 
            Internal_Reference                     varchar,
            Product_Name                           varchar,
            Bill_of_Materials                      TEXT,
            Transfers                              integer
        )'''
# Creating a table
        cursor.execute(sql);
        print("Calculated_MOS_Master_Mrp_Production table is created successfully..................")
    
        # Closing the cursor & connection
        cursor.close()
        conn.close()
        
    except OperationalError as err:
        # pass exception to function
        show_psycopg2_exception(err)
        # set the connection to 'None' in case of error
        conn = None


# Define function using psycopg2.extras.execute_values() to insert the dataframe.
def execute_values(conn, datafrm, table):
    
    # Creating a list of tupples from the dataframe values
    tpls = [tuple(x) for x in datafrm.to_numpy()]
    
    # dataframe columns with Comma-separated
    cols = ','.join(list(datafrm.columns))
    
    # SQL query to execute
    sql = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, sql, tpls)
        conn.commit()
        print("Data inserted using execute_values() successfully...")
    except (Exception, psycopg2.DatabaseError) as err:
        # pass exception to function
        show_psycopg2_exception(err)
        cursor.close()


# Connect to the database
conn = connect(conn_params_dic)
conn.autocommit = True


# Run the execute_batch method
execute_values(conn, MOS_Master_Mrp_Production, 'Calculated_MOS_Master_Mrp_Production')



