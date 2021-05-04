"""
Goal: To create Open order Production Master report
Code: Python ODOO API
Odoo-Model: Mrp.Bom
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


# Step-1- Id selection (Primary Key) for mrp.bom model for production Master

Production_bom_ids=models.execute_kw(db, uid, password,'mrp.bom', 'search',[[]])
#print(Production_bom_ids)

#Step-2- Print Number of results (This will be the final row count)
Production_bom_count = models.execute_kw(db, uid, password, 'mrp.bom', 'search_count',[[]])
#print(Production_bom_count)

### Getting results for mrp.bom model for Production_bom report

Production_bom_Master=models.execute_kw(db, uid, password,
                              'mrp.bom', 'read',
                              [Production_bom_ids], {'fields': ['id','product_tmpl_id', 'code','bom_line_ids', 'type',
                                                                'write_date','product_qty','consumption']})

for PBOM in Production_bom_Master:
    print(PBOM)


# Step-7- Create a Dataframe using panadas
Production_Master_Mrp_Bom = pd.DataFrame.from_dict(Production_bom_Master)
print(Production_Master_Mrp_Bom)

# Step-8- Change the Datatypes- Data Preprocessing 
Production_Master_Mrp_Bom['product_tmpl_id'] = Production_Master_Mrp_Bom['product_tmpl_id'].astype('str')
#Production_Master_Mrp_Bom['bom_line_ids'] = Production_Master_Mrp_Bom['bom_line_ids'].astype('int')

# Step-9- Rename the columns as per requirement  
Production_Master_Mrp_Bom.rename(columns={'product_tmpl_id': 'Product',
                                          'product_qty': 'Quantity',
                                          'code': 'Reference',
                                          'type': 'BOMTYPE',
                                          'write_date': 'Last_Updated_On'},inplace=True)

print(Production_Master_Mrp_Bom.columns)

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
        cursor.execute("DROP TABLE IF EXISTS Calculated_BOM_Mrp_BOM;") 
        sql = '''CREATE TABLE Calculated_BOM_Mrp_BOM(
            id                                     integer,
            bom_line_ids                           float,
            Product                                varchar,
            Quantity                               numeric,
            Reference                              varchar,
            BOMTYPE                                Text,
            Last_Updated_On                        timestamp,
            consumption                            Text
        )'''
# Creating a table
        cursor.execute(sql);
        print("Calculated_BOM_Mrp_BOM table is created successfully..................")
    
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

inter_df = pd.DataFrame()
inter_df = Production_Master_Mrp_Bom

print(inter_df )
inter_df['bom_line_ids'] = inter_df['bom_line_ids'].astype(str)
inter_df['codes'] = inter_df['bom_line_ids'].map(lambda x: x.lstrip('[').rstrip(']'))
inter_df_clean = pd.concat([inter_df['id'], inter_df['Product'], inter_df['consumption'], inter_df['Quantity'],
                            inter_df['Reference'],inter_df['BOMTYPE'],inter_df['Last_Updated_On'],
                                     inter_df['codes'].str.split(',', expand=True)], axis=1)

Production_Master_Mrp_Bom_final = pd.melt(inter_df_clean, id_vars=['id', 'Product', 'consumption','Quantity',
                                                                   'Reference','BOMTYPE','Last_Updated_On'], var_name='code_id', value_name='bom_line_ids')

df = Production_Master_Mrp_Bom_final[Production_Master_Mrp_Bom_final['bom_line_ids'].notna()]
final_df = df.drop(columns='code_id')
final_df['bom_line_ids'] = pd.to_numeric(final_df['bom_line_ids'])
print(final_df.info())

execute_values(conn, final_df, 'Calculated_BOM_Mrp_BOM')

