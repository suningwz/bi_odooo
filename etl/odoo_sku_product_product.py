  
"""
Goal: To create Open order SKU Master report
Code: Python ODOO API
Model: Product.Product
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
SKU_Master_ids=models.execute_kw(db, uid, password,'product.product', 'search',[[]])
print(SKU_Master_ids)

#Step-2- Print Number of results (This will be the final row count)
SKU_Master_Count = models.execute_kw(db, uid, password, 'product.product', 'search_count',[[]])
print(SKU_Master_Count)

SKU_Master=models.execute_kw(db, uid, password,
                              'product.product', 'read',
                              [SKU_Master_ids], {'fields': ['default_code',
                              'barcode',
                              'categ_id',
                              'type',
                              'name',
                              'standard_price',
                              'lst_price',
                              'virtual_available',
                              'qty_available',
                              'nbr_reordering_rules',
                              'bom_count',
                              'used_in_bom_count',
                              'purchased_product_qty',
                              'uom_po_id',
                              'quality_control_point_qty',
                              'sale_ok',
                              'x_studio_use_unit_of_measure_ml',
                              'purchase_method',
                              'property_stock_production',
                              'property_stock_inventory']})

for SKU in SKU_Master:
    print(SKU)


# Step-7- Create a Dataframe using panadas
SKU_Master_Product_Product = pd.DataFrame.from_dict(SKU_Master)
print(SKU_Master_Product_Product.head())

# Step-8- Change the Datatypes- Data Preprocessing 
SKU_Master_Product_Product['categ_id'] = SKU_Master_Product_Product['categ_id'].astype('str')
SKU_Master_Product_Product['uom_po_id'] = SKU_Master_Product_Product['uom_po_id'].astype('str')
SKU_Master_Product_Product['property_stock_production'] = SKU_Master_Product_Product['property_stock_production'].astype('str')
SKU_Master_Product_Product['property_stock_inventory'] = SKU_Master_Product_Product['property_stock_inventory'].astype('str')
SKU_Master_Product_Product['barcode'] = SKU_Master_Product_Product['barcode'].astype('int64')

# Step-9- Rename the columns as per requirement  
SKU_Master_Product_Product.columns
SKU_Master_Product_Product.rename(columns={'default_code': 'Internal_Reference',
                                           'categ_id': 'Product_Category','type': 'Product_Type',
                                           'qty_available': 'Quantity_On_Hand',
                                           'virtual_available': 'Forecasted_Quantity',
                                           'nbr_reordering_rules': 'Reordering_Rules',
                                           'lst_price': 'Sales_Price',
                                           'used_in_bom_count': 'Used_In',
                                           'lst_price': 'Sales_Price',
                                           'quality_control_point_qty': 'Quality_Points',
                                           'sale_ok': 'can_be_sold',
                                           'x_studio_use_unit_of_measure_ml': 'Density_kg_ml',
                                           'property_stock_production': 'Production_Location',
                                           'property_stock_inventory': 'Inventory_Location',},inplace=True)



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
        cursor.execute("DROP TABLE IF EXISTS bds_odoo.sku_master_open_order;") 
        sql = '''CREATE TABLE bds_odoo.sku_master_open_order(
            id                                     integer, 
            Internal_Reference                     varchar,
            barcode                                bigint,
            Product_Category                       varchar,
            Product_Type                           TEXT,
            name                                   TEXT,
            standard_price                         numeric,
            Sales_Price                            numeric,
            Forecasted_Quantity                    numeric,
            Quantity_On_Hand                       numeric,
            Reordering_Rules                       integer,
            bom_count                              integer,
            Used_In                                integer,
            purchased_product_qty                  numeric,
            uom_po_id                              TEXT,
            Quality_Points                         TEXT,
            can_be_sold                            TEXT,
            Density_kg_ml                          numeric,
            purchase_method                        TEXT,
            Production_Location                    TEXT,
            Inventory_Location                     TEXT
        )'''
# Creating a table
        cursor.execute(sql);
        print("sku_master_open_order table is created successfully..................")
    
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
execute_values(conn, SKU_Master_Product_Product, 'bds_odoo.sku_master_open_order')

