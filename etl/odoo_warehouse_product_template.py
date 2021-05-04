  
"""
Goal: To create Open order Warehouse Master report
Code: Python ODOO API
Model: Product.Template
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


# + `Getting results for product.Template model for Warehouse Master report`:
# 
#     Step-1- Id selection (Primary Key) for Product_Template Model for Warehouse Master
#     Step-2- Print Number of results (This will be the final row count)
#     Step-3- Identify the model, For Warehouse Master it is Product.Template
#     Step-4- To view the feild names in ODOO the user need to activate developer mode
#     Step-5- ODOO--> Settings-> Activate developer mode
#     Step-6- Select list of Feilds to get data for the final report
#     Step-7- Create a Dataframe using panadas
#     Step-8- Change the Datatypes
#     Step-9- Rename the columns as per requirement  


## Product_Template Model for Ware House Master
Warehouse_Master_ids=models.execute_kw(db, uid, password,'product.template', 'search',[[]])
print(Warehouse_Master_ids)

Warehouse_Master_count = models.execute_kw(db, uid, password, 'product.template', 'search_count',[[]])
print(Warehouse_Master_count)

### Getting results for product.product model for SKU Master report

Warehouse_Master=models.execute_kw(db, uid, password,
                              'product.template', 'read',
                              [Warehouse_Master_ids], {'fields': ['default_code',
                              'categ_id','type','x_studio_mfm_class','x_studio_char_field_0s6EM',
                              'standard_price', 'create_date','sales_count','bom_count','list_price',
                              'virtual_available','qty_available','reordering_min_qty',
                              'reordering_max_qty','quality_control_point_qty','sale_ok','purchase_ok',
                              'uom_po_id']})

for Warehouse in Warehouse_Master:
    print(Warehouse)
    

### WareHouse_Master dataframe
WareHouse_Master_Product_Template = pd.DataFrame.from_dict(Warehouse_Master)

WareHouse_Master_Product_Template['categ_id'] = WareHouse_Master_Product_Template['categ_id'].astype('str')
WareHouse_Master_Product_Template['uom_po_id'] = WareHouse_Master_Product_Template['uom_po_id'].astype('str')

WareHouse_Master_Product_Template.rename(columns={'default_code': 'Internal_Reference',
                                           'categ_id': 'Product_Category','type': 'Product_Type',
                                           'x_studio_mfm_class': 'VFG_CLASS',
                                           'qty_available': 'On_Hand',
                                           'virtual_available': 'Forecasted',
                                           'reordering_min_qty': 'Min',
                                            'reordering_max_qty': 'Max',
                                            'quality_control_point_qty': 'Quality_Points',
                                            'sale_ok': 'Can_be_sold',
                                            'purchase_ok': 'Can_be_purchased',
                                           'x_studio_char_field_0s6EM': 'EAN',
                                           'list_price': 'Sales_Price',
                                           'bom_count': 'Bill_of_Materials',
                                           'sales_count': 'Sold',
                                           'purchase_ok': 'can_be_purchased',
                                           'sale_ok': 'can_be_sold',
                                           'x_studio_use_unit_of_measure_ml': 'Density_(kg/ml)'}, inplace=True)

print(WareHouse_Master_Product_Template.head())


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
        cursor.execute("DROP TABLE IF EXISTS bds_odoo.warehouse_open_order_master;") 
        sql = '''CREATE TABLE bds_odoo.warehouse_open_order_master(
            id                                     integer, 
            Internal_Reference                     varchar,
            Product_Category                       varchar,
            Product_Type                           TEXT,
            VFG_CLASS                              TEXT,
            EAN                                    TEXT,
            create_date                            timestamp,
            standard_price                         numeric,
            Sold                                  numeric,
            Bill_of_Materials                     integer,
            Sales_Price                           numeric,
            Forecasted                            numeric,
            On_Hand                               numeric,
            Min                                   integer,
            Max                                   integer,
            Quality_Points                         TEXT,
            can_be_sold                            TEXT,
            can_be_purchased                        TEXT,
            uom_po_id                               TEXT
        )'''
        
        # Creating a table
        cursor.execute(sql);
        print("warehouse_open_order_master table is created successfully..................")
    
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
execute_values(conn, WareHouse_Master_Product_Template, 'bds_odoo.warehouse_open_order_master')
