

 # ODOO Business Intelligence work flow

1) Procurement open order reports
2)	MOS report
3)	SKU master
4)	Warehouse master
5)	Flatbom report
6)	work center operations report
7)	Stock movements reports


 ### 1) Procurement open order reports
 
Here we need to create Two tables. 

•	Run the SQL File named Procurement_Open_order_Report and Procurement_Open_order_Report_With_Product_Names.
            
            Final Table names: 1) Procurement_Open_order_Report
                               2) Procurement_Open_order_Report_With_Product_Names


 ### 2) MOS Open Order Report

First Run Python script and Later Run SQL script

•	Run python script named  Open_MOS_Report_Mrp_Production  [MOS is report name and  Mrp_production is model name ]

•	It will generate new Table in Public named Calculated_MOS_Master_Mrp_Production

•	Now Run SQL script MOS_Open_order_Report
          
          Final Table name should be= MOS_Open_order_Report



 ### 3) SKU master
 
•	Run the python script named odoo_SKU_Product_Product 

•	It will create a report directly in bds_odoo called sku_master_open_order_master

              Final Table name should be= sku_master_open_order_master


 ### 4) Warehouse master

•	Run the python script named odoo_warehouse_product_template 

•	It will create a table in bds_odoo called warehouse_open_order_master

  
             Final Table name should be= warehouse_open_order_master
             

 ### 5) Flatbom report
 
•	Run FlatBom Mrp.bom

•	It will generate new Table in Public named Calculated_BOM_Mrp_BOM

•	Now Run SQL script FlatBom_open_order_Report

              Final Table name should be= FlatBom_open_order_Report
