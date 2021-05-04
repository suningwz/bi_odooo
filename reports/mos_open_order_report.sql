
--
--  Author: Vivere BI Team
--  Date: 2021-04-15(Thursday, 15 April 2020)
--
-- Before running this script we need to run python script (Mrp_Production) and the new table will be saved in public as calculated_mos_master_mrp_production


CREATE TABLE bds_odoo.MOS_Open_order_Report AS

select mp.priority as Priority,mp.name as Reference,cmos.product_name as Product_Name,
       mp.create_date as Create_Date,mp.write_date as Last_Updated_On,
       mp.date_planned_start as Scheduled_Date,mp.date_planned_finished as Date_Planned_Finished,
       mp.date_finished as Date_Finished,mp.origin as Source,ru.login as Responsible,
       mp.reservation_state as Material_Availabitly,mp.product_qty as Quantity_to_Produce,
       mp.qty_producing as Quantity_Producing,mp.product_uom_qty as Total_Quantity,mp.state as State,
       mp.consumption as consumption, mp.product_description_variants as Custom_Description,
       pp.default_code as Product,
       uu.name as unit_of_measure,
       spl.name as Lot_Serial_Number,
       cmos.bill_of_materials as Bill_Of_Materials,cmos.transfers as Transfers,
       sl.complete_name as Componets_Location,sl.complete_name as Finished_Products_Location

from mrp_production mp
    left join product_product pp on mp.product_id=pp.id
    left join uom_uom uu on mp.product_uom_id=uu.id
    left join stock_production_lot spl on mp.lot_producing_id=spl.id
    left join res_users ru on mp.user_id = ru.id
    left join stock_location sl on mp.location_src_id = sl.id
    left join calculated_mos_master_mrp_production cmos on mp.product_id = cmos.id