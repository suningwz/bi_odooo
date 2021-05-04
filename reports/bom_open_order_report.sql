
--
--  Author: Vivere BI Team
--  Date: 2021-04-15(Thursday, 15 April 2020)
--

CREATE TABLE bds_odoo.Bom_open_order_Report AS

select mp.product_qty as Quantity, mp.lot_producing_id as Lot_serial_number,
       mp.bom_id as Bill_of_Materials, mp.date_planned_start as scheduled_date,
       ru.login as Responsible,mp.name as Reference,mp.date_deadline as deadline,
       mp.origin as Source,mp.reservation_state as material_availabilty,
       mp.state as state
from mrp_production mp
    left join res_users ru on mp.user_id = ru.id
    left join stock_production_lot spl on mp.lot_producing_id= spl.id
    left join mrp_bom mb on mp.bom_id = mb.id;