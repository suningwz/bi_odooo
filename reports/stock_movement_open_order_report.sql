--
--  Author: Vivere BI Team
--  Date: 2021-04-15(Thursday, 15 April 2020)
--

CREATE TABLE bds_odoo.stock_movement_Open_order_Report AS

select sp.id,sp.name as Reference, sp.origin as source_document, sp.create_date as Created_On,sl.complete_name as From,
       sll.complete_name as Deestination_Location, sp.scheduled_date as Scheduled_date, sp.date_deadline,spp.name as Back_order_of,
       spt.name as Operation_Type,spb.name as Batch_Transfer,sp.state as Sate,rp.name as Receive_From
from stock_picking sp
    left join stock_location sl on sp.location_id = sl.id
    left join stock_location sll on sp.location_dest_id = sll.id
    left join stock_picking spp on sp.backorder_id = spp.id
    left join stock_picking_type spt on sp.picking_type_id = spt.id
    left join stock_picking_batch spb on sp.batch_id = spb.id
    left join res_partner rp on sp.partner_id = rp.id