--
--  Author: Vivere BI Team
--  Date: 2021-04-15(Thursday, 15 April 2020)
--

CREATE TABLE bds_odoo.stock_move_report_with_Product_Description AS

select pp.default_code as product,
       sp.name as Reference,sp.origin as source_document,
       spl.name as Lot_seral_Number,sp.create_date as Created_On,sp.scheduled_date as Scheduled_date, sp.date_deadline, sp.move_type,
       sl.complete_name as From,
       sll.complete_name as Deestination_Location,
       spp.name as Back_order_of,
       spt.name as Operation_Type,
       sp.state as Sate,
       rp.name as Receive_From,
       sml.product_qty,
       sml.product_uom_qty as Reserved,
       sml.qty_done as Quantity_Done,
       uu.name as unit_of_measure
from stock_move_line sml
left join product_product pp on sml.product_id = pp.id
left join stock_production_lot spl on sml.lot_id= spl.id
left join uom_uom uu on sml.product_uom_id = uu.id
left join stock_picking sp on sml.picking_id = sp.id
left join stock_location sl on sp.location_id = sl.id
left join stock_location sll on sp.location_dest_id = sll.id
left join stock_picking spp on sp.backorder_id = spp.id
left join stock_picking_type spt on sp.picking_type_id = spt.id
left join stock_picking_batch spb on sp.batch_id = spb.id
left join res_partner rp on sp.partner_id = rp.id