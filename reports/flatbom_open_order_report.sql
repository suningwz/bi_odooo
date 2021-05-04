
--
--  Author: Vivere BI Team
--  Date: 2021-04-15(Thursday, 15 April 2020)
--


CREATE TABLE bds_odoo.FlatBom_open_order_Report AS

select
       cbmb.product,
       cbmb.last_updated_on,
       cbmb.reference,
       cbmb.quantity,
       pp.default_code as component,
       mbl.product_qty as Product_Qty,
       mbl.x_studio_militers as Density,
       mbl.x_studio_militersquantity as quantity_in_ML,
       uu.name as Product_unit_of_measure,
       mbl.sequence as Sequence,
       mrw.name as consumed_in_operation,
       cbmb.bomtype as BOM_Type

from
    product_product pp
    left join mrp_bom_line mbl on mbl.product_id = pp.id
    left join mrp_routing_workcenter mrw on mbl.operation_id = mrw.id
    left join uom_uom uu on mbl.product_uom_id=uu.id
    left join calculated_bom_mrp_bom cbmb on mbl.id=cbmb.bom_line_ids
