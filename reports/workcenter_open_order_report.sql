--
--  Author: Vivere BI Team
--  Date: 2021-04-15(Thursday, 15 April 2020)
--


CREATE TABLE bds_odoo.Workcenter_Open_Order_Report AS

select mrw.id as id,mrw.name as operation, mw.name as Workcenter,
       mrw.sequence as Sequenece,mrw.time_cycle_manual as defalut_Duratiom,
       mrw.time_mode_batch as Based_On, mrw.time_mode as Duration_Computation,
       mb.code as Bill_of_Materials

from mrp_routing_workcenter mrw

    left join mrp_workcenter mw on mrw.workcenter_id = mw.id
    left join mrp_bom mb on mrw.bom_id = mb.id