
-- Complete business logic details for Open-order-Procurement report without product names

--
--  Author: Vivere BI Team
--  Date: 2021-04-15(Thursday, 15 April 2020)
--



CREATE TABLE bds_odoo.Procurement_Open_order_Report AS

select po.id ,po.name as PO_Number, po.create_date as created_on,
       po.date_approve as Confirmation_Date, po.date_planned as Receipt_Date,
       po.partner_ref as invoice,po.priority as priority,po.state as status,
       po.amount_untaxed as untaxed,po.amount_total as Total_amount,
       po.date_order as Order_Deadline,
       rp.name as vendor,
       ru.login as buyer,
       rc.symbol as currency,
       ai.name as incoterm
from purchase_order po
    left join res_partner rp on po.partner_id=rp.id
    left join res_users ru on po.user_id = ru.id
    left join res_currency rc on po.currency_id= rc.id
    left join account_incoterms ai on po.incoterm_id = ai.id; 
