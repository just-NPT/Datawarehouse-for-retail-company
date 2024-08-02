SELECT  discount_rate, quantity_change, issue_type, issue_code, price, invoice_code, product_code, discount, quantity, unit, pkg_quantity FROM v1.invoice_items
where invoice_code in (
  select code
  FROM v1.invoice
  where created_on >= :yesterday and created_on < :runday
);