SELECT code, status, issue_on, created_on, org, total_value, total_quantity, src_stock_code, src_stock, vendor_id, payment_type, note_type, channel, total_discount, seq, src_ancestors, so_chung_tu
FROM v1.invoice
where created_on >= :yesterday and created_on < :runday;