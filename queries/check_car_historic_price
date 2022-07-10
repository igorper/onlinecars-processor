SELECT  
    MIN(cashprice_raw) - MAX(cashprice_raw) as price_change,
    model,
    date(MIN(scan_date), 'unixepoch', 'localtime') as first_day_price,
    date(MAX(scan_date), 'unixepoch', 'localtime') as last_day_price,
    internal_vehicle_number 
FROM autos 
GROUP BY internal_vehicle_number
ORDER BY MAX(cashprice_raw) - MIN(cashprice_raw) DESC