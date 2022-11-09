SELECT
        MIN(cashprice_raw) - MAX(cashprice_raw) as price_change,
        AVG(cashprice_raw) != MIN(cashprice_raw) as has_price_changed,
    model,
    date(MIN(scan_date), 'unixepoch', 'localtime') as first_day_price,
    date(MAX(scan_date), 'unixepoch', 'localtime') as last_day_price,
    internal_vehicle_number
FROM autos
GROUP BY internal_vehicle_number
HAVING MAX(scan_date) = (SELECT MAX(scan_date) from autos)
ORDER BY MAX(cashprice_raw) - MIN(cashprice_raw) DESC