SELECT
    modelgroup,
    round(AVG(days_to_sell),2) as average_days_to_sell
FROM (
         SELECT
             date(MIN(scan_date), 'unixepoch', 'localtime') as first_day_price,
     date(MAX(scan_date), 'unixepoch', 'localtime') as last_day_price,
    julianday(date(MAX(scan_date), 'unixepoch', 'localtime')) - julianday(date(MIN(scan_date), 'unixepoch', 'localtime')) as days_to_sell,
        internal_vehicle_number,
        modelgroup,
        *
FROM autos
GROUP BY internal_vehicle_number
HAVING MAX(scan_date) < (SELECT MAX(scan_date) from autos)
ORDER BY MAX(scan_date) DESC
    )
GROUP BY modelgroup
ORDER BY AVG(days_to_sell) DESC