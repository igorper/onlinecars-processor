SELECT
    date(scan_date, 'unixepoch', 'localtime') as scan_date, /*probably could remove, since scan date should be guaranteed by the where */
    date(licensedate, 'unixepoch', 'localtime') as licensedate ,
    *
from autos
where
    internal_vehicle_number IN (
    select
    /*GROUP_CONCAT(date(scan_date, 'unixepoch', 'localtime'), ',') as list_of_dates,*/ /* only used for debugging to see all the dates */
    internal_vehicle_number
    from autos
    group by
    internal_vehicle_number
    having
    count(distinct scan_date) = 1 AND
    scan_date = (SELECT max(scan_date) from autos))