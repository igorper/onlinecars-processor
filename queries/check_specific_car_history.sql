select
    internal_vehicle_number,
    model,  
    cashprice_raw,
    date(scan_date, 'unixepoch', 'localtime') as scan_date,
    date(licensedate, 'unixepoch', 'localtime') as licensedate
from autos
WHERE
    internal_vehicle_number=71571
    