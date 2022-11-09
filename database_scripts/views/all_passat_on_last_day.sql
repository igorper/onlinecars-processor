SELECT
    internal_vehicle_number,
    model,
    cashprice_raw,
    mileage_km,
    active_tempomat,
    dead_spot_warn,
    kw,
    last_maintenance_km,
    active_info_display,
    navigation,
    panorama_roof,
    parking_assistant,
    reverse_camera,
    seat_heating,
    sleepiness_sensor,
    start_stop,
    traffic_line_assist,
    traffic_sign_recognition,
    trailer_hitch,
    automatic,
    alloy_wheels,
    date(scan_date, 'unixepoch', 'localtime') as scan_date, /*probably could remove, since scan date should be guaranteed by the where */
    date(licensedate, 'unixepoch', 'localtime') as licensedate
from autos
where
    scan_date = (SELECT max(scan_date) from autos) and
    modelgroup = "Passat"
ORDER by cashprice_raw ASC