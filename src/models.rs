use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub struct Aircraft {
    pub icao24: String,
    timestamp: String,
    acars: String,
    adsb: String,
    built: String,
    #[serde(rename = "categoryDescription")]
    category_description: String,
    country: String,
    engines: String,
    #[serde(rename = "firstFlightDate")]
    firstflightdate: String,
    #[serde(rename = "firstSeen")]
    first_seen: String,
    #[serde(rename = "icaoAircraftClass")]
    icao_aircraft_class: String,
    #[serde(rename = "lineNumber")]
    line_number: String,
    #[serde(rename = "manufacturerIcao")]
    manufacturer_icao: String,
    #[serde(rename = "manufacturerName")]
    manufacturer_name: String,
    model: String,
    modes: String,
    #[serde(rename = "nextReg")]
    next_reg: String,
    operator: String,
    #[serde(rename = "operatorCallsign")]
    operator_callsign: String,
    #[serde(rename = "operatorIata")]
    operator_iata: String,
    #[serde(rename = "operatorIcao")]
    operator_icao: String,
    owner: String,
    #[serde(rename = "prevReg")]
    prev_reg: String,
    #[serde(rename = "regUntil")]
    reg_until: String,
    registered: String,
    registration: String,
    #[serde(rename = "selCal")]
    sel_cal: String,
    #[serde(rename = "serialNumber")]
    serial_number: String,
    status: String,
    typecode: String,
    vdl: String,
}
