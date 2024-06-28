use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub struct Aircraft {
    pub icao24: String,
    registration: String,
    manufacturericao: String,
    manufacturername: String,
    model: String,
    typecode: String,
    serialnumber: String,
    linenumber: String,
    icaoaircrafttype: String,
    operator: String,
    operatorcallsign: String,
    operatoricao: String,
    operatoriata: String,
    owner: String,
    testreg: String,
    registered: String,
    reguntil: String,
    status: String,
    built: String,
    firstflightdate: String,
    seatconfiguration: String,
    engines: String,
    modes: String,
    adsb: String,
    acars: String,
    notes: String,
    #[serde(rename = "categoryDescription")]
    category_description: String,
}
