use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub struct Aircraft {
    icao24: String,
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

impl Aircraft {
    pub fn is_valid(&self) -> bool {
        self.icao24 != ""
    }

    pub fn icao24_to_uppercase(&mut self) {
        self.icao24 = self.icao24.to_uppercase();
    }
}
