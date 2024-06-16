use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Aircraft {
    pub icao24: String,
    pub registration: String,
    pub manufacturericao: String,
    pub manufacturername: String,
    pub model: String,
    pub typecode: String,
    pub serialnumber: String,
    pub linenumber: String,
    pub icaoaircrafttype: String,
    pub operator: String,
    pub operatorcallsign: String,
    pub operatoricao: String,
    pub operatoriata: String,
    pub owner: String,
    pub testreg: String,
    pub registered: String,
    pub reguntil: String,
    pub status: String,
    pub built: String,
    pub firstflightdate: String,
    pub seatconfiguration: String,
    pub engines: String,
    pub modes: String,
    pub adsb: String,
    pub acars: String,
    pub notes: String,
    #[serde(rename = "categoryDescription")]
    pub category_description: String,
}

// Implement the Display trait for Aircraft
impl std::fmt::Display for Aircraft {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} - {}", self.icao24, self.registration)
    }
}
