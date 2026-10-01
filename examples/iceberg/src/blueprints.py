"""Which source views and which hospitals fan out into models — one model per
(hospital, view) pair, exactly like intelligence-src-entity-raw. The real image
lists a few hundred `viewreader.v*` names here (most commented out); this demo
keeps four so a run is quick to read."""

source_hospitals = ["ste", "sts"]

source_tables = [
    "vCodes_CareUnits",
    "vCodes_Drugs",
    "vCodes_Professions",
    "vCodes_Users",
]
