from enum import Enum

class UnitType(str, Enum):
    ENGINE = "engine"
    LADDER = "ladder"
    RESCUE = "rescue"
    EMS = "ems"
    PATROL = "patrol"
    ARFF = "arff"
    TOW = "tow"
    COMMAND = "command"

UNIT_LABEL = {
    UnitType.ENGINE: "Engine",
    UnitType.LADDER: "Ladder",
    UnitType.RESCUE: "Rescue/USAR",
    UnitType.EMS: "EMS/Ambulance",
    UnitType.PATROL: "Patrol/LEO",
    UnitType.ARFF: "ARFF",
    UnitType.TOW: "Tow",
    UnitType.COMMAND: "Command/BC",
}

class Status(str, Enum):
    OFFDUTY = "offduty"
    AVAILABLE = "available"
    ENROUTE = "enroute"
    ONSCENE = "onscene"
    TRANSPORT = "transport"
    CLEARING = "clearing"
    OOS = "outofservice"

RP_SKILLS = [
    "Firefighting","EMS","HazMat","TechRescue","ARFF","LEO-Tactics",
    "Command","Driving","Comms","Investigation"
]
