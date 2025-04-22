from enum import Enum, auto


class GameMode(Enum):
    Osu = "0"
    Taiko = "1"
    Catch = "2"
    Mania = "3"


class BanchoPyMode(Enum):
    OsuStandard = "0"
    TaikoStandard = "1"
    FruitsStandard = "2"
    ManiaStandard = "3"

    OsuRelax = "4"
    TaikoRelax = "5"
    FruitsRelax = "6"
    # ManiaRelax = '7'

    OsuAutopilot = "8"
    # TaikoAutopilot = '9'
    # FruitsAutopilot = '10'
    # ManiaAutopilot = '11'


class ScoreStatus(Enum):
    DNF = "0"
    Normal = "1"
    Picked = "2"


class MapStatus(Enum):
    NotSubmitted = "-1"
    Pending = "0"
    UpdateAvailable = "1"
    Ranked = "2"
    Approved = "3"
    Qualified = "4"
    Loved = "5"
