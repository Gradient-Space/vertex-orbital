import sqlite3, random, datetime as dt
from typing import List
from collections import namedtuple
from skyfield.api import EarthSatellite, load
from skyfield.toposlib import wgs84

Station = namedtuple(
    "Station", 
    ["StnName", "StnID", "Latitude", "Longitude", "Altitude", "MinHorizon"]
)

TLE = namedtuple(
    "TLE", 
    ["SatName", "SatID", "Line1", "Line2"]
)

Pass = namedtuple(
    "Pass", 
    ["StnName", "StnID", "SatName", "SatID", "Azimuth", "Elevation", "Start", "End", "Scheduled"]
)


def QueryStns(cursor: sqlite3.Cursor) -> List[Station]:
    query_stations = """
        SELECT 
            StnName,
            StnID, 
            Latitude,
            Longitude,
            Altitude, 
            MinHorizon
        FROM Stations;
    """

    stn_rows = cursor.execute(query_stations)
    stns = []

    for stn in stn_rows.fetchall():
        stns.append(
            Station(
                stn[0], 
                stn[1], 
                stn[2], 
                stn[3], 
                stn[4], 
                stn[5]
            )
        )

    return stns

def QueryTLEs(cursor: sqlite3.Cursor) -> List[TLE]:
    query_tles = """
        SELECT
            SatName,
            SatID, 
            Line1, 
            Line2 
        FROM TLEs;
    """

    tle_rows = cursor.execute(query_tles)
    tles = []

    for tle in tle_rows.fetchall():
        tles.append(
            TLE(
                tle[0], 
                tle[1], 
                tle[2], 
                tle[3]
            )
        )

    return tles

def QueryPasses(cursor: sqlite3.Cursor) -> List[Pass]:
    query_passes = """
        SELECT 
            StnName, 
            StnID,
            SatName, 
            SatID, 
            Azimuth,
            Elevation, 
            Start, 
            End,
            Scheduled
        FROM Passes;
    """

    pass_rows = cursor.execute(query_passes)
    passes = []

    for ps in pass_rows.fetchall():
        passes.append(
            Pass(
                ps[0], 
                ps[1], 
                ps[2], 
                ps[3], 
                ps[4], 
                ps[5],
                ps[6],
                ps[7],
                ps[8]
            )
        )

    return passes

def ComputePasses(stns: List[Station], tles: List[TLE]) -> List[Pass]:
    bools = [True, False]
    passes = []

    for stn in stns:
        stn_wgs84 = wgs84.latlon(stn.Latitude, stn.Longitude)

        for tle in tles:
            sat = EarthSatellite(tle.Line1, tle.Line2, name=tle.SatName, ts=load.timescale())
            start = load.timescale().now()
            end = start + dt.timedelta(days=1)
            t, events = sat.find_events(stn_wgs84, start, end, altitude_degrees=stn.MinHorizon)
            starts, ends = [], []

            for ti, event in zip(t, events):
                match event:
                    case 0:
                        starts.append(ti.utc_datetime())

                    case 1: 
                        continue

                    case 2:
                        ends.append(ti.utc_datetime())

            for start, end in zip(starts, ends):
                diff = sat - stn_wgs84
                ts = load.timescale().from_datetime(start)
                topo_pos = diff.at(ts)
                ele, azi, _ = topo_pos.altaz()
                ps = Pass(
                    stn.StnName, 
                    stn.StnID, 
                    tle.SatName, 
                    tle.SatID, 
                    float(azi.degrees), 
                    float(ele.degrees), 
                    start.isoformat(), 
                    end.isoformat(),
                    random.choice(bools)
                )
                passes.append(ps)

    return passes

def InsertPasses(conn: sqlite3.Connection, cursor: sqlite3.Cursor, passes: List[Pass]) -> None:
    insert_passes = """
        INSERT INTO Passes (
            StnName, 
            StnID, 
            SatName, 
            SatID, 
            Azimuth,
            Elevation, 
            Start, 
            End,
            Scheduled
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
    """

    cursor.executemany(insert_passes, passes)
    conn.commit()

    return

def main() -> None:
    conn = sqlite3.connect("./auroranet.db")
    cursor = conn.cursor()
    tles = QueryTLEs(cursor)
    stns = QueryStns(cursor)
    passes = ComputePasses(stns, tles)
    InsertPasses(conn, cursor, passes)
    passes = QueryPasses(cursor)
    
    for ps in passes:
        print(ps)
    
    conn.close()

    return 


if __name__ == "__main__":
    main()
