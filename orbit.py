import psycopg, os, time, copy, logging, datetime as dt
from typing import List
from collections import namedtuple
from skyfield.api import EarthSatellite, load
from skyfield.toposlib import wgs84


logger = logging.getLogger(__name__)


Notification = namedtuple(
    "Notification",
    ["Service"]
)

Station = namedtuple(
    "Station", 
    ["StnID", "StnName", "Latitude", "Longitude", "Altitude", "MinHorizon"]
)

TLE = namedtuple(
    "TLE", 
    ["NoradID", "SatName", "Line1", "Line2"]
)

Pass = namedtuple(
    "Pass", 
    ["StnID", "StnName", "NoradID", "SatName", "Azimuth", "Elevation", "Start", "End"]
)


def QueryStns(cursor: psycopg.Cursor) -> List[Station]:
    query_stations = """
        SELECT 
            stnid, 
            stnname,
            latitude,
            longitude,
            altitude, 
            minhorizon
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

def QueryTLEs(cursor: psycopg.Cursor) -> List[TLE]:
    query_tles = """
        SELECT
            noradid,
            satname,
            line1, 
            line2 
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

def QueryPasses(cursor: psycopg.Cursor) -> List[Pass]:
    query_passes = """
        SELECT 
            stnid,
            stnname, 
            noradid, 
            satname, 
            azimuth,
            elevation, 
            aos, 
            los
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
                ps[7]
            )
        )

    return passes


def CountNotifs(cursor: psycopg.Cursor) -> int:
    count_notifs = """
        SELECT
            COUNT(service)
        FROM Notifications 
        WHERE service='orbit';
    """

    cursor.execute(count_notifs)
    row = cursor.fetchone()
    num_notifs = int(row[0])

    return num_notifs


def DeleteNotifs(conn: psycopg.Connection, cursor: psycopg.Cursor) -> None:
    delete_notifs = """
        DELETE 
        FROM Notifications
        WHERE service='orbit';
    """
    
    with conn.transaction():
        cursor.execute(delete_notifs)

    conn.commit()

    return

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
                    stn.StnID, 
                    stn.StnName, 
                    tle.NoradID, 
                    tle.SatName, 
                    float(azi.degrees), 
                    float(ele.degrees), 
                    start.isoformat(), 
                    end.isoformat(),
                )
                passes.append(ps)

    return passes

def InsertPasses(conn: psycopg.Connection, cursor: psycopg.Cursor, passes: List[Pass]) -> None:
    insert_passes = """
        INSERT INTO Passes (
            stnid, 
            stnname, 
            noradid, 
            satname, 
            azimuth,
            elevation, 
            aos, 
            los
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """
    with conn.transaction():
        cursor.executemany(insert_passes, passes)
    
    conn.commit()

    return


def main() -> None:
    logging.basicConfig(
        filename="orbital_prediction_service.log",
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        level=logging.DEBUG,
        filemode='a',
    )
    
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(
        logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    )

    logger.addHandler(logging.StreamHandler())
    logger.info("Parsing Environment Variables")
    period = int(os.getenv("PERIOD"))
    db_url = os.getenv("DB_URL")

    while True:
        logger.info("Opening connection to database")
        conn = psycopg.connect(db_url)         
        conn.set_isolation_level(psycopg.IsolationLevel.READ_COMMITTED)
        cursor = conn.cursor()

        horizon = dt.datetime.now(dt.UTC) + dt.timedelta(seconds=period)
        logger.info(f"Next pass computation approximately: {horizon} UTC")
        logger.info("Querying Notifications")
        num_notifs = CountNotifs(cursor)
        logger.info(f"Current Notifications: {num_notifs}")
        
        if num_notifs > 0:
            logger.info("Querying TLEs and Stations")
            tles = QueryTLEs(cursor)
            stns = QueryStns(cursor)
            logger.info(f"Received {len(tles)} TLEs and {len(stns)} Stations")
            logger.info("Computing Passes")
            passes = ComputePasses(stns, tles)
            logger.info("Inserting Computed Passes Into Database")
            InsertPasses(conn, cursor, passes)
            logger.info(f"Inserted {len(passes)} Passes into Database")
        
        if CountNotifs(cursor) > num_notifs:
            logger.info(f"Closing Database connection")
            conn.close()
            continue

        logger.info(f"Deleting Notifications")
        DeleteNotifs(conn, cursor)
        logger.info(f"Closing Database connection")
        conn.close()
        now = dt.datetime.now(dt.UTC)
        
        if now < horizon:
            diff = horizon - now
            logger.info(f"Sleeping for {diff.seconds}")
            time.sleep(diff.seconds)
   
    return 


if __name__ == "__main__":
    main()
