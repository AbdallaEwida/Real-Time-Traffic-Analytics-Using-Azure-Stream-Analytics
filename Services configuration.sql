-- Creating Tables

--  (Overspeed)
CREATE TABLE dbo.OverspeedOutput (
    WindowEnd DATETIMEOFFSET,
    event_id BIGINT,
    plate_number NVARCHAR(50),
    location_id BIGINT,
    location_name NVARCHAR(100),
    road_type NVARCHAR(50),
    speed FLOAT,
    speed_limit FLOAT,
    kmh_over FLOAT,
    sensor_id NVARCHAR(100),
    severity NVARCHAR(10)
);
GO

--  (UnderSpeed)
CREATE TABLE dbo.UnderSpeedOutput (
    WindowEnd DATETIMEOFFSET,
    event_id BIGINT,
    plate_number NVARCHAR(50),
    location_id BIGINT,
    location_name NVARCHAR(100),
    road_type NVARCHAR(50),
    speed FLOAT,
    speed_limit FLOAT,
    sensor_id NVARCHAR(100)
);
GO

--  (Wrong Way Violation)
CREATE TABLE dbo.WrongWayOutput (
    WindowEnd DATETIMEOFFSET,
    event_id BIGINT,
    plate_number NVARCHAR(50),
    location_name NVARCHAR(100),
    VehicleDirection NVARCHAR(10),
    StreetDirection NVARCHAR(10)
);
GO

--  (Congestion)
CREATE TABLE dbo.CongestionOutput (
    WindowEnd DATETIMEOFFSET,
    location_id BIGINT,
    location_name NVARCHAR(100),
    road_type NVARCHAR(50),
    vehicle_count BIGINT,
    avg_speed FLOAT,
    speed_limit_in_area FLOAT
);
GO

--  (Accident)
CREATE TABLE dbo.AccidentOutput (
    WindowEnd DATETIMEOFFSET,
    location_id BIGINT,
    location_name NVARCHAR(100),
    risky_vehicle_count BIGINT,
    avg_speed FLOAT,
    speed_limit_in_area FLOAT
);
GO



-- ASA job Query 
WITH CastedEvents AS (
    SELECT
        System.Timestamp AS EventTime,
        timestamp AS OriginalTimestamp,
        CAST(event_id AS bigint) AS event_id,
        plate_number,
        vehicle_type,
        CAST(location_id AS bigint) AS location_id,
        location_name,
        road_type,
        TRY_CAST(speed AS float) AS speed,
        TRY_CAST(speed_limit AS float) AS speed_limit,
        sensor_id,
        street_direction,
        direction,
    FROM trafficeventsinput TIMESTAMP BY timestamp
    WHERE TRY_CAST(speed AS float) BETWEEN 0 AND 200
)

-- Overspeed with severity
SELECT
    c.EventTime AS WindowEnd,
    c.event_id,
    c.plate_number,
    c.location_id,
    c.location_name,
    c.road_type,
    c.speed,
    c.speed_limit,
    (c.speed - c.speed_limit) AS kmh_over,
    c.sensor_id,
    CASE 
        WHEN c.speed > (c.speed_limit * 1.35) THEN 'High'
        ELSE 'Medium' 
    END AS severity
INTO overspeedoutput
FROM CastedEvents c
WHERE c.speed > (c.speed_limit * 1.15);

-- UnderSpeed (ignore intersections)
SELECT
    c.EventTime AS WindowEnd,
    c.event_id,
    c.plate_number,
    c.location_id,
    c.location_name,
    c.road_type,
    c.speed,
    c.speed_limit,
    c.sensor_id
INTO underspeedoutput
FROM CastedEvents c
WHERE 
    c.speed < (c.speed_limit * 0.8)
    AND c.road_type != 'Intersection';

    -- Wrong Way Violation )
SELECT
    c.EventTime AS WindowEnd,
    c.event_id,
    c.plate_number,
    c.location_name,
    c.direction AS VehicleDirection,
    c.street_direction AS StreetDirection
INTO wrongwayOutput
FROM CastedEvents c
WHERE
    (c.street_direction = 'NS' AND (c.direction = 'E' OR c.direction = 'W'))
    OR
    (c.street_direction = 'EW' AND (c.direction = 'N' OR c.direction = 'S'));

-- Congestion
SELECT
    System.Timestamp AS WindowEnd,
    c.location_id,
    c.location_name,
    MAX(TRY_CAST(c.road_type AS NVARCHAR(MAX))) AS road_type,  
    COUNT_BIG(*) AS vehicle_count,
    AVG(c.speed) AS avg_speed,
    MAX(c.speed_limit) AS speed_limit_in_area
INTO congestionoutput
FROM CastedEvents c
GROUP BY 
    HOPPINGWINDOW(second, 60, 30), 
    c.location_id, 
    c.location_name
HAVING 
    COUNT_BIG(*) >= 3 
    AND AVG(c.speed) < (MAX(c.speed_limit) * 0.6);

-- High-Risk / Accident
SELECT
    System.Timestamp AS WindowEnd,
    c.location_id,
    c.location_name,
    COUNT_BIG(*) AS risky_vehicle_count,
    AVG(c.speed) AS avg_speed,
    MAX(c.speed_limit) AS speed_limit_in_area
INTO accidentoutput
FROM CastedEvents c
WHERE 
    (c.speed > (c.speed_limit * 1.4)) 
    OR (c.speed < 10 AND c.road_type IN ('Highway', 'Main Road'))
GROUP BY TUMBLINGWINDOW(second, 60), c.location_id, c.location_name
HAVING COUNT_BIG(*) >= 3;