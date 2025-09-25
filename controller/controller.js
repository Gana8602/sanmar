const { pool } = require("../db");
// require('dotenv').config();

const SibApiV3Sdk = require("sib-api-v3-sdk");
let defaultClient = SibApiV3Sdk.ApiClient.instance;
let apiKey = defaultClient.authentications["api-key"];
apiKey.apiKey =
  "paste api"; // 👈 replace with your Brevo key
const tranEmailApi = new SibApiV3Sdk.TransactionalEmailsApi();

const sendMail = async (req, res) => {
  const { to, subject, text } = req.body;

  const sender = { email: "ganapathi8602@gmail.com", name: "Tridel Terra 2.0" };
  const receivers = [{ email: to }];

  try {
    await tranEmailApi.sendTransacEmail({
      sender,
      to: receivers,
      subject,
      textContent: text,
    });

    res.json({ success: true, message: "Email sent successfully!" });
  } catch (err) {
    console.error(err);
    res.status(500).json({ success: false, message: "Failed to send email" });
  }
};
const fetchLive = async (req, res) => {
  try {
    const { from, to, type } = req.query;

    if (!from || !to || !type) {
      return res
        .status(400)
        .json({ error: "from, to, and type (obs|pre) are required" });
    }
    console.log("dates= from:", from, "to:", to);

    // Ensure only obs|pre allowed
    const suffix = type === "obs" ? "obs" : "pre";

    // Table names
    const tables = {
      tide: `sm_tide_${suffix}`,
      wind: `sm_wind_${suffix}`,
      wave: `sm_wave_${suffix}`,
      current: `sm_current_${suffix}`,
    };

    // Function to fetch rows from one table
    const fetchTableData = async (tableName) => {
      const query = `
        SELECT * FROM ${tableName}
        WHERE timestamp BETWEEN $1 AND $2
        ORDER BY timestamp ASC
      `;
      const result = await pool.query(query, [from, to]);

      if (result.rows.length > 0) {
        return result.rows;
      }

      // ⚠️ If no rows found → fetch last row instead
      const fallbackQuery = `
        SELECT * FROM ${tableName}
        ORDER BY timestamp DESC
        LIMIT 1
      `;
      const fallbackResult = await pool.query(fallbackQuery);
      return fallbackResult.rows; // either empty [] or 1 row
    };

    // Fetch all in parallel
    const [tide, wind, wave, current] = await Promise.all([
      fetchTableData(tables.tide),
      fetchTableData(tables.wind),
      fetchTableData(tables.wave),
      fetchTableData(tables.current),
    ]);

    // Helper to extract first/last timestamps + one parameter
    const summarizeTable = (rows, param) => {
      if (!rows.length) return null;
      return {
        firstTimestamp: rows[0].timestamp,
        lastTimestamp: rows[rows.length - 1].timestamp,
        sampleValue: rows[0][param] ?? null,
        isFallback: rows.length === 1 && rows[0].timestamp < from, // mark fallback rows
      };
    };

    // Example: choose parameters you want per table
    const summaries = {
      tide: summarizeTable(tide, "tide_height"),
      wind: summarizeTable(wind, "wind_speed"),
      wave: summarizeTable(wave, "wave_height"),
      current: summarizeTable(current, "current_speed"),
    };
    console.log("summary ==", summaries);

    // Response structure
    const data = {
      tide,
      wind,
      wave,
      current,
      summaries,
    };

    res.json({ data });
  } catch (err) {
    console.error("Error fetching live data:", err);
    res.status(500).json({ error: "Internal server error" });
  }
};


const fetchLive2 = async (req, res) => {
  try {
    const { date, type } = req.query;

    if (!date || !type) {
      return res
        .status(400)
        .json({ error: "date (DD-MM-YYYY) and type (obs|pre) are required" });
    }

    console.log("date query:", date);

    // Parse date → convert DD-MM-YYYY → YYYY-MM-DD
    const [day, month, year] = date.split("-");
    const formattedDate = `${year}-${month}-${day}`; // safe ISO date

    // Start & end of that day (UTC safe, since DB timestamps are usually UTC)
    const from = `${formattedDate} 00:00:00`;
    const to = `${formattedDate} 23:59:59`;

    // Ensure only obs|pre allowed
    const suffix = type === "obs" ? "obs" : "pre";

    // Table names
    const tables = {
      tide: `sm_tide_${suffix}`,
      wind: `sm_wind_${suffix}`,
      wave: `sm_wave_${suffix}`,
      current: `sm_current_${suffix}`,
    };

    // Function to fetch rows from one table
    const fetchTableData = async (tableName) => {
      const query = `
        SELECT * FROM ${tableName}
        WHERE timestamp BETWEEN $1 AND $2
        ORDER BY timestamp ASC
      `;
      const result = await pool.query(query, [from, to]);
      return result.rows;
    };

    // Fetch all in parallel
    const [tide, wind, wave, current] = await Promise.all([
      fetchTableData(tables.tide),
      fetchTableData(tables.wind),
      fetchTableData(tables.wave),
      fetchTableData(tables.current),
    ]);

    // Response structure
    const data = {
      tide,
      wind,
      wave,
      current,
    };

    res.json({ data });
  } catch (err) {
    console.error("Error fetching live data:", err);
    res.status(500).json({ error: "Internal server error" });
  }
};


const fetchAverages = async (req, res) => {
  try {
    const { date, table, parameters } = req.query;

    if (!date || !table || !parameters) {
      return res.status(400).json({
        error: "date, table, and parameters[] are required",
      });
    }

    // parameters will come as comma-separated string if sent via query
    // e.g. parameters=water_level,temp
    const paramsArray = Array.isArray(parameters)
      ? parameters
      : parameters.split(",");

    // Allowed tables for safety
    const allowedTables = [
      "sm_tide_obs",
      "sm_tide_pre",
      "sm_wind_obs",
      "sm_wind_pre",
      "sm_wave_obs",
      "sm_wave_pre",
      "sm_current_obs",
      "sm_current_pre",
    ];

    if (!allowedTables.includes(table)) {
      return res.status(400).json({ error: "Invalid table name" });
    }

    // Dynamically build SELECT fields
    const avgFields = paramsArray
      .map((param) => `AVG(${param}) AS avg_${param}`)
      .join(", ");

    const query = `
        SELECT
          DATE_TRUNC('hour', timestamp) AS hour,
          ${avgFields}
        FROM ${table}
        WHERE DATE(timestamp) = $1
        GROUP BY DATE_TRUNC('hour', timestamp)
        ORDER BY hour;
      `;

    const result = await pool.query(query, [date]);

    res.json({ data: result.rows });
  } catch (err) {
    console.error("Error fetching averages:", err);
    res.status(500).json({ error: `Internal server error ${err}` });
  }
};

const fetchTideObs = async (req, res) => {
  console.log("Received request to fetchTideObs", req.query);
  const { fromDate, toDate } = req.query;
  try {
    // Build SQL for raw data with optional station filter
    let rawDataSql = `SELECT * FROM sm_tide_obs WHERE timestamp BETWEEN $1 AND $2`;
    const params = [fromDate, toDate];

    rawDataSql += ` ORDER BY timestamp ASC`;

    // Build SQL for hourly averages
    let avgSql = `
        SELECT
          DATE_TRUNC('hour', timestamp) AS hour,
          AVG(water_level) AS avg_water_level
        FROM sm_tide_obs
        WHERE timestamp BETWEEN $1 AND $2
      `;

    const avgParams = [fromDate, toDate];

    avgSql += `
        GROUP BY DATE_TRUNC('hour', timestamp)
        ORDER BY hour ASC
      `;

    // Execute both queries in parallel
    const [rawDataResult, avgResult] = await Promise.all([
      pool.query(rawDataSql, params),
      pool.query(avgSql, avgParams),
    ]);

    res.json({
      rawData: rawDataResult.rows,
      hourlyAverages: avgResult.rows,
    });
  } catch (error) {
    console.error("Error fetching fetchTideObs data:", error.message);
    saveLog({
      message: "Failed to fetch tideObs. Error: " + error.message,
      location: "controller.fetchTideObs",
      type: "error",
    });
    res.status(500).json({ error: error.message });
  }
};

const fetchAllData = async (req, res) => {
  console.log("Received request to fetchAllData", req.query);
 
  const { fromDate, toDate } = req.query;
 
  try {
    const sql = `
 
          SELECT
            COALESCE(t.timestamp, w.timestamp, c.timestamp, wi.timestamp) AS timestamp,
            t.water_level,
 
            wi.wind_speed,
            wi.wind_direction,
 
            w.significant_wave_height,
            w.tmean,
            w.peak_wave_period,
            w.primary_swell_wave,
            w.primary_swell_period,
            w.primary_swell_direction,
            w.wind_wave_height,
            w.wind_wave_period,
            w.wind_wave_direction,
            w.wave_direction,
           
            c.current_speed,
            c.current_direction,
            c.pressure,
            c.battery,
            c.temperature,
           
            wi.wind_gust
          FROM sm_tide_obs t
          FULL OUTER JOIN sm_wave_obs w
            ON t.timestamp = w.timestamp
          FULL OUTER JOIN sm_current_obs c
            ON COALESCE(t.timestamp, w.timestamp) = c.timestamp
          FULL OUTER JOIN sm_wind_obs wi
            ON COALESCE(t.timestamp, w.timestamp, c.timestamp) = wi.timestamp
          WHERE COALESCE(t.timestamp, w.timestamp, c.timestamp, wi.timestamp) BETWEEN $1 AND $2
          ORDER BY timestamp ASC
        `;
 
    const params = [fromDate, toDate];
    const result = await pool.query(sql, params);
    res.json(result.rows);
  } catch (error) {
    console.error("Error fetching merged data:", error.message);
    res.status(500).json({ error: error.message });
  }
};
 
const fetchAverageData = async (req, res) => {
  console.log("Received request to fetchAverageData", req.query);
  const { fromDate, toDate } = req.query;
  try {
    const sql = `
        WITH averaged AS (
          SELECT
            -- 6-hour time buckets
            DATE_TRUNC('day', COALESCE(t.timestamp, w.timestamp, c.timestamp, wi.timestamp)) +
            (EXTRACT(HOUR FROM COALESCE(t.timestamp, w.timestamp, c.timestamp, wi.timestamp))::int / 6) * 6 * INTERVAL '1 hour' AS six_hour_period,
 
            -- Tide data averages
            AVG(t.water_level) AS avg_water_level,
 
            -- Wave data averages
            AVG(w.significant_wave_height) AS avg_significant_wave_height,
            AVG(w.period_tzc) AS avg_mean_wave_period,
            AVG(w.peak_wave_period) AS avg_peak_wave_period,
            AVG(w.primary_swell_wave) AS avg_primary_swell_wave,
            AVG(w.primary_swell_period) AS avg_primary_swell_period,
            AVG(w.primary_swell_direction) AS avg_primary_swell_direction,
            AVG(w.wind_wave_height) AS avg_wind_wave_height,
            AVG(w.wind_wave_period) AS avg_wind_wave_period,
            AVG(w.wind_wave_direction) AS avg_wind_wave_direction,
            AVG(w.wave_direction) AS avg_wave_direction,
            AVG(w.wave_heading) AS avg_wave_heading,
            AVG(w.p_max) AS avg_p_max,
            AVG(w.hm0) AS avg_hm0,
 
            -- Current data averages
            AVG(c.current_speed) AS avg_current_speed,
            AVG(c.current_direction) AS avg_current_direction,
            AVG(c.pressure) AS avg_pressure,
            AVG(c.battery) AS avg_battery,
            AVG(c.temperature) AS avg_temperature,
 
            -- Wind data averages
            AVG(wi.wind_speed) AS avg_wind_speed,
            AVG(wi.wind_direction) AS avg_wind_direction,
            AVG(wi.wind_gust) AS avg_wind_gust
          FROM sm_tide_obs t
          FULL OUTER JOIN sm_wave_obs w
            ON t.timestamp = w.timestamp
          FULL OUTER JOIN sm_current_obs c
            ON COALESCE(t.timestamp, w.timestamp) = c.timestamp
          FULL OUTER JOIN sm_wind_obs wi
            ON COALESCE(t.timestamp, w.timestamp, c.timestamp) = wi.timestamp
          WHERE COALESCE(t.timestamp, w.timestamp, c.timestamp, wi.timestamp) BETWEEN $1 AND $2
          GROUP BY
            DATE_TRUNC('day', COALESCE(t.timestamp, w.timestamp, c.timestamp, wi.timestamp)) +
            (EXTRACT(HOUR FROM COALESCE(t.timestamp, w.timestamp, c.timestamp, wi.timestamp))::int / 6) * 6 * INTERVAL '1 hour'
        )
        SELECT
          a.*,
          (SELECT MAX(t.timestamp) FROM sm_tide_obs t WHERE t.timestamp BETWEEN $1 AND $2) AS last_tide_timestamp
        FROM averaged a
        ORDER BY six_hour_period ASC;
      `;
 
    const params = [fromDate, toDate];
    const result = await pool.query(sql, params);
    res.json(result.rows);
  } catch (error) {
    console.error("Error fetching fetchAverageData:", error.message);
    saveLog({
      message: "Error: " + error.message,
      location: "controller.fetchAverageData",
      type: "error",
    });
    res.status(500).json({ error: error.message });
  }
};
 
const fetchReportData = async (req, res) => {
  console.log("Received request to fetchReportData", req.query);
 
  const { fromDate, toDate } = req.query;
 
  try {
    const sql = `
      WITH all_data AS (
        SELECT
          COALESCE(t.timestamp, w.timestamp, c.timestamp, wi.timestamp) AS timestamp,
          MAX(t.timestamp) OVER () AS last_tide_timestamp,
          t.water_level,
          w.significant_wave_height,
          w.period_tzc,
          w.peak_wave_period,
          w.primary_swell_wave,
          w.primary_swell_period,
          w.primary_swell_direction,
          w.wind_wave_height,
          w.wind_wave_period,
          w.wind_wave_direction,
          w.wave_direction,
          c.current_speed,
          c.current_direction,
          c.pressure,
          c.temperature,
          wi.wind_speed,
          wi.wind_direction
        FROM sm_tide_obs t
        FULL OUTER JOIN sm_wave_obs w
          ON t.timestamp = w.timestamp
        FULL OUTER JOIN sm_current_obs c
          ON COALESCE(t.timestamp, w.timestamp) = c.timestamp
        FULL OUTER JOIN sm_wind_obs wi
          ON COALESCE(t.timestamp, w.timestamp, c.timestamp) = wi.timestamp
        WHERE COALESCE(t.timestamp, w.timestamp, c.timestamp, wi.timestamp) BETWEEN $1 AND $2
      ),
      hourly_data AS (
        SELECT *,
          EXTRACT(HOUR FROM timestamp) as hour,
          EXTRACT(MINUTE FROM timestamp) as minute,
          -- Prioritize exact hour (00 mins) over 30 mins
          CASE
            WHEN EXTRACT(MINUTE FROM timestamp) = 0 THEN 1
            ELSE 2
          END as priority
        FROM all_data
        WHERE EXTRACT(HOUR FROM timestamp) IN (0, 6, 12, 18)
      ),
      ranked_data AS (
        SELECT *,
          ROW_NUMBER() OVER (
            PARTITION BY DATE(timestamp), EXTRACT(HOUR FROM timestamp)
            ORDER BY priority, timestamp
          ) as rank
        FROM hourly_data
      )
      SELECT
        timestamp,
        last_tide_timestamp,
        water_level,
        significant_wave_height,
        period_tzc,
        peak_wave_period,
        primary_swell_wave,
        primary_swell_period,
        primary_swell_direction,
        wind_wave_height,
        wind_wave_period,
        wind_wave_direction,
        wave_direction,
        current_speed,
        current_direction,
        pressure,
        temperature,
        wind_speed,
        wind_direction
      FROM ranked_data
      WHERE rank = 1
      ORDER BY timestamp ASC
    `;
 
    const params = [fromDate, toDate];
    const result = await pool.query(sql, params);
    res.json(result.rows);
  } catch (error) {
    console.error("Error fetching merged data:", error.message);
    saveLog({
      message: "Error: " + error.message,
      location: "controller.fetchAllData",
      type: "error",
    });
    res.status(500).json({ error: error.message });
  }
};

const fetchDataHealthReport = async (req, res) => {
  console.log("Received request to fetchDataHealthReport", req.query);

  const { fromDate, toDate } = req.query;

  try {
    const sql = `
  WITH all_data AS (
    SELECT
      COALESCE(t.timestamp, w.timestamp, c.timestamp, wi.timestamp) AS timestamp,
      t.water_level,
      w.significant_wave_height,
      w.period_tzc,
      w.peak_wave_period,
      w.primary_swell_wave,
      w.primary_swell_period,
      w.primary_swell_direction,
      w.wind_wave_height,
      w.wind_wave_period,
      w.wind_wave_direction,
      w.wave_direction,
      w.wave_heading,
      w.p_max,
      w.hm0,
      w.lat,
      w.lon,
      c.current_speed,
      c.current_direction,
      c.pressure,
      c.battery,
      c.temperature,
      wi.wind_speed,
      wi.wind_direction,
      wi.wind_gust
    FROM sm_tide_obs t
    FULL OUTER JOIN sm_wave_obs w
      ON t.timestamp = w.timestamp
    FULL OUTER JOIN sm_current_obs c
      ON COALESCE(t.timestamp, w.timestamp) = c.timestamp
    FULL OUTER JOIN sm_wind_obs wi
      ON COALESCE(t.timestamp, w.timestamp, c.timestamp) = wi.timestamp
    WHERE COALESCE(t.timestamp, w.timestamp, c.timestamp, wi.timestamp) BETWEEN $1 AND $2
  ),
  parameter_counts AS (
    SELECT
      COUNT(*) AS total_records,
      COUNT(water_level) FILTER (WHERE water_level IS NOT NULL AND water_level::text != '') AS water_level_present,
      COUNT(significant_wave_height) FILTER (WHERE significant_wave_height IS NOT NULL AND significant_wave_height::text != '') AS significant_wave_height_present,
      COUNT(peak_wave_period) FILTER (WHERE peak_wave_period IS NOT NULL AND peak_wave_period::text != '') AS peak_wave_period_present,
      COUNT(period_tzc) FILTER (WHERE period_tzc IS NOT NULL AND period_tzc::text != '') AS mean_wave_period_present,
      COUNT(primary_swell_wave) FILTER (WHERE primary_swell_wave IS NOT NULL AND primary_swell_wave::text != '') AS primary_swell_wave_present,
      COUNT(primary_swell_period) FILTER (WHERE primary_swell_period IS NOT NULL AND primary_swell_period::text != '') AS primary_swell_period_present,
      COUNT(primary_swell_direction) FILTER (WHERE primary_swell_direction IS NOT NULL AND primary_swell_direction::text != '') AS primary_swell_direction_present,
      COUNT(wind_wave_height) FILTER (WHERE wind_wave_height IS NOT NULL AND wind_wave_height::text != '') AS wind_wave_height_present,
      COUNT(wind_wave_period) FILTER (WHERE wind_wave_period IS NOT NULL AND wind_wave_period::text != '') AS wind_wave_period_present,
      COUNT(wind_wave_direction) FILTER (WHERE wind_wave_direction IS NOT NULL AND wind_wave_direction::text != '') AS wind_wave_direction_present,
      COUNT(wave_direction) FILTER (WHERE wave_direction IS NOT NULL AND wave_direction::text != '') AS wave_direction_present,
      COUNT(wave_heading) FILTER (WHERE wave_heading IS NOT NULL AND wave_heading::text != '') AS wave_heading_present,
      COUNT(p_max) FILTER (WHERE p_max IS NOT NULL AND p_max::text != '') AS p_max_present,
      COUNT(hm0) FILTER (WHERE hm0 IS NOT NULL AND hm0::text != '') AS hm0_present,
      COUNT(lat) FILTER (WHERE lat IS NOT NULL AND lat::text != '') AS lat_present,
      COUNT(lon) FILTER (WHERE lon IS NOT NULL AND lon::text != '') AS lon_present,
      COUNT(current_speed) FILTER (WHERE current_speed IS NOT NULL AND current_speed::text != '') AS current_speed_present,
      COUNT(current_direction) FILTER (WHERE current_direction IS NOT NULL AND current_direction::text != '') AS current_direction_present,
      COUNT(pressure) FILTER (WHERE pressure IS NOT NULL AND pressure::text != '') AS pressure_present,
      COUNT(battery) FILTER (WHERE battery IS NOT NULL AND battery::text != '') AS battery_present,
      COUNT(temperature) FILTER (WHERE temperature IS NOT NULL AND temperature::text != '') AS temperature_present,
      COUNT(wind_speed) FILTER (WHERE wind_speed IS NOT NULL AND wind_speed::text != '') AS wind_speed_present,
      COUNT(wind_direction) FILTER (WHERE wind_direction IS NOT NULL AND wind_direction::text != '') AS wind_direction_present,
      COUNT(wind_gust) FILTER (WHERE wind_gust IS NOT NULL AND wind_gust::text != '') AS wind_gust_present
    FROM all_data
  ),
  last_timestamps AS (
    SELECT
      MAX(timestamp) FILTER (WHERE table_name = 'tide') AS last_tide_timestamp,
      MAX(timestamp) FILTER (WHERE table_name = 'wave') AS last_wave_timestamp,
      MAX(timestamp) FILTER (WHERE table_name = 'current') AS last_current_timestamp,
      MAX(timestamp) FILTER (WHERE table_name = 'wind') AS last_wind_timestamp
    FROM (
      SELECT 'tide' AS table_name, MAX(timestamp) AS timestamp FROM sm_tide_obs
      UNION ALL
      SELECT 'wave', MAX(timestamp) FROM sm_wave_obs
      UNION ALL
      SELECT 'current', MAX(timestamp) FROM sm_current_obs
      UNION ALL
      SELECT 'wind', MAX(timestamp) FROM sm_wind_obs
    ) sub
  ),
  sensor_stats AS (
    SELECT
      -- Tide
      AVG(water_level) AS water_level_mean,
      PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY water_level) AS water_level_median,
      STDDEV_POP(water_level) AS water_level_stddev,

      AVG(significant_wave_height) AS swh_mean,
      PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY significant_wave_height) AS swh_median,
      STDDEV_POP(significant_wave_height) AS swh_stddev,

      AVG(peak_wave_period) AS peak_wave_period_mean,
      PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY peak_wave_period) AS peak_wave_period_median,
      STDDEV_POP(peak_wave_period) AS peak_wave_period_stddev,

      AVG(hm0) AS hm0_mean,
      PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY hm0) AS hm0_median,
      STDDEV_POP(hm0) AS hm0_stddev,

      -- Current
      AVG(current_speed) AS current_speed_mean,
      PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY current_speed) AS current_speed_median,
      STDDEV_POP(current_speed) AS current_speed_stddev,

      AVG(current_direction) AS current_direction_mean,
      PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY current_direction) AS current_direction_median,
      STDDEV_POP(current_direction) AS current_direction_stddev,

      -- Wind
      AVG(wind_speed) AS wind_speed_mean,
      PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY wind_speed) AS wind_speed_median,
      STDDEV_POP(wind_speed) AS wind_speed_stddev,

      AVG(wind_direction) AS wind_direction_mean,
      PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY wind_direction) AS wind_direction_median,
      STDDEV_POP(wind_direction) AS wind_direction_stddev,

      AVG(wind_gust) AS wind_gust_mean,
      PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY wind_gust) AS wind_gust_median,
      STDDEV_POP(wind_gust) AS wind_gust_stddev

    FROM all_data
  )
  SELECT
    pc.total_records,
    -- percentages
    CASE WHEN pc.total_records = 0 THEN 0 ELSE ROUND((water_level_present * 100.0 / pc.total_records), 2) END AS water_level_percentage,
    CASE WHEN pc.total_records = 0 THEN 0 ELSE ROUND((significant_wave_height_present * 100.0 / pc.total_records), 2) END AS significant_wave_height_percentage,
    CASE WHEN pc.total_records = 0 THEN 0 ELSE ROUND((mean_wave_period_present * 100.0 / pc.total_records), 2) END AS mean_wave_period_percentage,
    CASE WHEN pc.total_records = 0 THEN 0 ELSE ROUND((peak_wave_period_present * 100.0 / pc.total_records), 2) END AS peak_wave_period_percentage,
    CASE WHEN pc.total_records = 0 THEN 0 ELSE ROUND((primary_swell_wave_present * 100.0 / pc.total_records), 2) END AS primary_swell_wave_percentage,
    CASE WHEN pc.total_records = 0 THEN 0 ELSE ROUND((primary_swell_period_present * 100.0 / pc.total_records), 2) END AS primary_swell_period_percentage,
    CASE WHEN pc.total_records = 0 THEN 0 ELSE ROUND((primary_swell_direction_present * 100.0 / pc.total_records), 2) END AS primary_swell_direction_percentage,
    CASE WHEN pc.total_records = 0 THEN 0 ELSE ROUND((wind_wave_height_present * 100.0 / pc.total_records), 2) END AS wind_wave_height_percentage,
    CASE WHEN pc.total_records = 0 THEN 0 ELSE ROUND((wind_wave_period_present * 100.0 / pc.total_records), 2) END AS wind_wave_period_percentage,
    CASE WHEN pc.total_records = 0 THEN 0 ELSE ROUND((wind_wave_direction_present * 100.0 / pc.total_records), 2) END AS wind_wave_direction_percentage,
    CASE WHEN pc.total_records = 0 THEN 0 ELSE ROUND((wave_direction_present * 100.0 / pc.total_records), 2) END AS wave_direction_percentage,
    CASE WHEN pc.total_records = 0 THEN 0 ELSE ROUND((wave_heading_present * 100.0 / pc.total_records), 2) END AS wave_heading_percentage,
    CASE WHEN pc.total_records = 0 THEN 0 ELSE ROUND((p_max_present * 100.0 / pc.total_records), 2) END AS p_max_percentage,
    CASE WHEN pc.total_records = 0 THEN 0 ELSE ROUND((hm0_present * 100.0 / pc.total_records), 2) END AS hm0_percentage,
    CASE WHEN pc.total_records = 0 THEN 0 ELSE ROUND((lat_present * 100.0 / pc.total_records), 2) END AS lat_percentage,
    CASE WHEN pc.total_records = 0 THEN 0 ELSE ROUND((lon_present * 100.0 / pc.total_records), 2) END AS lon_percentage,
    CASE WHEN pc.total_records = 0 THEN 0 ELSE ROUND((current_speed_present * 100.0 / pc.total_records), 2) END AS current_speed_percentage,
    CASE WHEN pc.total_records = 0 THEN 0 ELSE ROUND((current_direction_present * 100.0 / pc.total_records), 2) END AS current_direction_percentage,
    CASE WHEN pc.total_records = 0 THEN 0 ELSE ROUND((pressure_present * 100.0 / pc.total_records), 2) END AS pressure_percentage,
    CASE WHEN pc.total_records = 0 THEN 0 ELSE ROUND((battery_present * 100.0 / pc.total_records), 2) END AS battery_percentage,
    CASE WHEN pc.total_records = 0 THEN 0 ELSE ROUND((temperature_present * 100.0 / pc.total_records), 2) END AS temperature_percentage,
    CASE WHEN pc.total_records = 0 THEN 0 ELSE ROUND((wind_speed_present * 100.0 / pc.total_records), 2) END AS wind_speed_percentage,
    CASE WHEN pc.total_records = 0 THEN 0 ELSE ROUND((wind_direction_present * 100.0 / pc.total_records), 2) END AS wind_direction_percentage,
    CASE WHEN pc.total_records = 0 THEN 0 ELSE ROUND((wind_gust_present * 100.0 / pc.total_records), 2) END AS wind_gust_percentage,

    -- overall health
    CASE WHEN pc.total_records = 0 THEN 0 ELSE ROUND((
      (water_level_present +
      significant_wave_height_present +
      mean_wave_period_present +
      peak_wave_period_present +
      primary_swell_wave_present +
      primary_swell_period_present +
      primary_swell_direction_present +
      wind_wave_height_present +
      wind_wave_period_present +
      wind_wave_direction_present +
      wave_direction_present +
      wave_heading_present +
      p_max_present +
      hm0_present +
      lat_present +
      lon_present +
      current_speed_present +
      current_direction_present +
      pressure_present +
      battery_present +
      temperature_present +
      wind_speed_present +
      wind_direction_present +
      wind_gust_present) * 100.0 / (pc.total_records * 22)
    ), 2) END AS overall_health_percentage,

    -- latest timestamps from each table
    lt.last_tide_timestamp,
    lt.last_wave_timestamp,
    lt.last_current_timestamp,
    lt.last_wind_timestamp,
    ss.*

  FROM parameter_counts pc
  CROSS JOIN last_timestamps lt
  CROSS JOIN sensor_stats ss;
`;

    const params = [fromDate, toDate];
    const result = await pool.query(sql, params);

    res.json(result.rows[0]);
  } catch (error) {
    console.error("Error fetching data health report:", error.message);
    saveLog({
      message: "Error: " + error.message,
      location: "controller.fetchDataHealthReport",
      type: "error",
    });
    res.status(500).json({ error: error.message });
  }
};

const fetchDataHealthChart = async (req, res) => {
  console.log("Received request to fetchDataHealthChart", req.query);

  const { fromDate, toDate } = req.query;

  try {
    const sql = `
          WITH date_range AS (
            SELECT
              $1::timestamp AS start_date,
              $2::timestamp AS end_date,
              ($2::timestamp - $1::timestamp) / 15 AS interval_length
          ),
          time_buckets AS (
            SELECT
              generate_series(
                start_date,
                end_date - interval_length,
                interval_length
              ) AS bucket_start,
              generate_series(
                start_date + interval_length,
                end_date,
                interval_length
              ) AS bucket_end
            FROM date_range
          ),
          all_data AS (
            SELECT
              COALESCE(t.timestamp, w.timestamp, c.timestamp, wi.timestamp) AS timestamp,

              -- Tide parameters
              t.water_level,

              -- Wave parameters
              w.significant_wave_height,
              w.period_tzc,
              w.peak_wave_period,
              w.primary_swell_wave,
              w.primary_swell_period,
              w.primary_swell_direction,
              w.wind_wave_height,
              w.wind_wave_period,
              w.wind_wave_direction,
              w.wave_direction,
              w.wave_heading,
              w.p_max,
              w.hm0,
              w.lat,
              w.lon,

              -- Current parameters
              c.current_speed,
              c.current_direction,
              c.pressure,
              c.battery,
              c.temperature,

              -- Wind parameters
              wi.wind_speed,
              wi.wind_direction,
              wi.wind_gust
            FROM sm_tide_obs t
            FULL OUTER JOIN sm_wave_obs w
              ON t.timestamp = w.timestamp
            FULL OUTER JOIN sm_current_obs c
              ON COALESCE(t.timestamp, w.timestamp) = c.timestamp
            FULL OUTER JOIN sm_wind_obs wi
              ON COALESCE(t.timestamp, w.timestamp, c.timestamp) = wi.timestamp
            WHERE COALESCE(t.timestamp, w.timestamp, c.timestamp, wi.timestamp)
                  BETWEEN $1 AND $2
          ),
          bucket_data AS (
            SELECT
              tb.bucket_start,
              tb.bucket_end,
              COUNT(*) AS total_records,

              -- Tide health
              SUM(CASE WHEN ad.water_level IS NOT NULL THEN 1 ELSE 0 END)::float
                / NULLIF(COUNT(ad.water_level), 0) AS water_level_health,

              -- Wave health
              SUM(CASE WHEN ad.significant_wave_height IS NOT NULL THEN 1 ELSE 0 END)::float
                / NULLIF(COUNT(ad.significant_wave_height), 0) AS significant_wave_height_health,
              SUM(CASE WHEN ad.period_tzc IS NOT NULL THEN 1 ELSE 0 END)::float
                / NULLIF(COUNT(ad.period_tzc), 0) AS mean_wave_period_health,
              SUM(CASE WHEN ad.peak_wave_period IS NOT NULL THEN 1 ELSE 0 END)::float
                / NULLIF(COUNT(ad.peak_wave_period), 0) AS peak_wave_period_health,
              SUM(CASE WHEN ad.primary_swell_wave IS NOT NULL THEN 1 ELSE 0 END)::float
                / NULLIF(COUNT(ad.primary_swell_wave), 0) AS primary_swell_wave_health,
              SUM(CASE WHEN ad.primary_swell_period IS NOT NULL THEN 1 ELSE 0 END)::float
                / NULLIF(COUNT(ad.primary_swell_period), 0) AS primary_swell_period_health,
              SUM(CASE WHEN ad.primary_swell_direction IS NOT NULL THEN 1 ELSE 0 END)::float
                / NULLIF(COUNT(ad.primary_swell_direction), 0) AS primary_swell_direction_health,
              SUM(CASE WHEN ad.wind_wave_height IS NOT NULL THEN 1 ELSE 0 END)::float
                / NULLIF(COUNT(ad.wind_wave_height), 0) AS wind_wave_height_health,
              SUM(CASE WHEN ad.wind_wave_period IS NOT NULL THEN 1 ELSE 0 END)::float
                / NULLIF(COUNT(ad.wind_wave_period), 0) AS wind_wave_period_health,
              SUM(CASE WHEN ad.wind_wave_direction IS NOT NULL THEN 1 ELSE 0 END)::float
                / NULLIF(COUNT(ad.wind_wave_direction), 0) AS wind_wave_direction_health,
              SUM(CASE WHEN ad.wave_direction IS NOT NULL THEN 1 ELSE 0 END)::float
                / NULLIF(COUNT(ad.wave_direction), 0) AS wave_direction_health,
              SUM(CASE WHEN ad.wave_heading IS NOT NULL THEN 1 ELSE 0 END)::float
                / NULLIF(COUNT(ad.wave_heading), 0) AS wave_heading_health,
              SUM(CASE WHEN ad.p_max IS NOT NULL THEN 1 ELSE 0 END)::float
                / NULLIF(COUNT(ad.p_max), 0) AS p_max_health,
              SUM(CASE WHEN ad.hm0 IS NOT NULL THEN 1 ELSE 0 END)::float
                / NULLIF(COUNT(ad.hm0), 0) AS hm0_health,
              SUM(CASE WHEN ad.lat IS NOT NULL THEN 1 ELSE 0 END)::float
                / NULLIF(COUNT(ad.lat), 0) AS lat_health,
              SUM(CASE WHEN ad.lon IS NOT NULL THEN 1 ELSE 0 END)::float
                / NULLIF(COUNT(ad.lon), 0) AS lon_health,

              -- Current health
              SUM(CASE WHEN ad.current_speed IS NOT NULL THEN 1 ELSE 0 END)::float
                / NULLIF(COUNT(ad.current_speed), 0) AS current_speed_health,
              SUM(CASE WHEN ad.current_direction IS NOT NULL THEN 1 ELSE 0 END)::float
                / NULLIF(COUNT(ad.current_direction), 0) AS current_direction_health,
              SUM(CASE WHEN ad.pressure IS NOT NULL THEN 1 ELSE 0 END)::float
                / NULLIF(COUNT(ad.pressure), 0) AS pressure_health,
              SUM(CASE WHEN ad.battery IS NOT NULL THEN 1 ELSE 0 END)::float
                / NULLIF(COUNT(ad.battery), 0) AS battery_health,
              SUM(CASE WHEN ad.temperature IS NOT NULL THEN 1 ELSE 0 END)::float
                / NULLIF(COUNT(ad.temperature), 0) AS temperature_health,

              -- Wind health
              SUM(CASE WHEN ad.wind_speed IS NOT NULL THEN 1 ELSE 0 END)::float
                / NULLIF(COUNT(ad.wind_speed), 0) AS wind_speed_health,
              SUM(CASE WHEN ad.wind_direction IS NOT NULL THEN 1 ELSE 0 END)::float
                / NULLIF(COUNT(ad.wind_direction), 0) AS wind_direction_health,
              SUM(CASE WHEN ad.wind_gust IS NOT NULL THEN 1 ELSE 0 END)::float
                / NULLIF(COUNT(ad.wind_gust), 0) AS wind_gust_health

            FROM time_buckets tb
            LEFT JOIN all_data ad
              ON ad.timestamp >= tb.bucket_start AND ad.timestamp < tb.bucket_end
            GROUP BY tb.bucket_start, tb.bucket_end
          )
          SELECT
            bucket_start AS timestamp,
            bucket_end,
            total_records,
            COALESCE(water_level_health, 0) AS water_level_health,
            COALESCE(significant_wave_height_health, 0) AS significant_wave_height_health,
            COALESCE(mean_wave_period_health, 0) AS mean_wave_period_health,
            COALESCE(peak_wave_period_health, 0) AS peak_wave_period_health,
            COALESCE(primary_swell_wave_health, 0) AS primary_swell_wave_health,
            COALESCE(primary_swell_period_health, 0) AS primary_swell_period_health,
            COALESCE(primary_swell_direction_health, 0) AS primary_swell_direction_health,
            COALESCE(wind_wave_height_health, 0) AS wind_wave_height_health,
            COALESCE(wind_wave_period_health, 0) AS wind_wave_period_health,
            COALESCE(wind_wave_direction_health, 0) AS wind_wave_direction_health,
            COALESCE(wave_direction_health, 0) AS wave_direction_health,
            COALESCE(wave_heading_health, 0) AS wave_heading_health,
            COALESCE(p_max_health, 0) AS p_max_health,
            COALESCE(hm0_health, 0) AS hm0_health,
            COALESCE(lat_health, 0) AS lat_health,
            COALESCE(lon_health, 0) AS lon_health,
            COALESCE(current_speed_health, 0) AS current_speed_health,
            COALESCE(current_direction_health, 0) AS current_direction_health,
            COALESCE(pressure_health, 0) AS pressure_health,
            COALESCE(battery_health, 0) AS battery_health,
            COALESCE(temperature_health, 0) AS temperature_health,
            COALESCE(wind_speed_health, 0) AS wind_speed_health,
            COALESCE(wind_direction_health, 0) AS wind_direction_health,
            COALESCE(wind_gust_health, 0) AS wind_gust_health
          FROM bucket_data
          ORDER BY bucket_start`;

    const params = [fromDate, toDate];
    const result = await pool.query(sql, params);

    // The result will contain multiple rows (15-20) with health scores for each time bucket
    res.json(result.rows);
  } catch (err) {
    console.error("Error fetching data health chart:", err.message);
    saveLog({
      message: "Error: " + err.message,
      location: "controller.fetchDataHealthChart",
      type: "error",
    });
    res.status(500).json({ error: err.message });
  }
};

const fetchWindData = async (req, res) => {
  try {
    // Query 1: Get all raw wind data
    const windQuery = `
      SELECT id, timestamp, station_id, wind_speed, wind_direction, wind_gust
      FROM sm_wind_obs
      WHERE timestamp >= DATE_TRUNC('day', NOW())
      ORDER BY timestamp ASC
    `;
    const windResult = await pool.query(windQuery);

    // Query 2: Get hourly averages for wind_speed, wind_direction, wind_gust
    const avgWindQuery = `
      SELECT
        DATE_TRUNC('hour', timestamp) AS ts_hour,
        EXTRACT(HOUR FROM timestamp) AS hour,
        DATE(timestamp) AS date,
        AVG(wind_speed)::numeric(10,2) AS avg_speed,
        AVG(wind_direction)::numeric(10,2) AS avg_direction,
        AVG(wind_gust)::numeric(10,2) AS avg_gust
      FROM sm_wind_obs
      WHERE timestamp >= NOW() - INTERVAL '7 days'
      GROUP BY ts_hour, hour, DATE(timestamp)
      ORDER BY ts_hour
    `;
    const avgWindResult = await pool.query(avgWindQuery);

    // Send both results
    res.json({
      windData: windResult.rows,
      averageData: avgWindResult.rows,
    });
  } catch (err) {
    console.error("Error fetching wind data:", err);
    res.status(500).json({ error: "Internal server error" });
  }
};

const fetchCurrentData = async (req, res) => {
  try {
    // First query: Get all current data
    const currentQuery = `
      SELECT id, timestamp, station_id, current_speed, current_direction,
             pressure, battery, temperature
      FROM sm_current_obs
      WHERE timestamp >= NOW() - INTERVAL '7 days'
      ORDER BY timestamp ASC
    `;
    const currentResult = await pool.query(currentQuery);

    // Second query: Get hourly average data
    const avgQuery = `
      SELECT
          DATE_TRUNC('hour', timestamp) AS ts_hour,
          EXTRACT(HOUR FROM timestamp) AS hour,
          DATE(timestamp) AS date,
          AVG(current_speed)::numeric(10,2) AS avg_speed,
          AVG(current_direction)::numeric(10,2) AS avg_direction
      FROM sm_current_obs
      WHERE timestamp >= NOW() - INTERVAL '7 days'
      GROUP BY ts_hour, hour, DATE(timestamp)
      ORDER BY ts_hour
    `;
    const avgResult = await pool.query(avgQuery);

    // Third query: Get 24hours Data
    const current24Query = `
      SELECT id, timestamp, station_id, current_speed, current_direction
      FROM sm_current_obs
      WHERE timestamp >= NOW() - INTERVAL '24 hours'
      ORDER BY timestamp ASC
    `;
    const current24Result = await pool.query(current24Query);

    // Fouth query: Lat and Long
    const latLong = `
      SELECT lat, lon, timestamp
      FROM sm_wave_obs
      ORDER BY timestamp DESC
      LIMIT 1
    `;
    const latLongResult = await pool.query(latLong);
 
    // Send four results
    res.json({
      currentData: currentResult.rows,
      averageData: avgResult.rows,
      current24Data: current24Result.rows,
      latLongData: latLongResult.rows,
      
    });
  } catch (err) {
    console.error("Error fetching data:", err);
    res.status(500).json({ error: "Internal server error" });
  }
};

const insertLogs = async (req, res) => {
  try {
    const { message, location, type } = req.body;

    if (!message) {
      return res.status(400).json({ error: "Message is required" });
    }

    const query = `
      INSERT INTO sm_logs (message, location, type, log_time)
      VALUES ($1, $2, $3, $4)
      RETURNING id
    `;

    const values = [message, location || null, type || null, new Date()];

    const result = await pool.query(query, values);

    res.json({ message: "Log inserted successfully", id: result.rows[0].id });
  } catch (error) {
    console.error("Error inserting log:", error);
    res.status(500).json({ error: "Internal server error" });
  }
};

const saveLog = async ({ message, location, type }) => {
  try {
    const query = `
      INSERT INTO sm_logs (message, location, type, log_time)
      VALUES ($1, $2, $3, $4)
      RETURNING id
    `;
    const values = [message, location || null, type || null, new Date()];
    const result = await pool.query(query, values);
    console.log("Log inserted successfully, id:", result.rows[0].id);
  } catch (error) {
    console.error("Error inserting log:", error);
  }
};

module.exports = {
  sendMail,
  fetchLive,
  fetchAverages,
  fetchTideObs,
  fetchAllData,
  fetchAverageData,
  fetchDataHealthReport,
  fetchDataHealthChart,
  fetchWindData,
  fetchCurrentData,
  insertLogs,
  fetchReportData,
  fetchLive2
};
