-- Smart Building Energy Optimization - PostgreSQL schema
--
-- Idempotent: safe to run repeatedly (python -m database init).
-- All timestamps are TIMESTAMPTZ. Every record carries its own unique id
-- (event_id / alert_id / action_id) so that Kafka redeliveries are absorbed
-- with ON CONFLICT instead of creating duplicates.

CREATE TABLE IF NOT EXISTS schema_version (
    version     INTEGER PRIMARY KEY,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- --------------------------------------------------------------------------
-- Building registry
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS zones (
    zone_id             TEXT PRIMARY KEY,
    building_id         TEXT NOT NULL,
    name                TEXT NOT NULL,
    zone_type           TEXT NOT NULL,
    floor_area_m2       DOUBLE PRECISION NOT NULL CHECK (floor_area_m2 > 0),
    max_occupancy       INTEGER NOT NULL CHECK (max_occupancy >= 0),
    occupied_setpoint_c DOUBLE PRECISION,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- --------------------------------------------------------------------------
-- Raw validated sensor events (written by workers.sensor_persistence)
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sensor_readings (
    event_id            UUID PRIMARY KEY,
    building_id         TEXT NOT NULL,
    zone_id             TEXT NOT NULL,
    zone_type           TEXT,
    sequence            BIGINT NOT NULL,
    event_time          TIMESTAMPTZ NOT NULL,      -- simulated measurement time
    emitted_at          TIMESTAMPTZ,               -- wall clock when produced
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    interval_seconds    DOUBLE PRECISION NOT NULL CHECK (interval_seconds > 0),
    temperature_c       DOUBLE PRECISION NOT NULL,
    humidity_pct        DOUBLE PRECISION NOT NULL CHECK (humidity_pct BETWEEN 0 AND 100),
    co2_ppm             DOUBLE PRECISION NOT NULL CHECK (co2_ppm >= 0),
    occupancy_count     INTEGER NOT NULL CHECK (occupancy_count >= 0),
    lighting_lux        DOUBLE PRECISION NOT NULL CHECK (lighting_lux >= 0),
    lighting_level_pct  DOUBLE PRECISION NOT NULL CHECK (lighting_level_pct BETWEEN 0 AND 100),
    hvac_mode           TEXT NOT NULL,
    hvac_status         TEXT NOT NULL,
    hvac_setpoint_c     DOUBLE PRECISION NOT NULL,
    hvac_power_kw       DOUBLE PRECISION NOT NULL CHECK (hvac_power_kw >= 0),
    lighting_power_kw   DOUBLE PRECISION NOT NULL CHECK (lighting_power_kw >= 0),
    appliance_power_kw  DOUBLE PRECISION NOT NULL CHECK (appliance_power_kw >= 0),
    total_power_kw      DOUBLE PRECISION NOT NULL CHECK (total_power_kw >= 0),
    energy_kwh          DOUBLE PRECISION NOT NULL CHECK (energy_kwh >= 0),
    outdoor_temperature_c DOUBLE PRECISION,
    outdoor_humidity_pct  DOUBLE PRECISION,
    kafka_partition     INTEGER,
    kafka_offset        BIGINT
);
CREATE INDEX IF NOT EXISTS ix_sensor_readings_zone_time ON sensor_readings (zone_id, event_time DESC);
CREATE INDEX IF NOT EXISTS ix_sensor_readings_time ON sensor_readings (event_time DESC);

-- --------------------------------------------------------------------------
-- Processed energy records with engineered features (workers.energy_processor)
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS energy_records (
    event_id                UUID PRIMARY KEY,      -- same id as the sensor event
    building_id             TEXT NOT NULL,
    zone_id                 TEXT NOT NULL,
    event_time              TIMESTAMPTZ NOT NULL,
    processed_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    interval_seconds        DOUBLE PRECISION NOT NULL,
    -- energy
    energy_kwh              DOUBLE PRECISION NOT NULL CHECK (energy_kwh >= 0),
    hvac_energy_kwh         DOUBLE PRECISION NOT NULL CHECK (hvac_energy_kwh >= 0),
    lighting_energy_kwh     DOUBLE PRECISION NOT NULL CHECK (lighting_energy_kwh >= 0),
    appliance_energy_kwh    DOUBLE PRECISION NOT NULL CHECK (appliance_energy_kwh >= 0),
    total_power_kw          DOUBLE PRECISION NOT NULL,
    -- carbon (NULL until carbon-intensity data is available)
    carbon_intensity_g_per_kwh DOUBLE PRECISION,
    carbon_kg_co2e          DOUBLE PRECISION,
    -- context
    temperature_c           DOUBLE PRECISION NOT NULL,
    outdoor_temperature_c   DOUBLE PRECISION,
    occupancy_count         INTEGER NOT NULL,
    hvac_status             TEXT NOT NULL,
    -- engineered features
    hour_of_day             SMALLINT NOT NULL CHECK (hour_of_day BETWEEN 0 AND 23),
    day_of_week             SMALLINT NOT NULL CHECK (day_of_week BETWEEN 0 AND 6),
    is_weekend              BOOLEAN NOT NULL,
    hour_sin                DOUBLE PRECISION NOT NULL,
    hour_cos                DOUBLE PRECISION NOT NULL,
    indoor_outdoor_delta_c  DOUBLE PRECISION,
    setpoint_deviation_c    DOUBLE PRECISION NOT NULL,
    occupancy_ratio         DOUBLE PRECISION,
    power_density_w_m2      DOUBLE PRECISION,
    hvac_share              DOUBLE PRECISION,
    power_change_kw         DOUBLE PRECISION,
    temperature_rate_c_per_h DOUBLE PRECISION,
    rolling_mean_power_kw   DOUBLE PRECISION NOT NULL,
    rolling_std_power_kw    DOUBLE PRECISION NOT NULL,
    rolling_max_power_kw    DOUBLE PRECISION NOT NULL,
    rolling_window_size     SMALLINT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_energy_records_zone_time ON energy_records (zone_id, event_time DESC);
CREATE INDEX IF NOT EXISTS ix_energy_records_time ON energy_records (event_time DESC);

-- --------------------------------------------------------------------------
-- External data (workers.record_persistence)
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS weather_records (
    event_id                UUID PRIMARY KEY,
    building_id             TEXT NOT NULL,
    observed_at             TIMESTAMPTZ NOT NULL,
    source                  TEXT NOT NULL,          -- e.g. open-meteo, mock
    is_forecast             BOOLEAN NOT NULL DEFAULT FALSE,
    temperature_c           DOUBLE PRECISION NOT NULL,
    humidity_pct            DOUBLE PRECISION CHECK (humidity_pct BETWEEN 0 AND 100),
    wind_speed_ms           DOUBLE PRECISION CHECK (wind_speed_ms >= 0),
    solar_irradiance_w_m2   DOUBLE PRECISION CHECK (solar_irradiance_w_m2 >= 0),
    cloud_cover_pct         DOUBLE PRECISION CHECK (cloud_cover_pct BETWEEN 0 AND 100),
    ingested_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (building_id, source, is_forecast, observed_at)
);
CREATE INDEX IF NOT EXISTS ix_weather_records_time ON weather_records (observed_at DESC);

CREATE TABLE IF NOT EXISTS carbon_intensity_records (
    event_id                UUID PRIMARY KEY,
    region                  TEXT NOT NULL,
    observed_at             TIMESTAMPTZ NOT NULL,
    source                  TEXT NOT NULL,
    is_forecast             BOOLEAN NOT NULL DEFAULT FALSE,
    intensity_g_per_kwh     DOUBLE PRECISION NOT NULL CHECK (intensity_g_per_kwh >= 0),
    ingested_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (region, source, is_forecast, observed_at)
);
CREATE INDEX IF NOT EXISTS ix_carbon_records_time ON carbon_intensity_records (observed_at DESC);

-- --------------------------------------------------------------------------
-- Security alerts. Status only moves forward: open -> acknowledged -> resolved.
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS security_alerts (
    alert_id            UUID PRIMARY KEY,
    zone_id             TEXT,                       -- NULL = building-wide
    source_event_id     UUID,                       -- sensor event that triggered it
    detected_at         TIMESTAMPTZ NOT NULL,
    alert_type          TEXT NOT NULL CHECK (alert_type IN ('anomaly', 'attack', 'drift', 'validation')),
    severity            TEXT NOT NULL CHECK (severity IN ('low', 'medium', 'high', 'critical')),
    detector            TEXT NOT NULL,
    score               DOUBLE PRECISION,
    description         TEXT NOT NULL,
    details             JSONB NOT NULL DEFAULT '{}'::jsonb,
    status              TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'acknowledged', 'resolved')),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at         TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS ix_security_alerts_time ON security_alerts (detected_at DESC);
CREATE INDEX IF NOT EXISTS ix_security_alerts_zone ON security_alerts (zone_id, detected_at DESC);

-- --------------------------------------------------------------------------
-- Optimization / agent action history (audit trail).
-- Status lifecycle: proposed -> approved | rejected -> executed | failed.
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS optimization_actions (
    action_id           UUID PRIMARY KEY,
    zone_id             TEXT NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL,
    updated_at          TIMESTAMPTZ NOT NULL,
    source              TEXT NOT NULL CHECK (source IN ('optimizer', 'agent', 'operator', 'baseline')),
    action_type         TEXT NOT NULL CHECK (action_type IN ('set_setpoint', 'set_hvac_mode', 'set_lighting', 'clear_overrides')),
    parameters          JSONB NOT NULL DEFAULT '{}'::jsonb,
    status              TEXT NOT NULL CHECK (status IN ('proposed', 'approved', 'rejected', 'executed', 'failed')),
    reason              TEXT,
    safety_reason       TEXT,
    related_alert_id    UUID,
    kafka_topic         TEXT
);
CREATE INDEX IF NOT EXISTS ix_optimization_actions_zone ON optimization_actions (zone_id, created_at DESC);
CREATE INDEX IF NOT EXISTS ix_optimization_actions_status ON optimization_actions (status);

-- --------------------------------------------------------------------------
-- Rejected messages (data-quality log / dead letters)
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS invalid_events (
    id                  BIGSERIAL PRIMARY KEY,
    worker              TEXT NOT NULL,
    topic               TEXT NOT NULL,
    kafka_partition     INTEGER NOT NULL,
    kafka_offset        BIGINT NOT NULL,
    event_id            TEXT,                       -- may itself be invalid
    zone_id             TEXT,
    errors              JSONB NOT NULL,
    payload             JSONB NOT NULL,
    received_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (worker, topic, kafka_partition, kafka_offset)
);
CREATE INDEX IF NOT EXISTS ix_invalid_events_time ON invalid_events (received_at DESC);

-- --------------------------------------------------------------------------
-- Analytics views
-- --------------------------------------------------------------------------
CREATE OR REPLACE VIEW hourly_zone_energy AS
SELECT
    zone_id,
    date_trunc('hour', event_time)          AS hour,
    count(*)                                AS readings,
    sum(energy_kwh)                         AS energy_kwh,
    sum(hvac_energy_kwh)                    AS hvac_energy_kwh,
    sum(lighting_energy_kwh)                AS lighting_energy_kwh,
    sum(appliance_energy_kwh)               AS appliance_energy_kwh,
    avg(total_power_kw)                     AS avg_power_kw,
    max(total_power_kw)                     AS peak_power_kw,
    sum(carbon_kg_co2e)                     AS carbon_kg_co2e,
    avg(temperature_c)                      AS avg_temperature_c,
    avg(occupancy_count)                    AS avg_occupancy
FROM energy_records
GROUP BY zone_id, date_trunc('hour', event_time);

INSERT INTO schema_version (version) VALUES (1) ON CONFLICT DO NOTHING;

-- ==========================================================================
-- Version 2: ML energy predictions (workers.prediction_service)
-- ==========================================================================
CREATE TABLE IF NOT EXISTS energy_predictions (
    prediction_id           UUID PRIMARY KEY,
    source_event_id         UUID NOT NULL,          -- sensor event the prediction was made from
    building_id             TEXT NOT NULL,
    zone_id                 TEXT NOT NULL,
    event_time              TIMESTAMPTZ NOT NULL,   -- time of the latest reading used
    predicted_for           TIMESTAMPTZ NOT NULL,   -- end of the predicted interval (= its reading's event_time)
    horizon_seconds         DOUBLE PRECISION NOT NULL CHECK (horizon_seconds > 0),
    predicted_power_kw      DOUBLE PRECISION NOT NULL CHECK (predicted_power_kw >= 0),
    predicted_energy_kwh    DOUBLE PRECISION NOT NULL CHECK (predicted_energy_kwh >= 0),
    model_name              TEXT NOT NULL,
    model_version           TEXT NOT NULL,
    created_at              TIMESTAMPTZ NOT NULL,
    ingested_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (source_event_id, model_version)
);
CREATE INDEX IF NOT EXISTS ix_energy_predictions_zone_time ON energy_predictions (zone_id, predicted_for DESC);

-- Online accuracy: each prediction joined with the reading that ends at predicted_for.
CREATE OR REPLACE VIEW prediction_accuracy AS
SELECT
    p.prediction_id, p.zone_id, p.model_version, p.event_time, p.predicted_for,
    p.predicted_power_kw,
    s.energy_kwh * 3600 / s.interval_seconds                        AS actual_power_kw,
    abs(p.predicted_power_kw - s.energy_kwh * 3600 / s.interval_seconds) AS abs_error_kw
FROM energy_predictions p
JOIN sensor_readings s ON s.zone_id = p.zone_id AND s.event_time = p.predicted_for;

INSERT INTO schema_version (version) VALUES (2) ON CONFLICT DO NOTHING;

-- ==========================================================================
-- Version 3: agentic AI (agent.graph, workers.agent_service)
-- ==========================================================================
-- Agent actions add sensor isolation (a software quarantine of a sensor's
-- readings; no equipment is touched).
ALTER TABLE optimization_actions DROP CONSTRAINT IF EXISTS optimization_actions_action_type_check;
ALTER TABLE optimization_actions ADD CONSTRAINT optimization_actions_action_type_check CHECK (action_type IN
    ('set_setpoint', 'set_hvac_mode', 'set_lighting', 'clear_overrides', 'isolate_sensor', 'restore_sensor'));

-- One row per agent run (incident): diagnosis, impact, recommendations, the
-- tools it called and the outcome of every proposed action.
CREATE TABLE IF NOT EXISTS agent_decisions (
    decision_id         UUID PRIMARY KEY,
    alert_id            UUID,                       -- triggering alert (NULL = manual request)
    zone_id             TEXT,
    started_at          TIMESTAMPTZ NOT NULL,
    finished_at         TIMESTAMPTZ NOT NULL,
    llm_provider        TEXT NOT NULL,              -- anthropic | offline
    llm_model           TEXT NOT NULL,
    diagnosis           TEXT NOT NULL,
    likely_cause        TEXT NOT NULL,
    confidence          TEXT NOT NULL CHECK (confidence IN ('low', 'medium', 'high')),
    energy_impact       TEXT NOT NULL,
    operational_impact  TEXT NOT NULL,
    recommendations     JSONB NOT NULL DEFAULT '[]'::jsonb,
    tool_calls          JSONB NOT NULL DEFAULT '[]'::jsonb,
    actions             JSONB NOT NULL DEFAULT '[]'::jsonb,
    outcome             TEXT NOT NULL,              -- e.g. "2 executed, 1 rejected"
    errors              JSONB NOT NULL DEFAULT '[]'::jsonb
);
CREATE INDEX IF NOT EXISTS ix_agent_decisions_time ON agent_decisions (started_at DESC);
CREATE INDEX IF NOT EXISTS ix_agent_decisions_alert ON agent_decisions (alert_id);

INSERT INTO schema_version (version) VALUES (3) ON CONFLICT DO NOTHING;
