# Schema verification report

Generated 2026-10-08 00:48 UTC by `verify_schema.py` from the live Azure PostgreSQL database.

## Tables and row counts

| Table | Rows | Columns |
|---|---|---|
| dim_device | 20 | 3 |
| dim_location | 5 | 3 |
| fact_event_stream | 1,899,200 | 10 |
| ml_predictions | 1,859,867 | 6 |
| ml_state | 1 | 4 |

## stream.dim_device

| Column | Type | Null? | Default |
|---|---|---|---|
| device_id | integer | NOT NULL | nextval('stream.dim_device_device_id_seq'::regclass) |
| device_code | character varying(50) | NOT NULL |  |
| device_type | character varying(50) | NOT NULL |  |

Constraints:

| Name | Kind | Definition |
|---|---|---|
| dim_device_pkey | PRIMARY KEY | PRIMARY KEY (device_id) |
| dim_device_device_code_key | UNIQUE | UNIQUE (device_code) |

## stream.dim_location

| Column | Type | Null? | Default |
|---|---|---|---|
| location_id | integer | NOT NULL | nextval('stream.dim_location_location_id_seq'::regclass) |
| city | character varying(100) | NOT NULL |  |
| region | character varying(50) | NOT NULL |  |

Constraints:

| Name | Kind | Definition |
|---|---|---|
| dim_location_pkey | PRIMARY KEY | PRIMARY KEY (location_id) |
| dim_location_city_key | UNIQUE | UNIQUE (city) |

## stream.fact_event_stream

| Column | Type | Null? | Default |
|---|---|---|---|
| event_id | bigint | NOT NULL | nextval('stream.fact_event_stream_event_id_seq'::regclass) |
| device_id | integer | NOT NULL |  |
| location_id | integer | NOT NULL |  |
| reading_value | numeric | NOT NULL |  |
| status | character varying(20) |  |  |
| ts | timestamp with time zone | NOT NULL | now() |
| extra | jsonb |  |  |
| is_injected | boolean |  | false |
| is_flagged | boolean |  | false |
| scored | boolean | NOT NULL | false |

Constraints:

| Name | Kind | Definition |
|---|---|---|
| fact_event_stream_device_id_fkey | FOREIGN KEY | FOREIGN KEY (device_id) REFERENCES stream.dim_device(device_id) |
| fact_event_stream_location_id_fkey | FOREIGN KEY | FOREIGN KEY (location_id) REFERENCES stream.dim_location(location_id) |
| fact_event_stream_pkey | PRIMARY KEY | PRIMARY KEY (event_id) |

## stream.ml_predictions

| Column | Type | Null? | Default |
|---|---|---|---|
| event_id | bigint | NOT NULL |  |
| device_id | integer | NOT NULL |  |
| ts | timestamp with time zone | NOT NULL |  |
| predicted | double precision | NOT NULL |  |
| actual | double precision | NOT NULL |  |
| naive_pred | double precision | NOT NULL |  |

Constraints:

| Name | Kind | Definition |
|---|---|---|
| ml_predictions_event_id_fkey | FOREIGN KEY | FOREIGN KEY (event_id) REFERENCES stream.fact_event_stream(event_id) |
| ml_predictions_pkey | PRIMARY KEY | PRIMARY KEY (event_id) |

## stream.ml_state

| Column | Type | Null? | Default |
|---|---|---|---|
| name | text | NOT NULL |  |
| state | bytea | NOT NULL |  |
| last_event_id | bigint | NOT NULL |  |
| updated_at | timestamp with time zone | NOT NULL | now() |

Constraints:

| Name | Kind | Definition |
|---|---|---|
| ml_state_pkey | PRIMARY KEY | PRIMARY KEY (name) |

## Indexes

| Table | Index | Definition |
|---|---|---|
| dim_device | dim_device_device_code_key | CREATE UNIQUE INDEX dim_device_device_code_key ON stream.dim_device USING btree (device_code) |
| dim_device | dim_device_pkey | CREATE UNIQUE INDEX dim_device_pkey ON stream.dim_device USING btree (device_id) |
| dim_location | dim_location_city_key | CREATE UNIQUE INDEX dim_location_city_key ON stream.dim_location USING btree (city) |
| dim_location | dim_location_pkey | CREATE UNIQUE INDEX dim_location_pkey ON stream.dim_location USING btree (location_id) |
| fact_event_stream | fact_event_stream_pkey | CREATE UNIQUE INDEX fact_event_stream_pkey ON stream.fact_event_stream USING btree (event_id) |
| fact_event_stream | idx_fact_device | CREATE INDEX idx_fact_device ON stream.fact_event_stream USING btree (device_id) |
| fact_event_stream | idx_fact_ts | CREATE INDEX idx_fact_ts ON stream.fact_event_stream USING btree (ts) |
| fact_event_stream | idx_fact_unscored | CREATE INDEX idx_fact_unscored ON stream.fact_event_stream USING btree (event_id) WHERE (NOT scored) |
| ml_predictions | idx_pred_ts | CREATE INDEX idx_pred_ts ON stream.ml_predictions USING btree (ts) |
| ml_predictions | ml_predictions_pkey | CREATE UNIQUE INDEX ml_predictions_pkey ON stream.ml_predictions USING btree (event_id) |
| ml_state | ml_state_pkey | CREATE UNIQUE INDEX ml_state_pkey ON stream.ml_state USING btree (name) |

## Expected keys

| Result | Check |
|---|---|
| PASS | dim_device: PRIMARY KEY (device_id) |
| PASS | dim_device: UNIQUE (device_code) |
| PASS | dim_location: PRIMARY KEY (location_id) |
| PASS | dim_location: UNIQUE (city) |
| PASS | fact_event_stream: PRIMARY KEY (event_id) |
| PASS | fact_event_stream: FOREIGN KEY (device_id) -> dim_device |
| PASS | fact_event_stream: FOREIGN KEY (location_id) -> dim_location |
| PASS | ml_predictions: PRIMARY KEY (event_id) |
| PASS | ml_predictions: FOREIGN KEY (event_id) -> fact_event_stream |
| PASS | ml_state: PRIMARY KEY (name) |

## Integrity checks (0 is good)

| Check | Result |
|---|---|
| Readings whose device_id is missing from dim_device | 0 |
| Readings whose location_id is missing from dim_location | 0 |
| Duplicate device_code values in dim_device | 0 |
| Duplicate city values in dim_location | 0 |
| Predictions whose device_id differs from their reading's device_id (ml_predictions.device_id is not a foreign key, so this is checked here) | 0 |
