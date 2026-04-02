CREATE TABLE IF NOT EXISTS races (
  race_id TEXT PRIMARY KEY,
  race_date TEXT NOT NULL,
  venue_id INTEGER NOT NULL,
  venue_name TEXT,
  race_no INTEGER NOT NULL,
  race_name TEXT,
  weather TEXT,
  wind_speed REAL,
  wind_dir TEXT,
  wave_height REAL,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS entries (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  race_id TEXT NOT NULL,
  lane INTEGER NOT NULL,
  registration_no INTEGER,
  name TEXT,
  class TEXT,
  branch TEXT,
  age INTEGER,
  weight REAL,
  avg_st REAL,
  nationwide_win_rate REAL,
  local_win_rate REAL,
  motor2_rate REAL,
  boat2_rate REAL,
  exhibition_time REAL,
  tilt REAL,
  f_hold_count INTEGER,
  entry_course INTEGER,
  exhibition_st REAL,
  FOREIGN KEY (race_id) REFERENCES races(race_id)
);

CREATE TABLE IF NOT EXISTS results (
  race_id TEXT PRIMARY KEY,
  finish_1 INTEGER,
  finish_2 INTEGER,
  finish_3 INTEGER,
  payout_2t INTEGER,
  payout_3t INTEGER,
  decision_type TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS prediction_logs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  race_id TEXT NOT NULL,
  race_key TEXT,
  race_date TEXT,
  venue_code INTEGER,
  venue_name TEXT,
  race_no INTEGER,
  prediction_timestamp TEXT,
  model_version TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  race_pattern TEXT,
  buy_type TEXT,
  risk_score REAL,
  recommendation TEXT,
  top3_json TEXT,
  prediction_json TEXT,
  race_decision_json TEXT,
  probabilities_json TEXT,
  ev_analysis_json TEXT,
  bet_plan_json TEXT,
  FOREIGN KEY (race_id) REFERENCES races(race_id)
);

CREATE TABLE IF NOT EXISTS similar_race_features (
  race_id TEXT PRIMARY KEY,
  prediction_snapshot_id INTEGER,
  race_date TEXT,
  venue_code INTEGER,
  venue_name TEXT,
  race_no INTEGER,
  race_pattern TEXT,
  race_pattern_score REAL,
  boat1_head_pre REAL,
  second_cluster_score REAL,
  near_tie_count INTEGER,
  chaos_level REAL,
  top6_coverage REAL,
  outside_break_risk_pre REAL,
  venue_bias_score REAL,
  venue_bias_json TEXT,
  avg_lap_time REAL,
  avg_exhibition_time REAL,
  entry_signature TEXT,
  predicted_entry_order_json TEXT,
  actual_entry_order_json TEXT,
  entry_confirmed INTEGER,
  style_signature TEXT,
  style_signature_json TEXT,
  style_score_avg REAL,
  lane_rate_json TEXT,
  hard_scenario TEXT,
  hard_scenario_score REAL,
  hard_race_index REAL,
  top6_scenario TEXT,
  top6_scenario_score REAL,
  second_given_head_json TEXT,
  near_tie_second_json TEXT,
  top6_json TEXT,
  optional_active INTEGER NOT NULL DEFAULT 0,
  optional_size INTEGER,
  formation_reason TEXT,
  predicted_head INTEGER,
  racers_feature_json TEXT,
  confidence_score REAL,
  prediction_stability_score REAL,
  recommended_bet_mode TEXT,
  final_result TEXT,
  head_hit INTEGER,
  bet_hit INTEGER,
  top6_hit INTEGER,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS settlement_logs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  race_id TEXT NOT NULL,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  combo TEXT NOT NULL,
  bet_amount INTEGER NOT NULL,
  hit_flag INTEGER NOT NULL,
  payout INTEGER NOT NULL,
  profit_loss INTEGER NOT NULL,
  FOREIGN KEY (race_id) REFERENCES races(race_id)
);

CREATE TABLE IF NOT EXISTS feature_snapshots (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  race_id TEXT NOT NULL,
  lane INTEGER NOT NULL,
  registration_no INTEGER,
  name TEXT,
  class TEXT,
  prediction_score REAL,
  prediction_rank INTEGER,
  predicted_top3_flag INTEGER,
  features_json TEXT NOT NULL,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  UNIQUE (race_id, lane),
  FOREIGN KEY (race_id) REFERENCES races(race_id)
);

CREATE TABLE IF NOT EXISTS race_snapshot_index (
  race_id TEXT PRIMARY KEY,
  race_date TEXT NOT NULL,
  venue_id INTEGER NOT NULL,
  race_no INTEGER NOT NULL,
  venue_name TEXT,
  snapshot_status TEXT NOT NULL DEFAULT 'SNAPSHOT_MISSING',
  entry_count INTEGER NOT NULL DEFAULT 0,
  feature_count INTEGER NOT NULL DEFAULT 0,
  generated_by TEXT,
  last_error_code TEXT,
  last_error_message TEXT,
  metadata_json TEXT,
  generated_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_race_snapshot_index_date_venue_race
  ON race_snapshot_index(race_date, venue_id, race_no);

CREATE INDEX IF NOT EXISTS idx_race_snapshot_index_status
  ON race_snapshot_index(snapshot_status, race_date);

CREATE TABLE IF NOT EXISTS placed_bets (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  race_id TEXT NOT NULL,
  race_date TEXT NOT NULL,
  venue_id INTEGER NOT NULL,
  race_no INTEGER NOT NULL,
  source TEXT DEFAULT 'ai',
  bet_type TEXT DEFAULT 'trifecta',
  copied_from_ai INTEGER DEFAULT 0,
  ai_reference_id TEXT,
  combo TEXT NOT NULL,
  bet_amount INTEGER NOT NULL,
  bought_odds REAL,
  recommended_prob REAL,
  recommended_ev REAL,
  recommended_bet INTEGER,
  memo TEXT,
  hit_flag INTEGER,
  payout INTEGER DEFAULT 0,
  profit_loss INTEGER,
  settled_at TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (race_id) REFERENCES races(race_id)
);

CREATE TABLE IF NOT EXISTS self_learning_snapshots (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  snapshot_date TEXT NOT NULL,
  sample_size INTEGER NOT NULL,
  mode TEXT NOT NULL DEFAULT 'proposal_only',
  current_weights_json TEXT NOT NULL,
  suggested_weights_json TEXT NOT NULL,
  applied_weights_json TEXT,
  summary TEXT
);

CREATE TABLE IF NOT EXISTS race_start_displays (
  race_id TEXT PRIMARY KEY,
  start_display_order_json TEXT,
  start_display_st_json TEXT,
  start_display_positions_json TEXT,
  start_display_signature TEXT,
  start_display_timing_json TEXT,
  start_display_raw_json TEXT,
  start_display_layout_mode TEXT,
  start_display_source TEXT,
  source_fetched_at TEXT,
  prediction_snapshot_json TEXT,
  fetched_result TEXT,
  settled_result TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (race_id) REFERENCES races(race_id)
);

CREATE TABLE IF NOT EXISTS manual_lap_exhibitions (
  race_id TEXT PRIMARY KEY,
  score_scale TEXT DEFAULT '0-2',
  scores_json TEXT NOT NULL,
  race_memo TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (race_id) REFERENCES races(race_id)
);

CREATE TABLE IF NOT EXISTS prediction_feature_logs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  race_id TEXT NOT NULL UNIQUE,
  race_date TEXT,
  venue_id INTEGER,
  venue_name TEXT,
  race_no INTEGER,
  race_grade TEXT,
  weather TEXT,
  wind REAL,
  wave REAL,
  motor_rate_avg REAL,
  boat_rate_avg REAL,
  avg_st_avg REAL,
  exhibition_time_avg REAL,
  start_display_order_json TEXT,
  start_display_st_json TEXT,
  start_display_timing_json TEXT,
  start_display_raw_json TEXT,
  start_display_signature TEXT,
  predicted_entry_order_json TEXT,
  actual_entry_order_json TEXT,
  entry_changed INTEGER,
  entry_change_type TEXT,
  ranking_score REAL,
  recommendation_score REAL,
  confidence REAL,
  recommendation_mode TEXT,
  prediction_snapshot_json TEXT,
  prediction_before_entry_change_json TEXT,
  prediction_after_entry_change_json TEXT,
  actual_result TEXT,
  hit_flag INTEGER,
  settled_bet_hit_count INTEGER,
  settled_bet_count INTEGER,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (race_id) REFERENCES races(race_id)
);

CREATE TABLE IF NOT EXISTS prediction_feature_log_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  race_id TEXT NOT NULL,
  race_key TEXT,
  race_date TEXT,
  venue_id INTEGER,
  venue_name TEXT,
  race_no INTEGER,
  race_grade TEXT,
  weather TEXT,
  wind REAL,
  wave REAL,
  motor_rate_avg REAL,
  boat_rate_avg REAL,
  avg_st_avg REAL,
  exhibition_time_avg REAL,
  start_display_st_json TEXT,
  start_display_signature TEXT,
  predicted_entry_order_json TEXT,
  actual_entry_order_json TEXT,
  entry_changed INTEGER,
  entry_change_type TEXT,
  ranking_score REAL,
  recommendation_score REAL,
  confidence REAL,
  recommendation_mode TEXT,
  prediction_snapshot_json TEXT,
  prediction_before_entry_change_json TEXT,
  prediction_after_entry_change_json TEXT,
  source_timestamp TEXT,
  learning_run_id INTEGER,
  learned_at TEXT,
  actual_result TEXT,
  hit_flag INTEGER,
  settled_bet_hit_count INTEGER,
  settled_bet_count INTEGER,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS learning_weight_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  mode TEXT NOT NULL,
  sample_size INTEGER NOT NULL DEFAULT 0,
  base_weights_json TEXT NOT NULL,
  suggested_weights_json TEXT NOT NULL,
  applied_weights_json TEXT,
  summary TEXT,
  reverted_from_run_id INTEGER
);

CREATE TABLE IF NOT EXISTS learning_weight_state (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  active_weights_json TEXT NOT NULL,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
  last_run_id INTEGER
);

CREATE TABLE IF NOT EXISTS race_verification_logs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  race_id TEXT NOT NULL,
  race_date TEXT,
  venue_code INTEGER,
  venue_name TEXT,
  race_no INTEGER,
  verified_at TEXT DEFAULT CURRENT_TIMESTAMP,
  prediction_snapshot_id INTEGER,
  verified_against_snapshot_id INTEGER,
  verification_status TEXT,
  verification_reason TEXT,
  confirmed_result TEXT,
  head_hit INTEGER,
  bet_hit INTEGER,
  learning_ready INTEGER,
  predicted_top3 TEXT,
  actual_top3 TEXT,
  hit_miss TEXT,
  mismatch_categories_json TEXT,
  verification_summary_json TEXT,
  is_hidden_from_results INTEGER NOT NULL DEFAULT 0,
  is_invalid_verification INTEGER NOT NULL DEFAULT 0,
  exclude_from_learning INTEGER NOT NULL DEFAULT 0,
  invalid_reason TEXT,
  invalidated_at TEXT
);
