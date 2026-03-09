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

CREATE TABLE IF NOT EXISTS placed_bets (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  race_id TEXT NOT NULL,
  race_date TEXT NOT NULL,
  venue_id INTEGER NOT NULL,
  race_no INTEGER NOT NULL,
  source TEXT DEFAULT 'ai',
  bet_type TEXT DEFAULT 'trifecta',
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
