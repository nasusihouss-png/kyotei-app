import db from "./db.js";

function buildRaceId(race) {
  const yyyymmdd = String(race.date || "").replace(/-/g, "");
  return `${yyyymmdd}_${race.venueId}_${race.raceNo}`;
}

function ensureEntryColumns() {
  const cols = db.prepare("PRAGMA table_info(entries)").all();
  const names = new Set(cols.map((c) => String(c.name)));
  if (!names.has("f_hold_count")) {
    db.exec("ALTER TABLE entries ADD COLUMN f_hold_count INTEGER");
  }
}

export function saveRace(data) {
  ensureEntryColumns();
  const { race, racers } = data;
  const raceId = buildRaceId(race);

  const insertRace = db.prepare(`
    INSERT INTO races (
      race_id,
      race_date,
      venue_id,
      venue_name,
      race_no,
      race_name,
      weather,
      wind_speed,
      wind_dir,
      wave_height
    ) VALUES (
      @race_id,
      @race_date,
      @venue_id,
      @venue_name,
      @race_no,
      @race_name,
      @weather,
      @wind_speed,
      @wind_dir,
      @wave_height
    )
    ON CONFLICT(race_id) DO UPDATE SET
      race_date=excluded.race_date,
      venue_id=excluded.venue_id,
      venue_name=excluded.venue_name,
      race_no=excluded.race_no,
      race_name=excluded.race_name,
      weather=excluded.weather,
      wind_speed=excluded.wind_speed,
      wind_dir=excluded.wind_dir,
      wave_height=excluded.wave_height
  `);

  const deleteEntries = db.prepare("DELETE FROM entries WHERE race_id = ?");
  const insertEntry = db.prepare(`
    INSERT INTO entries (
      race_id,
      lane,
      registration_no,
      name,
      class,
      branch,
      age,
      weight,
      avg_st,
      nationwide_win_rate,
      local_win_rate,
      motor2_rate,
      boat2_rate,
      exhibition_time,
      tilt,
      f_hold_count,
      entry_course,
      exhibition_st
    ) VALUES (
      @race_id,
      @lane,
      @registration_no,
      @name,
      @class,
      @branch,
      @age,
      @weight,
      @avg_st,
      @nationwide_win_rate,
      @local_win_rate,
      @motor2_rate,
      @boat2_rate,
      @exhibition_time,
      @tilt,
      @f_hold_count,
      @entry_course,
      @exhibition_st
    )
  `);

  const tx = db.transaction(() => {
    insertRace.run({
      race_id: raceId,
      race_date: race.date,
      venue_id: race.venueId,
      venue_name: race.venueName ?? null,
      race_no: race.raceNo,
      race_name: race.raceName ?? null,
      weather: race.weather ?? null,
      wind_speed: race.windSpeed ?? null,
      wind_dir: race.windDirection ?? race.windDir ?? null,
      wave_height: race.waveHeight ?? null
    });

    deleteEntries.run(raceId);

    for (const racer of racers) {
      insertEntry.run({
        race_id: raceId,
        lane: racer.lane,
        registration_no: racer.registrationNo ?? null,
        name: racer.name ?? null,
        class: racer.class ?? null,
        branch: racer.branch ?? null,
        age: racer.age ?? null,
        weight: racer.weight ?? null,
        avg_st: racer.avgSt ?? null,
        nationwide_win_rate: racer.nationwideWinRate ?? null,
        local_win_rate: racer.localWinRate ?? null,
        motor2_rate: racer.motor2Rate ?? null,
        boat2_rate: racer.boat2Rate ?? null,
        exhibition_time: racer.exhibitionTime ?? null,
        tilt: racer.tilt ?? null,
        f_hold_count: racer.fHoldCount ?? 0,
        entry_course: racer.entryCourse ?? null,
        exhibition_st: racer.exhibitionSt ?? null
      });
    }
  });

  tx();

  return raceId;
}
