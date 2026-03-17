function clamp(min, max, value) {
  if (!Number.isFinite(value)) return min;
  return Math.max(min, Math.min(max, value));
}

function estimateRate(nationwideWinRate, classScore, laneType) {
  const nwr = Number.isFinite(nationwideWinRate) ? nationwideWinRate : 0;
  const cs = Number.isFinite(classScore) ? classScore : 0;

  // Convert win-rate/class into percentage-like course stats when source data is unavailable.
  if (laneType === "course1_win_rate") {
    return clamp(0, 100, nwr * 7.5 + cs * 2.5);
  }
  if (laneType === "course1_2rate") {
    return clamp(0, 100, nwr * 10.5 + cs * 4.5);
  }
  if (laneType === "course2_2rate") {
    return clamp(0, 100, nwr * 9.8 + cs * 4.0);
  }
  if (laneType === "course3_3rate") {
    return clamp(0, 100, nwr * 11.0 + cs * 4.0);
  }
  if (laneType === "course4_3rate") {
    return clamp(0, 100, nwr * 10.0 + cs * 3.5);
  }

  return 0;
}

function toRate(raw, nationwideWinRate, classScore, laneType) {
  const hasExplicitValue = raw !== null && raw !== undefined && String(raw).trim() !== "";
  const v = hasExplicitValue ? Number(raw) : NaN;
  if (hasExplicitValue && Number.isFinite(v)) {
    return clamp(0, 100, v);
  }
  return estimateRate(nationwideWinRate, classScore, laneType);
}

function calcCourseFitScore(f) {
  const actualLane = Number.isFinite(Number(f.actual_lane)) ? Number(f.actual_lane) : Number(f.lane || 0);
  switch (actualLane) {
    case 1:
      return f.course1_win_rate * 0.5 + f.course1_2rate * 0.2;
    case 2:
      return f.course2_2rate * 0.4;
    case 3:
      return f.course3_3rate * 0.35;
    case 4:
      return f.course4_3rate * 0.3;
    case 5:
    case 6:
      return f.nationwide_win_rate * 0.1;
    default:
      return 0;
  }
}

export function applyCoursePerformanceFeatures(racersWithFeatures) {
  return (racersWithFeatures || []).map((item) => {
    const f = item.features || {};
    const nationwideWinRate = Number(f.nationwide_win_rate || 0);
    const classScore = Number(f.class_score || 0);

    const course1_win_rate = toRate(
      f.course1_win_rate,
      nationwideWinRate,
      classScore,
      "course1_win_rate"
    );
    const course1_2rate = toRate(
      f.course1_2rate,
      nationwideWinRate,
      classScore,
      "course1_2rate"
    );
    const course2_2rate = toRate(
      f.course2_2rate,
      nationwideWinRate,
      classScore,
      "course2_2rate"
    );
    const course3_3rate = toRate(
      f.course3_3rate,
      nationwideWinRate,
      classScore,
      "course3_3rate"
    );
    const course4_3rate = toRate(
      f.course4_3rate,
      nationwideWinRate,
      classScore,
      "course4_3rate"
    );

    const enrichedFeatures = {
      ...f,
      course1_win_rate,
      course1_2rate,
      course2_2rate,
      course3_3rate,
      course4_3rate
    };

    return {
      ...item,
      features: {
        ...enrichedFeatures,
        course_fit_score: Number(calcCourseFitScore(enrichedFeatures).toFixed(4))
      }
    };
  });
}
