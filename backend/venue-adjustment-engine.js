const STABLE_INNER_VENUES = new Set([
  2, 4, 10, 11, 14, 16, 18, 22
]);

const VOLATILE_VENUES = new Set([
  3, 5, 9, 13, 17, 20, 21, 24
]);

function toVenueId(value) {
  const n = Number(value);
  return Number.isInteger(n) ? n : 0;
}

function laneBonusAdjustByVenue(lane, innerLaneMultiplier) {
  if (!Number.isFinite(innerLaneMultiplier) || innerLaneMultiplier <= 1) return 0;

  // Inner-lane bonus emphasis for venue profile.
  if (lane === 1) return (innerLaneMultiplier - 1) * 4.0;
  if (lane === 2) return (innerLaneMultiplier - 1) * 2.2;
  if (lane === 3) return (innerLaneMultiplier - 1) * 1.0;
  return 0;
}

export function getVenueAdjustments(venueIdInput) {
  const venueId = toVenueId(venueIdInput);

  let innerLaneMultiplier = 1.0;
  if (STABLE_INNER_VENUES.has(venueId)) innerLaneMultiplier = 1.12;

  let chaosAdjustment = 0;
  if (VOLATILE_VENUES.has(venueId)) chaosAdjustment = 8;
  else if (STABLE_INNER_VENUES.has(venueId)) chaosAdjustment = -4;

  return {
    venueId,
    innerLaneMultiplier,
    chaosAdjustment,
    isStableInnerVenue: STABLE_INNER_VENUES.has(venueId),
    isVolatileVenue: VOLATILE_VENUES.has(venueId)
  };
}

export function applyVenueAdjustments(racersWithFeatures, raceContext = {}) {
  const venue = getVenueAdjustments(raceContext?.venueId);

  const adjusted = (racersWithFeatures || []).map((item) => {
    const f = item.features || {};
    const lane = Number(f.lane || item?.racer?.lane || 0);

    return {
      ...item,
      features: {
        ...f,
        venue_inner_lane_multiplier: venue.innerLaneMultiplier,
        venue_lane_adjustment: laneBonusAdjustByVenue(lane, venue.innerLaneMultiplier),
        venue_chaos_adjustment: venue.chaosAdjustment
      }
    };
  });

  return {
    racersWithFeatures: adjusted,
    venue
  };
}
