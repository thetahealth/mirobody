# Oura Provider Design

## Provider Info

- **Provider Name**: Oura
- **Provider Slug**: `theta_oura`
- **Description**: Smart ring for sleep, activity, and readiness tracking
- **Auth Type**: OAUTH2

## Config Keys

- `OURA_CLIENT_ID`
- `OURA_CLIENT_SECRET`
- `OURA_REDIRECT_URL`

## OAuth2 Configuration

- **Auth URL**: `https://cloud.ouraring.com/oauth/authorize`
- **Token URL**: `https://api.ouraring.com/oauth/token`
- **Scopes**: `personal daily heartrate workout session spo2`

## API Configuration

- **API Base URL**: `https://api.ouraring.com`

### Endpoints

| Path | data_type | Description |
|------|-----------|-------------|
| `/v2/usercollection/personal_info` | `personal_info` | Age, weight, height, sex |
| `/v2/usercollection/sleep` | `sleep` | Detailed sleep periods with HR, HRV, stages |
| `/v2/usercollection/daily_sleep` | `daily_sleep` | Daily sleep score and contributors |
| `/v2/usercollection/daily_activity` | `daily_activity` | Steps, calories, MET, activity time |
| `/v2/usercollection/daily_readiness` | `daily_readiness` | Readiness score, temperature, HRV balance |
| `/v2/usercollection/heartrate` | `heartrate` | 5-min interval heart rate (uses start_datetime/end_datetime) |
| `/v2/usercollection/daily_spo2` | `daily_spo2` | Daily average SpO2 |
| `/v2/usercollection/daily_stress` | `daily_stress` | Stress/recovery minutes |
| `/v2/usercollection/daily_resilience` | `daily_resilience` | Resilience level and contributors |
| `/v2/usercollection/daily_cardiovascular_age` | `daily_cardiovascular_age` | Vascular age estimate |
| `/v2/usercollection/vo2_max` | `vo2_max` | VO2 max estimate |
| `/v2/usercollection/workout` | `workout` | Workout type, calories, distance, intensity |
| `/v2/usercollection/session` | `session` | Guided sessions (meditation, breathing) |
| `/v2/usercollection/sleep_time` | `sleep_time` | Optimal bedtime recommendation |

## Data Strategy

- **Pull Enabled**: true
- **Pull Interval Hours**: 24
- **Backfill Days**: 30
- **Webhook Enabled**: false (planned for future phase)

## Indicator Mapping

API doc URL for auto-generation: `https://cloud.ouraring.com/v2/docs`

Oura API documentation can also be referenced from open-source clients:
- https://github.com/hedgertronic/oura-ring (Python client with full field definitions)

### Full Mapping Table

| data_type | Oura Field | StandardIndicator | Unit | Storage |
|-----------|-----------|-------------------|------|---------|
| sleep | total_sleep_duration | dailyTotalSleepTime | s | series |
| sleep | time_in_bed | sleepAnalysis_InBed | s | series |
| sleep | awake_time | sleepAnalysis_Awake | s | series |
| sleep | deep_sleep_duration | sleepAnalysis_Asleep(Deep) | s | series |
| sleep | light_sleep_duration | sleepAnalysis_Asleep(Core) | s | series |
| sleep | rem_sleep_duration | sleepAnalysis_Asleep(REM) | s | series |
| sleep | efficiency | sleepEfficiency | % | series |
| sleep | latency | sleepLatency | seconds | series |
| sleep | average_heart_rate | dailyHeartRateAvg | bpm | series |
| sleep | lowest_heart_rate | dailyHeartRateMin | bpm | series |
| sleep | average_hrv | hrvRMSSD | ms | series |
| sleep | average_breath | respiratoryRates | count/min | series |
| sleep | restless_periods | sleepDisturbances | count | series |
| sleep | temperature_delta | temperatureDelta | °C | series |
| daily_sleep | score | sleepOverallScore | score | summary |
| daily_activity | steps | dailySteps | count | summary |
| daily_activity | active_calories | dailyCaloriesActive | kcal | summary |
| daily_activity | total_calories | dailyTotalCalories | kcal | summary |
| daily_activity | equivalent_walking_distance | dailyDistance | m | summary |
| daily_activity | high_activity_time | dailyActivityIntensityHigh | s | summary |
| daily_activity | medium_activity_time | dailyActivityIntensityMedium | s | summary |
| daily_activity | low_activity_time | dailyActivityIntensityLow | s | summary |
| daily_activity | sedentary_time | sedentaryTime | s | summary |
| daily_activity | resting_time | restingTime | s | summary |
| daily_activity | score | dailyActivityScore | score | summary |
| daily_readiness | score | recoveryScore | score | summary |
| daily_readiness | temperature_deviation | temperatureDelta | °C | summary |
| heartrate | bpm | heartRates | count/min | series |
| daily_spo2 | spo2_percentage.average | oxygenSaturations | % | series |
| daily_stress | stress_high | stressLevel | min | summary |
| daily_stress | recovery_high | recoveryes | min | summary |
| vo2_max | vo2_max | vo2Maxs | mL/kg/min | summary |
| daily_cardiovascular_age | vascular_age | bodyAge | year | summary |
| workout | calories | activeCalories | kcal | series |
| workout | distance | walkingRunningDistances | m | series |
| personal_info | weight | bodyMasss | kg | series |
| personal_info | height | heights | m | series |

### Comparison with vital.oura (2026-03-09)

**theta_oura unique indicators** (not in vital.oura):

| Indicator | Source | Description |
|-----------|--------|-------------|
| oxygenSaturations | daily_spo2 | Blood oxygen saturation (SpO2) |
| recoveryScore | daily_readiness | Oura Readiness score (0-100) |
| recoveryes | daily_stress | High recovery state duration per day |
| sleepOverallScore | daily_sleep | Oura Sleep score (0-100) |
| dailyActivityScore | daily_activity | Oura Activity score (0-100) |
| sedentaryTime | daily_activity | Sedentary duration |
| restingTime | daily_activity | Resting duration |
| bodyAge | daily_cardiovascular_age | Vascular age estimate (requires Oura Membership, API returns 401) |
| sleepDisturbances | sleep | Restless periods count during sleep |
| sleepAnalysis_InBed | sleep | Total time in bed |
| bodyMasss / heights | personal_info | Weight and height (vital does not collect these) |

**Naming differences** (same data, different indicator names):

| theta_oura | vital.oura | Meaning |
|-----------|-----------|---------|
| hrvRMSSD | hrvDatas | HRV during sleep |
| dailyHeartRateAvg | sleepHeartRateAverage | Average heart rate during sleep |
| dailyHeartRateMin | sleepHeartRateLowest | Lowest heart rate during sleep |
| dailyTotalSleepTime | totalSleepTime | Total sleep duration |
| sleepOverallScore | sleepQuality | Sleep quality score |

**vital.oura has but theta_oura missing**:

| Indicator | Description | Notes |
|-----------|-------------|-------|
| exerciseMinutes | Workout duration | Could map from workout endpoint `duration` field |
