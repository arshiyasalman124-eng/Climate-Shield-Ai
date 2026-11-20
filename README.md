# ClimateShield AI

A responsive local weather and hazard awareness dashboard. Weather and city search use Open-Meteo; browser coordinates are reverse-geocoded through OpenStreetMap Nominatim. Forecast thresholds produce clearly labeled automated risk assessments. The backend checks regional official alert feeds and reports unreachable sources as unavailable. Safety answers use built-in hazard guidance by default, with an optional OpenAI-compatible LLM configured through environment variables. PDF reports are generated from fresh weather data and report-generation metadata is stored in SQLite.

## Run locally

Requirements: Node.js 20+, Python 3.10+, and npm.

1. From this folder, install the frontend tools: `npm install` and `npm install --prefix client`.
2. Install backend requirements: `python -m pip install -r server/requirements.txt`.
3. Optionally copy `server/.env.example` to `server/.env` and configure an LLM API key. Without it, the safety assistant uses built-in practical guidance.
4. Run `npm run dev` and open `http://localhost:5173`. The API listens on `http://localhost:8000`.

Location search, reverse geocoding, weather, air quality, and alert feeds require an internet connection. India uses the NDMA SACHET and IMD CAP RSS feeds, the United States uses the National Weather Service, and other countries use GDACS alerts within 150 km. Weather responses include current US/EU AQI, PM2.5, PM10, UV, and per-hazard trigger metrics/timestamps. Air-quality data degrades gracefully when unavailable. Official warnings are displayed separately from automated forecast-based risk assessments. Hazard thresholds are screening rules and do not predict impacts or replace official warnings. Location reverse lookup is user-triggered and includes OpenStreetMap attribution in the dashboard.

## API

- `GET /api/health` — API and SQLite status
- `GET /api/weather?latitude=…&longitude=…` — forecast, AQI/UV, hazard triggers, and official alerts
- `GET /api/alerts?latitude=…&longitude=…&location_name=…&state=…&country=…&country_code=…` — verified alerts from the source selected for the location
- `GET /api/location/reverse?latitude=…&longitude=…` — reverse geocode browser coordinates for alert routing
- `POST /api/assistant` — hazard-aware safety guidance, with optional LLM integration
- `GET|POST /api/report.pdf` — fresh-data PDF report; POST accepts a JSON body and both methods save report history
- `GET /api/reports` — last ten generated report summaries
- `GET /api/reports/history` — recent report generation records
