# HMS Icelandic Real Estate — Automated Data Pipeline

Pulls HMS's daily "Kaupskrá fasteigna" (property transaction register) CSV,
cleans it, and pushes it to a Google Sheet that a Tableau Public dashboard
reads from live. GitHub Actions runs the whole thing once a day for free.

```
HMS CSV (updated daily) → GitHub Actions (cron) → update_data.py → Google Sheet → Tableau Public (auto-refreshes every 24h)
```

## Why Google Sheets in the middle?

Tableau Public cannot auto-refresh from an arbitrary CSV URL — you'd have to
manually re-publish the workbook every time you wanted new data. The one
data source Tableau Public *does* auto-refresh (every 24 hours) is Google
Sheets. So the pipeline's job is to keep a Google Sheet in sync with HMS's
source file, and Tableau does the rest on its own schedule.

## One-time setup

### 1. Create the Google Sheet
Create a blank Google Sheet. Copy the spreadsheet ID out of its URL:
`https://docs.google.com/spreadsheets/d/`**`THIS_PART`**`/edit`

### 2. Create a Google Cloud service account
1. In [Google Cloud Console](https://console.cloud.google.com/), create (or reuse) a project.
2. Enable the **Google Sheets API** and **Google Drive API**.
3. Create a **Service Account**, then create a JSON key for it and download it.
4. Open your Google Sheet, click **Share**, and share it (Editor access) with
   the service account's email address (looks like
   `something@project-id.iam.gserviceaccount.com` — it's inside the JSON key file).

### 3. Create the GitHub repo and add secrets
1. Push this folder to a new GitHub repo.
2. In the repo, go to **Settings → Secrets and variables → Actions** and add:
   - `GOOGLE_SERVICE_ACCOUNT_JSON` — paste the **entire contents** of the JSON key file
   - `SPREADSHEET_ID` — the ID you copied in step 1
3. That's it — the workflow in `.github/workflows/update-data.yml` will run
   daily at 07:00 UTC, and you can also trigger it manually from the
   **Actions** tab (`Run workflow`) to test it right away.

### 4. Point Tableau Public at the Sheet
1. Open Tableau Public Desktop → **Connect → Google Sheets**.
2. Sign in and select the same spreadsheet.
3. You'll see two tabs: `transactions` (row-level data) and
   `monthly_summary` (pre-aggregated by month/municipality/property type —
   use this for headline trend charts, it'll render much faster).
4. Build your dashboard, then **Publish to Tableau Public**.
5. Tableau Public will silently re-pull from the Sheet every 24 hours from
   then on — no manual republishing needed.

## Running locally (no Google Sheets, just to sanity check the transform)

```bash
pip install -r requirements.txt
python update_data.py
```

Without the two env vars set, the script writes `transactions.csv` and
`monthly_summary.csv` to the current folder instead of pushing to Sheets,
so you can eyeball the output first.

## Data notes

- Source: HMS "Kaupskrá fasteigna", updated daily by HMS.
  https://hms.is/gogn-og-maelabord/grunngogntilnidurhals/kaupskra-fasteigna
- `onothaefur_samningur` flags contracts HMS itself considers unreliable for
  index/comparison purposes (e.g. sales between relatives, non-arm's-length
  deals). It's kept as a column rather than silently dropped, so you can
  filter it in Tableau — the monthly summary table already excludes them.
- `verd_per_fm` (price per m²) is calculated as `kaupverd / einflm`.
- If the source file grows large enough to risk Google Sheets' 10M-cell
  limit, the script automatically drops the oldest year(s) from the
  granular `transactions` tab to stay under the limit — the
  `monthly_summary` tab is unaffected either way, so long-run trends stay
  intact.

## Attribution

Per HMS's reuse terms, dashboards built on this data should credit HMS as
the source (e.g. "Byggir á upplýsingum frá HMS").
