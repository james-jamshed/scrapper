# LinkedIn Profile Scraper — Apify Actor

This actor accepts LinkedIn profile URLs, saves them to Google Drive in a structured folder format, sends them to the processing webhook, and polls for completion status.

## What it does

1. Accepts LinkedIn URLs (manual input or bulk CSV/Sheet upload)
2. Saves URLs to Google Drive in structured folders (one folder per customer)
3. POSTs each URL to the input webhook and receives a `request_id`
4. Polls the stats webhook every N minutes until status = `completed`
5. Saves all results to the Apify dataset

## Input fields

| Field | Required | Description |
|---|---|---|
| `customer_name` | Yes | Creates a Drive subfolder with this name |
| `customer_email` | Yes | Used for delivery identification |
| `service_name` | Yes | Service being requested |
| `service_request_tag_name` | Yes | Tag/category |
| `linkedin_urls` | Optional | Manual list of LinkedIn URLs |
| `bulk_upload_csv` | Optional | Google Sheet or CSV URL with LinkedIn URLs in first column |
| `google_service_account_json` | Optional | Google Service Account JSON for Drive access |
| `drive_folder_id` | Optional | Root Google Drive folder ID |
| `poll_interval_minutes` | Optional | How often to poll stats webhook (default: 3 mins) |

## Webhooks used

- Input: `https://n8n-main.chitlangia.co/webhook/private-profiles-scraper-request`
- Stats: `https://n8n-main.chitlangia.co/webhook/private-profile-export-request-stats`

## Google Drive folder structure created

```
Root Drive Folder (drive_folder_id)
  └── Customer Name/
        └── Customer Name - LinkedIn URLs (Google Sheet)
              ├── linkedin_url
              ├── status
              └── submitted_at
```

## How to set up Google Drive access

1. Go to Google Cloud Console → create a Service Account
2. Download the JSON key
3. Share your Drive folder with the service account email
4. Paste the JSON into `google_service_account_json` input field
5. Paste the Drive folder ID into `drive_folder_id` input field

## Output

Results are saved to the Apify dataset with:
- Summary (total URLs, completed count, spreadsheet URL)
- Per-URL results (request_id, status, webhook response)
