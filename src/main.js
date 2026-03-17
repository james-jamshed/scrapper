import { Actor, log } from 'apify';
import { google } from 'googleapis';
import axios from 'axios';
import { parse } from 'csv-parse/sync';

// ─── Webhook URLs ─────────────────────────────────────────────────────────────
const INPUT_WEBHOOK_URL =
  'https://n8n-main.chitlangia.co/webhook/private-profiles-scraper-request';
const STATS_WEBHOOK_URL =
  'https://n8n-main.chitlangia.co/webhook/private-profile-export-request-stats';

// ─── Helper: sleep ────────────────────────────────────────────────────────────
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ─── Helper: Validate LinkedIn URL ───────────────────────────────────────────
function isValidLinkedInUrl(url) {
  return (
    typeof url === 'string' &&
    url.trim().startsWith('https://www.linkedin.com/in/')
  );
}

// ─── Google Drive: Get or create folder ──────────────────────────────────────
async function getOrCreateFolder(drive, folderName, parentId) {
  log.info(`Checking for folder: "${folderName}" under parent: ${parentId}`);
  const res = await drive.files.list({
    q: `name='${folderName}' and mimeType='application/vnd.google-apps.folder' and '${parentId}' in parents and trashed=false`,
    fields: 'files(id, name)',
  });
  if (res.data.files.length > 0) {
    log.info(`Found existing folder: ${res.data.files[0].id}`);
    return res.data.files[0].id;
  }
  const folder = await drive.files.create({
    requestBody: {
      name: folderName,
      mimeType: 'application/vnd.google-apps.folder',
      parents: [parentId],
    },
    fields: 'id',
  });
  log.info(`Created new folder: ${folder.data.id}`);
  return folder.data.id;
}

// ─── Google Sheets: Create sheet with LinkedIn URLs ──────────────────────────
async function createLinkedInSheet(sheets, drive, folderId, customerName, urls) {
  log.info(`Creating Google Sheet for customer: ${customerName}`);
  const sheetTitle = `${customerName} - LinkedIn URLs`;
  const spreadsheet = await sheets.spreadsheets.create({
    requestBody: {
      properties: { title: sheetTitle },
      sheets: [
        {
          properties: { title: 'LinkedIn URLs' },
          data: [
            {
              startRow: 0,
              startColumn: 0,
              rowData: [
                {
                  values: [
                    { userEnteredValue: { stringValue: 'linkedin_url' } },
                    { userEnteredValue: { stringValue: 'status' } },
                    { userEnteredValue: { stringValue: 'submitted_at' } },
                  ],
                },
                ...urls.map((url) => ({
                  values: [
                    { userEnteredValue: { stringValue: url } },
                    { userEnteredValue: { stringValue: 'pending' } },
                    { userEnteredValue: { stringValue: new Date().toISOString() } },
                  ],
                })),
              ],
            },
          ],
        },
      ],
    },
  });
  const spreadsheetId = spreadsheet.data.spreadsheetId;
  log.info(`Created spreadsheet: ${spreadsheetId}`);
  await drive.files.update({
    fileId: spreadsheetId,
    addParents: folderId,
    removeParents: 'root',
    fields: 'id, parents',
  });
  log.info(`Moved spreadsheet into customer folder`);
  return {
    spreadsheetId,
    spreadsheetUrl: `https://docs.google.com/spreadsheets/d/${spreadsheetId}`,
  };
}

// ─── Fetch URLs from bulk CSV/Sheet link ─────────────────────────────────────
async function fetchUrlsFromBulk(bulkUrl) {
  log.info(`Fetching bulk URLs from: ${bulkUrl}`);
  let csvData;
  if (bulkUrl.includes('docs.google.com/spreadsheets')) {
    const sheetId = bulkUrl.match(/\/d\/([a-zA-Z0-9-_]+)/)?.[1];
    if (!sheetId) throw new Error('Invalid Google Sheet URL');
    const exportUrl = `https://docs.google.com/spreadsheets/d/${sheetId}/export?format=csv`;
    const response = await axios.get(exportUrl);
    csvData = response.data;
  } else {
    const response = await axios.get(bulkUrl);
    csvData = response.data;
  }
  const records = parse(csvData, { skip_empty_lines: true });
  const urls = records.flat().map((v) => String(v).trim()).filter(isValidLinkedInUrl);
  log.info(`Found ${urls.length} valid LinkedIn URLs from bulk upload`);
  return urls;
}

// ─── POST to Input Webhook ────────────────────────────────────────────────────
async function postToInputWebhook(payload) {
  log.info(`Posting to input webhook...`);
  log.info(`Payload: ${JSON.stringify(payload)}`);

  const response = await axios.post(INPUT_WEBHOOK_URL, payload, {
    headers: { 'Content-Type': 'application/json' },
    timeout: 30000,
  });

  // FIX 2: Log full raw response to see exact field names
  log.info(`RAW INPUT WEBHOOK RESPONSE: ${JSON.stringify(response.data)}`);
  log.info(`Response status code: ${response.status}`);

  // FIX 2: Try all possible request_id field name variations
  const requestId =
    response.data?.request_id ||
    response.data?.requestId ||
    response.data?.id ||
    response.data?.requestID ||
    response.data?.ReqId ||
    response.data?.data?.request_id ||
    response.data?.data?.requestId ||
    response.data?.data?.id ||
    null;

  if (!requestId) {
    log.warning(`Could not find request_id in response: ${JSON.stringify(response.data)}`);
    return { requestId: null, rawResponse: response.data };
  }

  log.info(`Received request_id: ${requestId}`);
  return { requestId, rawResponse: response.data };
}

// ─── FIX 3: Call Stats Webhook — try POST first, fall back to GET ─────────────
async function callStatsWebhook(requestId) {
  let response;
  try {
    log.info(`Trying POST to stats webhook with request_id: ${requestId}`);
    response = await axios.post(
      STATS_WEBHOOK_URL,
      { request_id: requestId },
      { headers: { 'Content-Type': 'application/json' }, timeout: 30000 }
    );
    log.info(`POST to stats webhook succeeded`);
  } catch (postErr) {
    log.warning(`POST failed: ${postErr.message} — trying GET fallback...`);
    response = await axios.get(STATS_WEBHOOK_URL, {
      params: { request_id: requestId },
      timeout: 30000,
    });
    log.info(`GET to stats webhook succeeded`);
  }
  log.info(`RAW STATS RESPONSE: ${JSON.stringify(response.data)}`);
  return response.data;
}

// ─── Poll Stats Webhook ───────────────────────────────────────────────────────
async function pollStatsWebhook(requestId, rawResponse, pollIntervalMs, maxAttempts = 40) {
  log.info(`Starting poll | request_id: ${requestId} | interval: ${pollIntervalMs / 1000}s | max: ${maxAttempts} attempts`);

  if (!requestId) {
    log.warning(`No request_id — skipping poll. Raw input webhook response: ${JSON.stringify(rawResponse)}`);
    return { status: 'no_request_id', data: rawResponse };
  }

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    log.info(`--- Poll attempt ${attempt}/${maxAttempts} ---`);
    try {
      const data = await callStatsWebhook(requestId);

      // FIX 2: Try all possible status field name variations
      const status = (
        data?.status ||
        data?.Status ||
        data?.state ||
        data?.data?.status ||
        data?.result?.status ||
        ''
      ).toLowerCase();

      log.info(`Status: "${status}"`);

      await Actor.pushData({
        type: 'poll_result',
        attempt,
        request_id: requestId,
        status,
        response: data,
        polled_at: new Date().toISOString(),
      });

      if (status === 'completed') {
        log.info(`Job COMPLETED!`);
        return { status: 'completed', data };
      }

      if (status === 'failed' || status === 'error') {
        log.error(`Job FAILED with status: ${status}`);
        return { status: 'failed', data };
      }

      if (attempt < maxAttempts) {
        log.info(`Status is "${status || 'unknown'}". Waiting ${pollIntervalMs / 1000}s...`);
        await sleep(pollIntervalMs);
      }

    } catch (err) {
      log.warning(`Poll attempt ${attempt} error: ${err.message}`);
      if (attempt < maxAttempts) await sleep(pollIntervalMs);
    }
  }

  log.warning(`Reached max ${maxAttempts} attempts without completion`);
  return { status: 'timeout', data: null };
}

// ─── MAIN ─────────────────────────────────────────────────────────────────────
await Actor.init();

try {
  const input = await Actor.getInput();

  log.info('=== LinkedIn Profile Scraper Actor Started ===');
  log.info(`Customer: ${input.customer_name}`);
  log.info(`Email: ${input.customer_email}`);
  log.info(`Service: ${input.service_name}`);
  log.info(`Tag: ${input.service_request_tag_name}`);
  log.info(`Poll interval: ${input.poll_interval_minutes || 3} mins`);

  // ── 1. Collect LinkedIn URLs ──────────────────────────────────────────────
  let allUrls = [];

  if (input.linkedin_urls && input.linkedin_urls.length > 0) {
    const validManual = input.linkedin_urls.map((u) => u.trim()).filter(isValidLinkedInUrl);
    log.info(`Manual URLs: ${input.linkedin_urls.length} provided | ${validManual.length} valid`);
    allUrls = [...allUrls, ...validManual];
  }

  if (input.bulk_upload_csv && input.bulk_upload_csv.trim()) {
    const bulkUrls = await fetchUrlsFromBulk(input.bulk_upload_csv.trim());
    allUrls = [...allUrls, ...bulkUrls];
  }

  allUrls = [...new Set(allUrls)];

  if (allUrls.length === 0) {
    throw new Error('No valid LinkedIn URLs found. URLs must start with https://www.linkedin.com/in/');
  }

  log.info(`Total unique URLs: ${allUrls.length}`);
  log.info(`URLs: ${JSON.stringify(allUrls)}`);

  // ── 2. Save to Google Drive (optional) ───────────────────────────────────
  let spreadsheetUrl = null;

  if (input.google_service_account_json && input.drive_folder_id) {
    log.info('Google Drive credentials found — saving to Drive...');
    try {
      const serviceAccountKey = JSON.parse(input.google_service_account_json);
      const auth = new google.auth.GoogleAuth({
        credentials: serviceAccountKey,
        scopes: [
          'https://www.googleapis.com/auth/drive',
          'https://www.googleapis.com/auth/spreadsheets',
        ],
      });
      const drive = google.drive({ version: 'v3', auth });
      const sheets = google.sheets({ version: 'v4', auth });
      const customerFolderId = await getOrCreateFolder(drive, input.customer_name, input.drive_folder_id);
      const { spreadsheetUrl: sheetUrl } = await createLinkedInSheet(sheets, drive, customerFolderId, input.customer_name, allUrls);
      spreadsheetUrl = sheetUrl;
      log.info(`Sheet created: ${spreadsheetUrl}`);
    } catch (driveErr) {
      log.error(`Drive error (non-fatal): ${driveErr.message}`);
      log.warning(`Continuing without Drive — webhooks will still be called`);
    }
  } else {
    log.warning('No Drive credentials — skipping Drive save. Webhooks will still be called.');
  }

  // ── 3. POST each URL to Input Webhook ────────────────────────────────────
  log.info('=== Sending to input webhook ===');
  const requestIds = [];

  for (let i = 0; i < allUrls.length; i++) {
    const url = allUrls[i];
    log.info(`URL ${i + 1}/${allUrls.length}: ${url}`);
    const payload = {
      service_name: input.service_name,
      service_request_tag_name: input.service_request_tag_name,
      service_request_url: url,
      source: 'Dev',
      customer_email: input.customer_email,
    };
    try {
      const { requestId, rawResponse } = await postToInputWebhook(payload);
      requestIds.push({ url, requestId, rawResponse });
      if (i < allUrls.length - 1) await sleep(1500);
    } catch (err) {
      log.error(`Failed to post ${url}: ${err.message}`);
      requestIds.push({ url, requestId: null, error: err.message });
    }
  }

  log.info(`Submitted: ${requestIds.filter((r) => r.requestId).length}/${allUrls.length}`);

  // ── 4. Poll Stats Webhook ─────────────────────────────────────────────────
  log.info('=== Polling stats webhook ===');
  const pollIntervalMs = (input.poll_interval_minutes || 3) * 60 * 1000;
  const results = [];

  for (const { url, requestId, rawResponse, error } of requestIds) {
    if (error) {
      results.push({ url, requestId: null, status: 'submission_failed', error });
      continue;
    }
    log.info(`\nPolling: ${url} | request_id: ${requestId}`);
    const pollResult = await pollStatsWebhook(requestId, rawResponse, pollIntervalMs);
    results.push({
      url,
      requestId,
      status: pollResult.status,
      webhook_response: pollResult.data,
      spreadsheet_url: spreadsheetUrl,
      completed_at: new Date().toISOString(),
    });
  }

  // ── 5. Final Summary ──────────────────────────────────────────────────────
  const summary = {
    type: 'final_summary',
    customer_name: input.customer_name,
    customer_email: input.customer_email,
    total_urls: allUrls.length,
    submitted: requestIds.filter((r) => r.requestId).length,
    completed: results.filter((r) => r.status === 'completed').length,
    failed: results.filter((r) => r.status === 'failed').length,
    timed_out: results.filter((r) => r.status === 'timeout').length,
    spreadsheet_url: spreadsheetUrl,
    finished_at: new Date().toISOString(),
    results,
  };

  await Actor.pushData(summary);

  log.info('=== Actor Finished Successfully ===');
  log.info(`Completed: ${summary.completed}/${allUrls.length}`);
  log.info(`Drive sheet: ${spreadsheetUrl || 'Not saved'}`);

} catch (err) {
  log.error(`Fatal error: ${err.message}`);
  log.error(err.stack);
  await Actor.pushData({
    type: 'fatal_error',
    error: err.message,
    stack: err.stack,
    failed_at: new Date().toISOString(),
  });
  process.exit(1);
} finally {
  await Actor.exit();
}
