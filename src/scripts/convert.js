// index.js
import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";
import { pipeline } from "node:stream/promises";
import { parse as csvParse } from "csv-parse";
import dayjs from "dayjs";
import fetch from "node-fetch";

// ------------------------ CSV helpers ------------------------

function readCsv(filePath) {
  return new Promise((resolve, reject) => {
    const rows = [];
    fs.createReadStream(filePath)
      .pipe(
        csvParse({
          columns: true,
          skip_empty_lines: true,
          trim: true,
        }),
      )
      .on("data", (rec) => rows.push(rec))
      .on("end", () => resolve(rows))
      .on("error", reject);
  });
}

function splitImageUrls(images) {
  if (!images) return [];
  return images
    .split(",")
    .map((s) => s.trim())
    .filter((s) => s.length > 0);
}

// ------------------------ Date, file, mime utils ------------------------

function guessExtFromMime(mime) {
  if (!mime) return "";
  const m = mime.toLowerCase();
  if (m.includes("jpeg") || m.includes("jpg")) return ".jpg";
  if (m.includes("png")) return ".png";
  if (m.includes("gif")) return ".gif";
  if (m.includes("webp")) return ".webp";
  if (m.includes("mp4")) return ".mp4";
  if (m.includes("svg")) return ".svg";
  return "";
}

function normalizeDateToISO(s) {
  if (!s) return null;
  const d = dayjs(s);
  return d.isValid() ? d.toISOString() : null;
}

function sanitizeFilename(name) {
  return (name || "file").replace(/[\\/:*?"<>|]+/g, "_");
}

// ------------------------ Google Drive link helpers ------------------------

function extractDriveFileId(url) {
  try {
    const u = new URL(url.trim());
    if (u.hostname !== "drive.google.com") return null;

    // Supported formats:
    // - https://drive.google.com/open?id=FILEID
    // - https://drive.google.com/file/d/FILEID/view
    // - https://drive.google.com/uc?id=FILEID&export=download
    if (u.pathname.startsWith("/file/d/")) {
      const parts = u.pathname.split("/");
      const idx = parts.findIndex((p) => p === "d");
      if (idx >= 0 && parts[idx + 1]) return parts[idx + 1];
    }
    const idParam = u.searchParams.get("id");
    if (idParam) return idParam;
  } catch (_) {
    // ignore
  }
  return null;
}

function toDriveDirectDownload(url) {
  const id = extractDriveFileId(url);
  if (!id) return url;
  // Prefer usercontent endpoint which generally returns the file bytes directly
  return `https://drive.usercontent.google.com/uc?id=${id}&export=download`;
}

// ------------------------ Download ------------------------

async function downloadFile(url, outPath) {
  // Force direct download if it's a Drive share link
  const effectiveUrl = toDriveDirectDownload(url);

  const res = await fetch(effectiveUrl);
  if (!res.ok) {
    throw new Error(`Fetch failed ${res.status} ${res.statusText}`);
  }

  await fsp.mkdir(path.dirname(outPath), { recursive: true });
  await pipeline(res.body, fs.createWriteStream(outPath));

  const mime = res.headers.get("content-type") || undefined;
  return { mimeType: mime, effectiveUrl };
}

// ------------------------ Main pipeline ------------------------

async function main() {
  const inputCsv = process.argv[2];
  const outputJson = process.argv[3] || "events.json";
  const outDir = process.argv[4] || "downloaded_images";

  if (!inputCsv) {
    console.error(
      "Usage: node index.js <input.csv> [output.json] [imagesOutDir]",
    );
    process.exit(1);
  }

  const rows = await readCsv(inputCsv);
  const results = [];

  for (const [idx, row] of rows.entries()) {
    const postTimeISO = normalizeDateToISO(row.post_time);
    const eventDateISO = normalizeDateToISO(row.date);
    const images = splitImageUrls(row.Images);

    const uploaded = [];

    for (const [i, url] of images.entries()) {
      const record = { sourceUrl: url };
      try {
        // Skip HEAD for Drive links; it may hit HTML viewer
        const isDrive = !!extractDriveFileId(url);

        let mimeHead;
        if (!isDrive) {
          const headRes = await fetch(url, { method: "HEAD" }).catch(
            () => null,
          );
          mimeHead = headRes?.ok
            ? headRes.headers.get("content-type") || undefined
            : undefined;
        }

        let ext = guessExtFromMime(mimeHead);

        // Derive a filename base from URL
        let base = "image";
        try {
          const eff = isDrive ? toDriveDirectDownload(url) : url;
          const u = new URL(eff);
          const candidate = path.basename(u.pathname || "image");
          base = candidate && candidate !== "/" ? candidate : "image";
        } catch {
          // ignore
        }

        if (!ext) {
          const urlExt = path.extname(base);
          if (urlExt.length <= 6 && urlExt.length >= 2) {
            ext = urlExt;
          }
        }
        if (!ext) ext = ".png";

        const safeEvent = sanitizeFilename(row.event_name || "event");
        const filename = `${safeEvent}_${idx + 1}_${i + 1}${ext}`;
        const localPath = path.join(outDir, filename);

        const { mimeType } = await downloadFile(url, localPath);

        // If HEAD was skipped or unclear, MIME from GET is authoritative
        record.localPath = localPath;
        record.mimeType = mimeType || mimeHead || undefined;
        // Clean up
        delete record.sourceUrl;
        delete record.mimeType;
      } catch (e) {
        record.error = e?.message || String(e);
        console.error(
          `Image download failed (row ${idx + 1}): ${url} -> ${record.error}`,
        );
      }
      uploaded.push(record);
    }

    results.push({
      postTimeISO,
      author: row.name || "",
      eventName: row.event_name || "",
      eventDateISO,
      description: row.description || "",
      images: uploaded,
    });
  }

  await fsp.writeFile(outputJson, JSON.stringify(results, null, 2), "utf-8");
  console.log(
    `Done. Wrote ${results.length} events to ${outputJson}. Downloaded images to "${outDir}".`,
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
