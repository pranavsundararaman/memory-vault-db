const express = require("express");
const cors = require("cors");
const multer = require("multer");
const crypto = require("crypto");
const fs = require("fs");
const path = require("path");
const { promisify } = require("util");
const { execFile } = require("child_process");
const exifr = require("exifr");
const sharp = require("sharp");
const ffprobe = require("ffprobe-static");
const { Pool } = require("pg");

const execFileAsync = promisify(execFile);
const app = express();
const port = process.env.PORT || 3000;

const uploadDir = path.join(__dirname, "uploads");
fs.mkdirSync(uploadDir, { recursive: true });

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.NODE_ENV === "production" ? { rejectUnauthorized: false } : false,
});

app.use(cors());
app.use(express.json());
app.use("/uploads", express.static(uploadDir));

const storage = multer.diskStorage({
  destination: (_req, _file, cb) => cb(null, uploadDir),
  filename: (_req, file, cb) => {
    const safeName = `${Date.now()}-${file.originalname.replace(/\s+/g, "_")}`;
    cb(null, safeName);
  },
});

const upload = multer({ storage });

app.get("/api/health", (_req, res) => {
  res.json({ ok: true });
});

app.post("/api/media/upload", upload.single("file"), async (req, res) => {
  const client = await pool.connect();

  try {
    if (!req.file) {
      return res.status(400).json({ message: "File is required" });
    }

    const fileBuffer = fs.readFileSync(req.file.path);
    const checksumSha256 = crypto.createHash("sha256").update(fileBuffer).digest("hex");
    const mimeType = req.file.mimetype || "application/octet-stream";
    const mediaType = mimeType.startsWith("image/") ? "photo" : "video";
    const relativeFilePath = `/uploads/${path.basename(req.file.path)}`;

    const extracted = await extractMetadata(req.file.path, mediaType);
    const captureTs = extracted.captureTs || null;
    const isFavourite = false;

    await client.query("BEGIN");

    const duplicateCheck = await client.query(
      `
        SELECT media_id, file_name, file_path
        FROM media_items
        WHERE checksum_sha256 = $1
        LIMIT 1
      `,
      [checksumSha256]
    );

    if (duplicateCheck.rowCount > 0) {
      await client.query("ROLLBACK");
      removeLocalFile(req.file.path);
      return res.status(409).json({
        message: "Duplicate file detected",
        duplicate: duplicateCheck.rows[0],
      });
    }

    const deviceId = await ensureDevice(client, extracted.device);
    const locationId = await ensureLocation(client, extracted.location);

    const mediaInsert = await client.query(
      `
        INSERT INTO media_items (
          device_id,
          location_id,
          file_name,
          file_path,
          file_size_bytes,
          mime_type,
          media_type,
          capture_ts,
          checksum_sha256,
          is_favourite
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
        RETURNING media_id, file_name, file_path, media_type, capture_ts, is_favourite
      `,
      [
        deviceId,
        locationId,
        req.file.originalname,
        relativeFilePath,
        req.file.size,
        mimeType,
        mediaType,
        captureTs,
        checksumSha256,
        isFavourite,
      ]
    );

    const media = mediaInsert.rows[0];

    if (mediaType === "photo") {
      await client.query(
        `
          INSERT INTO photo_metadata (
            media_id,
            iso,
            aperture,
            shutter_speed_text,
            focal_length_mm,
            flash_fired,
            color_space,
            width,
            height
          )
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
          ON CONFLICT (media_id) DO NOTHING
        `,
        [
          media.media_id,
          extracted.photo.iso,
          extracted.photo.aperture,
          extracted.photo.shutterSpeedText,
          extracted.photo.focalLengthMm,
          extracted.photo.flashFired,
          extracted.photo.colorSpace,
          extracted.photo.width,
          extracted.photo.height,
        ]
      );
    } else {
      await client.query(
        `
          INSERT INTO video_metadata (
            media_id,
            duration_seconds,
            fps,
            codec,
            bitrate_kbps,
            audio_channels,
            width,
            height
          )
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
          ON CONFLICT (media_id) DO NOTHING
        `,
        [
          media.media_id,
          extracted.video.durationSeconds,
          extracted.video.fps,
          extracted.video.codec,
          extracted.video.bitrateKbps,
          extracted.video.audioChannels,
          extracted.video.width,
          extracted.video.height,
        ]
      );
    }

    await client.query("COMMIT");

    return res.status(201).json({
      message: "Upload successful",
      media: {
        mediaId: media.media_id,
        fileName: media.file_name,
        filePath: media.file_path,
        mediaType: media.media_type,
        captureTs: media.capture_ts,
        isFavourite: media.is_favourite,
      },
      extracted: {
        device: extracted.device,
        location: extracted.location,
        photo: extracted.photo,
        video: extracted.video,
      },
    });
  } catch (error) {
    await client.query("ROLLBACK");
    removeLocalFile(req.file?.path);
    console.error(error);
    return res.status(500).json({
      message: "Upload failed",
      error: error.message,
    });
  } finally {
    client.release();
  }
});

app.get("/api/media", async (_req, res) => {
  try {
    const result = await pool.query(
      `
        SELECT
          media_id,
          file_name,
          file_path,
          media_type,
          capture_ts,
          is_favourite
        FROM media_items
        ORDER BY capture_ts DESC NULLS LAST, ingested_at DESC
      `
    );

    res.json(result.rows);
  } catch (error) {
    res.status(500).json({ message: "Failed to fetch media", error: error.message });
  }
});

app.listen(port, () => {
  console.log(`Server running on port ${port}`);
});

async function extractMetadata(filePath, mediaType) {
  if (mediaType === "photo") {
    return extractPhotoMetadata(filePath);
  }
  return extractVideoMetadata(filePath);
}

async function extractPhotoMetadata(filePath) {
  const exif = (await exifr.parse(filePath, {
    gps: true,
    tiff: true,
    exif: true,
    ifd0: true,
  })) || {};

  const image = await sharp(filePath).metadata();

  return {
    captureTs: toIsoString(exif.DateTimeOriginal || exif.CreateDate || image.exif?.CreateDate),
    device: {
      make: cleanText(exif.Make),
      model: cleanText(exif.Model),
    },
    location: buildLocation(exif.latitude, exif.longitude),
    photo: {
      iso: toNumberOrNull(exif.ISO),
      aperture: toNumberOrNull(exif.FNumber),
      shutterSpeedText: formatExposure(exif.ExposureTime),
      focalLengthMm: toNumberOrNull(exif.FocalLength),
      flashFired: Boolean(exif.Flash),
      colorSpace: cleanText(exif.ColorSpace),
      width: toNumberOrNull(image.width),
      height: toNumberOrNull(image.height),
    },
    video: emptyVideoMetadata(),
  };
}

async function extractVideoMetadata(filePath) {
  const probe = await getFfprobeMetadata(filePath);
  const videoStream = probe.streams.find((stream) => stream.codec_type === "video") || {};
  const audioStream = probe.streams.find((stream) => stream.codec_type === "audio") || {};

  return {
    captureTs: toIsoString(
      probe.format?.tags?.creation_time ||
      videoStream.tags?.creation_time
    ),
    device: {
      make: null,
      model: cleanText(probe.format?.tags?.encoder),
    },
    location: null,
    photo: emptyPhotoMetadata(),
    video: {
      durationSeconds: toNumberOrNull(probe.format?.duration),
      fps: parseFps(videoStream.avg_frame_rate || videoStream.r_frame_rate),
      codec: cleanText(videoStream.codec_name),
      bitrateKbps: toKbps(probe.format?.bit_rate),
      audioChannels: toNumberOrNull(audioStream.channels),
      width: toNumberOrNull(videoStream.width),
      height: toNumberOrNull(videoStream.height),
    },
  };
}

async function getFfprobeMetadata(filePath) {
  const { stdout } = await execFileAsync(ffprobe.path, [
    "-v",
    "quiet",
    "-print_format",
    "json",
    "-show_format",
    "-show_streams",
    filePath,
  ]);

  return JSON.parse(stdout);
}

async function ensureDevice(client, device) {
  if (!device?.make && !device?.model) {
    return null;
  }

  const make = device.make || "Unknown";
  const model = device.model || "Unknown";

  const existing = await client.query(
    `
      SELECT device_id
      FROM devices
      WHERE lower(make) = lower($1)
        AND lower(model) = lower($2)
      LIMIT 1
    `,
    [make, model]
  );

  if (existing.rowCount > 0) {
    return existing.rows[0].device_id;
  }

  const inserted = await client.query(
    `
      INSERT INTO devices (make, model)
      VALUES ($1, $2)
      RETURNING device_id
    `,
    [make, model]
  );

  return inserted.rows[0].device_id;
}

async function ensureLocation(client, location) {
  if (!location || location.latitude === null || location.longitude === null) {
    return null;
  }

  const inserted = await client.query(
    `
      INSERT INTO locations (place_name, city, country, latitude, longitude)
      VALUES ($1, $2, $3, $4, $5)
      ON CONFLICT (location_hash) DO UPDATE
      SET place_name = COALESCE(locations.place_name, EXCLUDED.place_name)
      RETURNING location_id
    `,
    [
      location.placeName,
      location.city,
      location.country || "Unknown",
      location.latitude,
      location.longitude,
    ]
  );

  return inserted.rows[0].location_id;
}

function buildLocation(latitude, longitude) {
  const lat = toNumberOrNull(latitude);
  const lng = toNumberOrNull(longitude);

  if (lat === null || lng === null) {
    return null;
  }

  return {
    placeName: "Auto-detected location",
    city: null,
    country: "Unknown",
    latitude: lat,
    longitude: lng,
  };
}

function parseFps(value) {
  if (!value || value === "0/0") {
    return null;
  }

  const [num, den] = String(value).split("/").map(Number);
  if (!den || Number.isNaN(num) || Number.isNaN(den)) {
    return toNumberOrNull(value);
  }

  return Number((num / den).toFixed(2));
}

function formatExposure(value) {
  if (value === undefined || value === null) {
    return null;
  }
  if (typeof value === "number" && value > 0 && value < 1) {
    return `1/${Math.round(1 / value)}`;
  }
  return String(value);
}

function toIsoString(value) {
  if (!value) {
    return null;
  }

  const date = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(date.getTime())) {
    return null;
  }

  return date.toISOString();
}

function toNumberOrNull(value) {
  if (value === undefined || value === null || value === "") {
    return null;
  }

  const num = Number(value);
  return Number.isNaN(num) ? null : num;
}

function toKbps(value) {
  const bitsPerSecond = toNumberOrNull(value);
  if (bitsPerSecond === null) {
    return null;
  }
  return Math.round(bitsPerSecond / 1000);
}

function cleanText(value) {
  if (!value) {
    return null;
  }
  return String(value).trim() || null;
}

function emptyPhotoMetadata() {
  return {
    iso: null,
    aperture: null,
    shutterSpeedText: null,
    focalLengthMm: null,
    flashFired: false,
    colorSpace: null,
    width: null,
    height: null,
  };
}

function emptyVideoMetadata() {
  return {
    durationSeconds: null,
    fps: null,
    codec: null,
    bitrateKbps: null,
    audioChannels: null,
    width: null,
    height: null,
  };
}

function removeLocalFile(filePath) {
  if (!filePath) return;
  if (fs.existsSync(filePath)) {
    fs.unlinkSync(filePath);
  }
}
