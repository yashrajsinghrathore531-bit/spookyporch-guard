import { RekognitionClient, DetectLabelsCommand } from "@aws-sdk/client-rekognition";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, GetCommand, PutCommand, DeleteCommand, UpdateCommand } from "@aws-sdk/lib-dynamodb";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { SNSClient, PublishCommand } from "@aws-sdk/client-sns";
import { randomUUID } from "crypto";

const rek = new RekognitionClient({});
const ddb = DynamoDBDocumentClient.from(new DynamoDBClient({}));
const s3 = new S3Client({});
const sns = new SNSClient({});

const TABLE = process.env.TABLE_NAME;
const BUCKET = process.env.BUCKET;
const TOPIC_ARN = process.env.TOPIC_ARN || "";
const LINGER_SECONDS = Number(process.env.LINGER_SECONDS || 10);
const PACKAGE_LABELS = ["Box", "Package", "Package Delivery", "Cardboard", "Carton", "Parcel"];

const resp = (statusCode, body) => ({
  statusCode,
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify(body),
});

export const handler = async (event) => {
  try {
    const { sessionId, image } = JSON.parse(event.body || "{}");
    if (!sessionId || !image) return resp(400, { error: "sessionId and image are required" });

    const bytes = Buffer.from(image.replace(/^data:image\/\w+;base64,/, ""), "base64");

    // 1. Ask Rekognition what is in the frame
    const out = await rek.send(
      new DetectLabelsCommand({ Image: { Bytes: bytes }, MaxLabels: 25, MinConfidence: 70 })
    );
    const labels = (out.Labels || []).map((l) => l.Name);
    const personDetected = labels.includes("Person");
    const packageDetected = labels.some((n) => PACKAGE_LABELS.includes(n));

    const stateKey = `state#${sessionId}`;
    const now = Date.now();

    // 2. Person + package together? Otherwise reset the timer.
    if (!(personDetected && packageDetected)) {
      await ddb.send(new DeleteCommand({ TableName: TABLE, Key: { eventId: stateKey } }));
      return resp(200, {
        status: personDetected ? "visitor" : "clear",
        seconds: 0,
        personDetected,
        packageDetected,
        labels,
      });
    }

    // 3. Lingering timer
    const existing = await ddb.send(new GetCommand({ TableName: TABLE, Key: { eventId: stateKey } }));
    let state = existing.Item;
    if (!state) {
      state = { eventId: stateKey, firstSeen: now, alerted: false };
      await ddb.send(new PutCommand({ TableName: TABLE, Item: state }));
    }
    const seconds = Math.round((now - state.firstSeen) / 1000);
    const suspicious = seconds >= LINGER_SECONDS;

    // 4. Alert once per lingering episode
    let alerted = state.alerted;
    if (suspicious && !state.alerted) {
      const id = randomUUID();
      const frameKey = `suspicious/${id}.jpg`;

      await s3.send(new PutObjectCommand({ Bucket: BUCKET, Key: frameKey, Body: bytes, ContentType: "image/jpeg" }));

      await ddb.send(
        new PutCommand({
          TableName: TABLE,
          Item: { eventId: id, type: "SUSPICIOUS", sessionId, seconds, labels, frameKey, createdAt: new Date(now).toISOString() },
        })
      );

      if (TOPIC_ARN) {
        try {
          await sns.send(
            new PublishCommand({
              TopicArn: TOPIC_ARN,
              Subject: "Phantom Alert: SpookyPorch Guard",
              Message: `A phantom has been hovering near your candy stash for ${seconds} seconds!`,
            })
          );
        } catch (e) {
          console.error("SNS publish failed", e);
        }
      }

      await ddb.send(
        new UpdateCommand({
          TableName: TABLE,
          Key: { eventId: stateKey },
          UpdateExpression: "SET alerted = :t",
          ExpressionAttributeValues: { ":t": true },
        })
      );
      alerted = true;
    }

    return resp(200, {
      status: suspicious ? "suspicious" : "watching",
      seconds,
      personDetected,
      packageDetected,
      alerted,
      labels,
    });
  } catch (err) {
    console.error(err);
    return resp(500, { error: "The spirits are restless", detail: String(err.message || err) });
  }
};
