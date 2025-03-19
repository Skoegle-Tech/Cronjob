const { S3Client, ListObjectsV2Command, GetObjectCommand } = require('@aws-sdk/client-s3');
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');
require('dotenv').config();
const connectDB = require('mb64-connect');
const mongoose = require('mongoose');
const cron = require('node-cron');

const testvidios = connectDB.validation(
  'testvidios',
  {
    url: { type: String, required: true },
    filename: { type: String, required: true },
    divisename: { type: String, required: false },
    date: { type: String, required: false },
    fromtime: { type: String, required: false },
    totime: { type: String, required: false },
  },
  { timestamps: false }
);

connectDB(process.env.MONGODB_URI);

const s3Client = new S3Client({
  region: process.env.AWS_REGION,
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY,
    secretAccessKey: process.env.AWS_SECRET_KEY,
  },
});

const extractFilename = (url) => {
  const regex = /([^/]+?\.mp4)(?=\?|$)/;
  const match = url.match(regex);
  if (match) {
    let filename = decodeURIComponent(match[0].split('%20').join(' '));
    filename = filename.replace(/ - Compressed\.mp4$/, '');
    return SliceFileName(filename);
  }
  return null;
};

function SliceFileName(filename) {
  const match = filename.match(/^(Device-\d+)-(\d{14})-(\d{14})$/);
  if (match) {
    return {
      filename,
      divisename: match[1],
      date: `${match[2].slice(0, 2)}-${match[2].slice(2, 4)}-${match[2].slice(4, 8)}`,
      fromtime: `${match[2].slice(8, 10)}:${match[2].slice(10, 12)}:${match[2].slice(12, 14)}`,
      totime: `${match[3].slice(8, 10)}:${match[3].slice(10, 12)}:${match[3].slice(12, 14)}`,
    };
  }
  return null;
}

const getSignedUrls = async (bucketName) => {
  let continuationToken = null;
  try {
    const existingRecords = await testvidios.find({}, 'filename').lean();
    const existingFilenames = new Set(existingRecords.map((rec) => rec.filename));

    do {
      const params = { Bucket: bucketName, ContinuationToken: continuationToken };
      const command = new ListObjectsV2Command(params);
      const data = await s3Client.send(command);
      const newRecords = [];

      for (const item of data.Contents || []) {
        const getObjectParams = { Bucket: bucketName, Key: item.Key };
        const command = new GetObjectCommand(getObjectParams);
        const url = await getSignedUrl(s3Client, command, { expiresIn: 432000 });

        const extractedData = extractFilename(url);
        if (extractedData && !existingFilenames.has(extractedData.filename)) {
          newRecords.push({ url, ...extractedData });
          existingFilenames.add(extractedData.filename);
        }
      }

      if (newRecords.length > 0) {
        await testvidios.insertMany(newRecords);
      }

      continuationToken = data.IsTruncated ? data.NextContinuationToken : null;
    } while (continuationToken);

    // Cleanup: Remove old records that are no longer in S3
    await testvidios.deleteMany({ filename: { $nin: [...existingFilenames] } });
  } catch (error) {
    console.error('Error in getSignedUrls:', error);
  }
};

// Schedule the job to run every Monday at midnight (00:00)
cron.schedule('0 0 * * 1', async () => {
  console.log('Starting the synchronization task...');
  await getSignedUrls(process.env.AWS_BUCKET_NAME);
  console.log('Synchronization task completed.');
});

console.log('Cron job scheduled: Runs every Monday at midnight.');
