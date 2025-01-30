const { S3Client, ListObjectsV2Command, GetObjectCommand } = require('@aws-sdk/client-s3');
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');
require('dotenv').config();
const connectDB = require('mb64-connect');
const mongoose = require('mongoose');

const testvidios = connectDB.validation('testvidios', {
  url: { type: String, required: true },
  filename: { type: String, required: true },
  divisename: { type: String, required: false },
  date: { type: String, required: false },
  fromtime: { type: String, required: false },
  totime: { type: String, required: false }
}, { timestamps: false });

connectDB(process.env.MONGODB_URI);

const s3Client = new S3Client({
  region: process.env.AWS_REGION,
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY,
    secretAccessKey: process.env.AWS_SECRET_KEY
  }
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
      totime: `${match[3].slice(8, 10)}:${match[3].slice(10, 12)}:${match[3].slice(12, 14)}`
    };
  }
  return null;
}

const getSignedUrls = async (bucketName) => {
  let continuationToken = null;
  const newUrls = [];
  const session = await mongoose.startSession();
  try {
    await session.withTransaction(async () => {
      do {
        const params = { Bucket: bucketName, ContinuationToken: continuationToken };
        const command = new ListObjectsV2Command(params);
        const data = await s3Client.send(command);
        
        for (let item of data.Contents || []) {
          const getObjectParams = { Bucket: bucketName, Key: item.Key };
          const command = new GetObjectCommand(getObjectParams);
          const url = await getSignedUrl(s3Client, command, { expiresIn: 432000 });
          
          const extractedData = extractFilename(url);
          if (extractedData) {
            newUrls.push({ url, ...extractedData });
          }
        }

        continuationToken = data.IsTruncated ? data.NextContinuationToken : null;
      } while (continuationToken);

      if (newUrls.length > 0) {
        await testvidios.deleteMany({}, { session });
        await testvidios.insertMany(newUrls, { session });
      }
    });
  } catch (error) {
    console.error('Transaction error:', error);
    await session.abortTransaction();
  } finally {
    session.endSession();
  }
};

const continuousSync = async () => {
  while (true) {
    console.log('Starting the synchronization task...');
    await getSignedUrls(process.env.AWS_BUCKET_NAME);
    console.log('Task completed. Waiting for 10 seconds before restarting...');
    await new Promise(resolve => setTimeout(resolve, 10000));
  }
};

continuousSync();
