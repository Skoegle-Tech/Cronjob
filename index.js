const { S3Client, ListObjectsV2Command, GetObjectCommand } = require('@aws-sdk/client-s3');
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');
require('dotenv').config();
const connectDB = require('mb64-connect');

connectDB(process.env.MONGODB_URI);

const testvidios = connectDB.validation('testvidios', {
  url: { type: String, required: true },
  filename: { type: String, required: true },
  divisename: { type: String, required: false },
  date: { type: String, required: false },
  fromtime: { type: String, required: false },
  totime: { type: String, required: false }
}, { timestamps: false });

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
    const date = `${match[2].slice(0, 2)}-${match[2].slice(2, 4)}-${match[2].slice(4, 8)}`;
    const fromtime = `${match[2].slice(8, 10)}:${match[2].slice(10, 12)}:${match[2].slice(12, 14)}`;
    const totime = `${match[3].slice(8, 10)}:${match[3].slice(10, 12)}:${match[3].slice(12, 14)}`;
    const divisename = match[1];
    return { filename, divisename, date, fromtime, totime };
  }
  return null;
}

const getSignedUrls = async (bucketName) => {
  let continuationToken = null;
  const newUrls = [];

  try {
    do {
      console.log('Fetching objects from S3 bucket...');

      const params = { Bucket: bucketName, ContinuationToken: continuationToken };
      const command = new ListObjectsV2Command(params);
      const data = await s3Client.send(command);
      const urls = data.Contents.map(item => item.Key);

      console.log(`Found ${urls.length} files in S3 bucket.`);

      for (let key of urls) {
        console.log(`Generating signed URL for file: ${key}`);
        
        const getObjectParams = { Bucket: bucketName, Key: key };
        const command = new GetObjectCommand(getObjectParams);
        const url = await getSignedUrl(s3Client, command, { expiresIn: 432000 }); // 5 days expiration (432000 seconds)

        const extractedData = extractFilename(url);

        if (extractedData) {
          const { filename, divisename, date, fromtime, totime } = extractedData;
          console.log(`Extracted data for ${filename}:`, extractedData);
          
          newUrls.push({ url, filename, divisename, date, fromtime, totime });
        }
      }

      continuationToken = data.IsTruncated ? data.NextContinuationToken : null;
    } while (continuationToken);

    if (newUrls.length > 0) {
      console.log(`Inserting ${newUrls.length} records into MongoDB...`);
      
      try {
        await testvidios.insertMany(newUrls); // Insert all collected records at once
        console.log(`Successfully inserted ${newUrls.length} records into MongoDB.`);
      } catch (error) {
        console.error('Error inserting records into MongoDB:', error);
      }
    }

  } catch (error) {
    console.error('Error fetching or storing signed URLs:', error);
  }
};


const runTaskEveryInterval = async () => {
  const interval = 20 * 1000; 


  const executeTask = async () => {
    console.log('Starting the task...');
    await getSignedUrls(process.env.AWS_BUCKET_NAME);
    console.log('Task completed, waiting for the next interval...');
  };


  executeTask();
  setInterval(executeTask, interval);
};


runTaskEveryInterval();
