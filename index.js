const { S3Client, ListObjectsV2Command, GetObjectCommand } = require('@aws-sdk/client-s3');
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');
require('dotenv').config();
const connectDB = require('mb64-connect');
const mongoose = require('mongoose');
const express = require('express');
const app = express();
const cors =require("cors")

app.use(cors())
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
    const date = `${match[2].slice(0, 2)}-${match[2].slice(2, 4)}-${match[2].slice(4, 8)}`;
    const fromtime = `${match[2].slice(8, 10)}:${match[2].slice(10, 12)}:${match[2].slice(12, 14)}`;
    const totime = `${match[3].slice(8, 10)}:${match[3].slice(10, 12)}:${match[3].slice(12, 14)}`;
    const divisename = match[1];
   // console.log({ filename, divisename, date, fromtime, totime });
    return { filename, divisename, date, fromtime, totime };
  }
  return null;
}

const getSignedUrls = async (bucketName) => {
  let continuationToken = null;
  const newUrls = [];

  const session = await mongoose.startSession();  // Start MongoDB session

  try {
    session.startTransaction();  // Begin transaction
    
    do {
      const params = { Bucket: bucketName, ContinuationToken: continuationToken };
      const command = new ListObjectsV2Command(params);
      const data = await s3Client.send(command);
      const urls = data.Contents.map(item => item.Key);

      for (let key of urls) {
        const getObjectParams = { Bucket: bucketName, Key: key };
        const command = new GetObjectCommand(getObjectParams);
        const url = await getSignedUrl(s3Client, command, { expiresIn: 432000 }); // 5 days expiration (432000 seconds)

        const extractedData = extractFilename(url);

        if (extractedData) {
          const { filename, divisename, date, fromtime, totime } = extractedData;
          newUrls.push({ url, filename, divisename, date, fromtime, totime });
          // console.log(`Prepared URL: ${url} with extracted data:`, extractedData);
        }
      }

      continuationToken = data.IsTruncated ? data.NextContinuationToken : null;
    } while (continuationToken);

    if (newUrls.length > 0) {
      await testvidios.deleteMany({}, { session });  // Delete all existing documents in the collection
      await testvidios.insertMany(newUrls, { session });  // Insert new URLs
      // console.log(`Stored ${newUrls.length} new URLs.`);
    }

    await session.commitTransaction();  // Commit transaction
    session.endSession();  // End session

  } catch (error) {
    await session.abortTransaction();  // Rollback transaction on error
    session.endSession();  // End session
    // console.error('Error fetching or storing signed URLs:', error);
  }
};

let intervalId;
let isRunning = false;
let lastStartSignalTime = null;
let stopSignalReceived = false;
let stopTimeoutId;

const startScheduledTasks = () => {
  const interval = 15 * 1000; // 15 seconds in milliseconds

  const executeTask = async () => {
    console.log('Starting the task...');
    await getSignedUrls(process.env.AWS_BUCKET_NAME);
    console.log('Task completed, waiting for the next interval...');
  };

  if (!isRunning) {
    isRunning = true;

    // Run the task initially and set the interval
    executeTask();
    intervalId = setInterval(executeTask, interval);

    // Set up the timeout to stop the task if no "start" signal received within 2 minutes
    stopTimeoutId = setTimeout(() => {
      if (!stopSignalReceived) {
        console.log('No start signal received in 2 minutes. Stopping task...');
        clearInterval(intervalId);
        isRunning = false;
      }
    }, 10 * 60 * 1000); // 2 minutes timeout
  }
};

const stopScheduledTasks = () => {
  stopSignalReceived = true;
  console.log('Stop signal received.');
  clearInterval(intervalId); // Stop the interval
  clearTimeout(stopTimeoutId); // Clear the timeout
  isRunning = false;
};

app.use(express.json());

// Endpoint to handle start and stop signals
app.post('/signal', (req, res) => {
  const { action } = req.body;

  if (action === 'start') {
    lastStartSignalTime = Date.now();
    stopSignalReceived = false;
    console.log('Start signal received.');
    // If task is not running, start the task
    if (!isRunning) {
      startScheduledTasks();
    }

    // Reset the timeout as the start signal is received within the 2-minute window
    clearTimeout(stopTimeoutId);
    stopTimeoutId = setTimeout(() => {
      if (!stopSignalReceived) {
        console.log('No start signal received in 2 minutes. Stopping task...');
        clearInterval(intervalId);
        isRunning = false;
      }
    }, 2 * 60 * 1000); // Reset 2 minutes timeout

    res.status(200).send('Start signal processed.');
  } else if (action === 'stop') {
    stopScheduledTasks();
    res.status(200).send('Task stopped.');
  } else {
    res.status(400).send('Invalid action.');
  }
});


app.get("/find", async (req, res) => {

  const { fromdate, todate, fromtime, totime,divisename } = req.query;
  
 
  if (!fromdate || !todate) {
    return res.status(400).json({ message: "Both fromdate and todate are required" });
  }

  try {
   
    const query = {
      date: {
        $gte: fromdate, 
        $lte: todate,   
      },
    };

   
    if (fromtime && totime) {
      query.$and = [
        { fromtime: { $gte: fromtime } }, 
        { totime: { $lte: totime } },   
      ];
    }

  
    if (divisename) {
      query.divisename = divisename;
    }

    const urls = await testvidios.find(query);
    res.status(200).json(urls);
  } catch (err) {
    res.status(500).json({ message: err.message });
  }
});


app.get('/check-live', async (req, res) => {
  const divisename = "Device-2"
  try {
    const now = new Date();
    const currentDate = now.toISOString().slice(0, 10);
    const currentTime = now.toTimeString().slice(0, 8); 
    const oneMinuteAgo = new Date(now.getTime() - 1.5 * 60 * 1000);
    const oneMinuteAgoTime = oneMinuteAgo.toTimeString().slice(0, 8); 
    const formatDate = (date) => {
      const d = new Date(date);
      const day = String(d.getDate()).padStart(2, "0");
      const month = String(d.getMonth() + 1).padStart(2, "0");
      const year = d.getFullYear();
      return `${day}-${month}-${year}`;
  };

 const datt= formatDate(currentDate)
    // console.log(datt,oneMinuteAgoTime,currentTime)

    const fromdate=datt,todate=datt,fromtime=oneMinuteAgoTime ,totime=currentTime
   
    const query = {
      date: {
        $gte: fromdate, 
        $lte: todate,   
      },
    };

   
    if (fromtime && totime) {
      query.$and = [
        { fromtime: { $gte: fromtime } }, 
        { totime: { $lte: totime } },   
      ];
    }

  
    if (divisename) {
      query.divisename = divisename;
    }

    const urls = await testvidios.find(query);
// console.log(urls)
    if(urls.length===0){
    return  res.send({isLive:false})
    }
    res.send({isLive:true,urls})
    } catch (err) {
      res.status(500).json({ message: err.message });
    }
});




app.listen(3000, () => {
  console.log('Server is running on port 3000');
});
