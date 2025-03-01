const { MongoClient } = require('mongodb');
const { getSimilarities } = require('calculate-string-similarity');

if (!process.env.MONGODB_URI) {
  throw new Error('Invalid environment variable: "MONGODB_URI"');
}

const MONGODB_URI = process.env.MONGODB_URI;
const DATASET_DB_NAME = process.env.DATASET_DB_NAME;

let cachedClient = null;
let cachedDb = null;

async function getMongoClient() {
  if (cachedClient && cachedDb) {
    try {
      await cachedDb.command({ ping: 1 });
      return cachedClient;
    } catch (e) {
      await cachedClient.close();
      cachedClient = null;
      cachedDb = null;
    }
  }

  try {
    const client = new MongoClient(MONGODB_URI);
    await client.connect();
    cachedClient = client;
    cachedDb = client.db(DATASET_DB_NAME);
    return cachedClient;
  } catch (error) {
    console.error('Failed to connect to MongoDB:', error);
    throw new Error('Failed to connect to MongoDB');
  }
}

exports.handler = async (event) => {
  console.log('Received event:', JSON.stringify(event, null, 2));

  const { nameToSearchArr } = event;

  if (!nameToSearchArr) {
    return {
      statusCode: 400,
      body: JSON.stringify({ message: 'Missing required fields or invalid format: nameToSearchArr' })
    };
  }

  try {
    const client = await getMongoClient();
    const db = client.db(DATASET_DB_NAME);

    const data = await Promise.all(nameToSearchArr.map(async (name) => {
      try {
        return await getJudgments(db, name);
      } catch (error) {
        console.error(`Error searching for judgments with name ${name}:`, error);
        return [];
      }
    }));

    // Flatten the results array
    const flattenedData = data.flat();

    return {
      statusCode: 200,
      body: JSON.stringify({ status: 200, message: 'Success', data: flattenedData })
    };
  } catch (error) {
    console.error('Judgment search failed:', error);
    return {
      statusCode: 500,
      body: JSON.stringify({ message: 'Judgment search failed', error: error.message })
    };
  }
};

async function getJudgments(db, name) {
  // 預處理搜尋名稱
  const normalizedSearchName = name.toLowerCase().trim();
  console.log('原始搜尋名稱:', name);
  console.log('正規化後的搜尋名稱:', normalizedSearchName);

  // 使用寬鬆的條件從資料庫取得候選資料
  const pipeline = [
    {
      $match: {
        title: { $regex: `.*${normalizedSearchName}.*`, $options: "i" }
      }
    }
  ];

  console.log('聚合管道:', JSON.stringify(pipeline, null, 2));
  
  const candidates = await db.collection("judgment").aggregate(pipeline).toArray();
  console.log(`找到 ${candidates.length} 筆候選資料`);

  // 使用字串相似度進行過濾
  const result = candidates.filter(item => {
    if (typeof item.title === 'string') {
      const lowerItemTitle = item.title.toLowerCase();
      console.log('比對字串:', {
        搜尋名稱: normalizedSearchName,
        目標名稱: lowerItemTitle
      });

      // 計算相似度
      const similarities = getSimilarities(normalizedSearchName, [lowerItemTitle], {
        caseSensitive: false,
        orderSensitive: false,
        threshold: 20,
        thresholdType: '>='
      });

      console.log('相似度結果:', similarities);

      if (similarities.length > 0) {
        console.log('找到匹配！相似度:', similarities[0]);
        return true;
      }
      
      console.log('相似度未達閾值');
      return false;
    }
    return false;
  });

  console.log(`相似度過濾後剩下 ${result.length} 筆資料`);
  
  if (result.length > 0) {
    console.log('第一筆資料:', JSON.stringify(result[0], null, 2));
  }

  return result;
}