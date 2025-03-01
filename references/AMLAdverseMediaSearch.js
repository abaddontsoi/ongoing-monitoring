const { MongoClient } = require('mongodb');
const { getSimilarities } = require('calculate-string-similarity');

const DATASET_MONGO_URI = process.env.DATASET_MONGO_URI;
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
        const client = new MongoClient(DATASET_MONGO_URI);
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
    const { nameToSearchArr } = event;

    try {
        const client = await getMongoClient();
        const db = client.db(DATASET_DB_NAME);

        const data = await Promise.all(nameToSearchArr.map(async (name) => {
            try {
                return await getAdverseMedia(db, name);
            } catch (error) {
                console.error(`Error searching for adverse media with name ${name}:`, error);
                return [];
            }
        }));

        const flattenedData = data.flat();

        return {
            statusCode: 200,
            body: JSON.stringify({ status: 200, message: 'Success', data: flattenedData })
        };
    } catch (error) {
        console.error('Adverse media search failed:', error);
        return {
            statusCode: 500,
            body: JSON.stringify({ message: 'Adverse media search failed', error: error.message })
        };
    }
};

async function getAdverseMedia(db, name) {
    // 預處理搜尋名稱
    const normalizedSearchName = name.toLowerCase().trim();
    console.log('原始搜尋名稱:', name);
    console.log('正規化後的搜尋名稱:', normalizedSearchName);

    // 先用寬鬆的條件從資料庫取得候選資料
    const pipeline = [
        // 展開 target 陣列
        { $unwind: { path: "$target", preserveNullAndEmptyArrays: true } },

        // 匹配條件
        {
            $match: {
                $or: [
                    // 第一種格式：直接匹配 target.name_en
                    { "target.name_en": { $regex: `.*${normalizedSearchName}.*`, $options: "i" } },
                    { "target.name_zh": { $regex: `.*${normalizedSearchName}.*`, $options: "i" } },

                    // 第二種格式：匹配 target.en.ceName
                    { "target.en.ceName": { $regex: `.*${normalizedSearchName}.*`, $options: "i" } },
                    { "target.zh.ceName": { $regex: `.*${normalizedSearchName}.*`, $options: "i" } },
                ]
            }
        },

        // 重新組合相同文件的不同 target
        {
            $group: {
                _id: "$_id",
                doc: { $first: "$$ROOT" }
            }
        },

        // 恢復文件結構
        { $replaceRoot: { newRoot: "$doc" } }
    ];

    console.log('聚合管道:', JSON.stringify(pipeline, null, 2));

    const candidates = await db.collection("adverse_media").aggregate(pipeline).toArray();
    console.log(`找到 ${candidates.length} 筆候選資料`);

    // 使用字串相似度進行過濾
    const result = candidates.filter(item => {
        console.log('檢查項目:', {
            target結構: JSON.stringify(item.target, null, 2)
        });

        // 檢查 target 物件
        if (item.target?.name_en) {
            console.log('比對字串:', {
                搜尋名稱: normalizedSearchName,
                目標名稱: item.target.name_en.toLowerCase(),
            });

            const similarities = getSimilarities(normalizedSearchName, [item.target.name_en.toLowerCase()], {
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
            console.log('相似度未達閾值 20%');
            return false;
        }

        // 檢查第二種格式
        if (item.target?.en) {
            console.log('檢查第二種格式:', {
                en結構: JSON.stringify(item.target.en, null, 2)
            });

            const hasMatch = item.target.en.some(target => {
                console.log('比對字串:', {
                    搜尋名稱: normalizedSearchName,
                    目標名稱: target.ceName.toLowerCase(),
                });

                const similarities = getSimilarities(normalizedSearchName, [target.ceName.toLowerCase()], {
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
                console.log('相似度未達閾值 20%');
                return false;
            });

            return hasMatch;
        }

        console.log('未找到匹配');
        return false;
    });

    console.log(`相似度過濾後剩下 ${result.length} 筆資料`);
    let data = [];
    if (result.length > 0) {
        result.map(item => {
            const subtitle = item.source.title + (item.published ? ` - ${item.published}` : '')
            data.push({
                _id: item._id,
                title: item.headline?.en,
                link: item.urls[0],
                subtitle: subtitle,
                description: item.content?.en,
            })

        })
    }

    return result;
} 