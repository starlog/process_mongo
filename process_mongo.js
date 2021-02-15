///////////////////////////////////////////////////////////////////////////////////////////////////
// MongoDB partial update tool
// By Felix (2021/2/14)
///////////////////////////////////////////////////////////////////////////////////////////////////

'use strict';
let async = require('async');
const mongo = require('wavve-tool').interface.mongodb;
const util2 = require('wavve-tool').util.util2;

util2.setLogLevel2('error');
let processList = {
    insertList: [],
    deleteList: [],
    insertMax: 0,
    insertIndex: 0,
    deleteMax: 0,
    deleteIndex: 0
};

///////////////////////////////////////////////////////////////////////////////////////////////////
// DB 설정
///////////////////////////////////////////////////////////////////////////////////////////////////
let mongoConnections = [
    {
        name: 'source',
        url: 'mongodb://localhost:27017',
        options: {
            poolSize: 1,
            connectTimeoutMS: 2000,
            useNewUrlParser: true,
            useUnifiedTopology: true
        },
        useRedis: false,
        RedisTtl: 60
    },
    {
        name: 'destination',
        url: 'mongodb://localhost:27017',
        options: {
            poolSize: 1,
            connectTimeoutMS: 2000,
            useNewUrlParser: true,
            useUnifiedTopology: true
        },
        useRedis: false,
        RedisTtl: 60
    },
];
///////////////////////////////////////////////////////////////////////////////////////////////////
// PROCESS 컬랙션에 저장되어 있는 데이타를 한 레코드씩 TARGET 컬랙션에 insert/delete 처리를 한다.
///////////////////////////////////////////////////////////////////////////////////////////////////
const PROCESS_CONFIG = 'source';
const PROCESS_DB = 'recovery';
const PROCESS_COLLECTION = 'process-list';

const TARGET_CONFIG = 'destination';
const TARGET_DB = 'recovery';
const TARGET_COLLECTION = 'supercontent_old';

///////////////////////////////////////////////////////////////////////////////////////////////////
// Process begin
///////////////////////////////////////////////////////////////////////////////////////////////////
async.series([
    async.apply(init, mongoConnections),
    async.apply(getList, mongo, processList), // 목록 가져오기, insert와 delete로 구분
    async.apply(processDelete, mongo, processList), // delete 처리
    async.apply(processInsert, mongo, processList) // insert 처리
], function (err, result)
{
    if (err)
    {
        console.log('Result is ' + err);
    }
    else
    {
        console.log('Process SUCCESS');
    }
    process.exit();
});


///////////////////////////////////////////////////////////////////////////////////////////////////
function init(mongoConnections, callback)
{
    mongo.init(mongoConnections, function (err)
    {
        callback(err);
    });
}

///////////////////////////////////////////////////////////////////////////////////////////////////
function getList(mongo, processList, callback)
{
    try
    {
        let sourceQueryObject = {
            name: PROCESS_CONFIG,
            db: PROCESS_DB,
            collection: PROCESS_COLLECTION,
            query: {},
            sort: {},
            fields: {_id: 0},
            skip: 0,
            limit: 2000
        };
        mongo.find(sourceQueryObject, function (err, result)
            {
                if (!err && result)
                {
                    for (let i = 0; i < result.length; i++)
                    {
                        if (result[i]['flagfield'] === 'new')
                        {
                            processList.insertList.push(result[i]);
                        }
                        else if (result[i]['flagfield'] === 'deleted')
                        {
                            processList.deleteList.push(result[i]);
                        }
                    }
                    processList.insertMax = processList.insertList.length;
                    processList.deleteMax = processList.deleteList.length;
                }
                callback(err);
            }
        );
    }
    catch
        (exception)
    {
        console.log('getList try-catch exception:' + exception);
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////
function processInsert(mongo, processList, callback)
{
    try
    {
        async.until(
            async.apply(testInsertCount, processList),
            async.apply(doInsertTask, mongo, processList),
            (err) =>
            {
                if (err === 'done-insert')
                {
                    err = null;
                }
                else if (err)
                {
                    console.log('processInsert error=' + err);
                }
                callback(err);
            }
        );
    }
    catch (exception)
    {
        console.log('processInsert try-catch exception:' + exception);
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////
function testInsertCount(processList, callback)
{
    if (processList.insertIndex >= processList.insertMax)
    {
        callback('done-insert');
    }
    else
    {
        callback(null);
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////
function doInsertTask(mongo, processList, callback)
{
    try
    {
        mongo.findOne({
            name: TARGET_CONFIG,
            db: TARGET_DB,
            collection: TARGET_COLLECTION,
            query: {_id: new mongo.ObjectID(processList.insertList[processList.insertIndex].id)}
        }, function (err, result)
        {
            if (!err || err === 'Mongodb found none')
            {
                if (result) //Exists, skip
                {
                    console.log('ADDING: SKIP(Exists): '
                        + util2.debugDump(processList.insertList[processList.insertIndex], 3, 200, false));
                    processList.insertIndex++;
                    callback(null);
                }
                else //Insert
                {
                    mongo.insertOne({
                        name: TARGET_CONFIG,
                        db: TARGET_DB,
                        collection: TARGET_COLLECTION,
                        newValue: processList.insertList[processList.insertIndex].json
                    }, (err, result) =>
                    {
                        console.log('ADDING: affect:' + result.result.n + ', success:' + result.result.ok + ', '
                            + util2.debugDump(processList.insertList[processList.insertIndex], 3, 200, false));
                        processList.insertIndex++;
                        callback(err)
                    });
                }
            }
            else
            {
                callback(err);
            }
        });
    }
    catch (exception)
    {
        console.log('doInsertTask try-catch exception:' + exception);
        callback(exception);
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////
function processDelete(mongo, processList, callback)
{
    try
    {
        async.until(
            async.apply(testDeleteCount, processList),
            async.apply(doDeleteTask, mongo, processList),
            (err) =>
            {
                if (err === 'done-delete')
                {
                    err = null;
                }
                else if (err && err !== 'done')
                {
                    console.log('processDelete error=' + err);
                }
                callback(err);
            }
        );
    }
    catch (exception)
    {
        console.log('processDelete try-catch exception:' + exception);
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////
function testDeleteCount(processList, callback)
{
    if (processList.deleteIndex >= processList.deleteMax)
    {
        callback('done-delete');
    }
    else
    {
        callback(null);
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////
function doDeleteTask(mongo, processList, callback)
{
    try
    {
        mongo.findOne({
            name: TARGET_CONFIG,
            db: TARGET_DB,
            collection: TARGET_COLLECTION,
            query: {_id: new mongo.ObjectID(processList.deleteList[processList.deleteIndex].id)}
        }, function (err, result)
        {
            if (!err || err === 'Mongodb found none')
            {
                if (result) //Exists
                {
                    mongo.deleteOne({
                        name: TARGET_CONFIG,
                        db: TARGET_DB,
                        collection: TARGET_COLLECTION,
                        query: {_id: new mongo.ObjectID(processList.deleteList[processList.deleteIndex].id)}
                    }, (err, result) =>
                    {
                        console.log('DELETING: affect:' + result.result.n + ', success:' + result.result.ok + ', '
                            + util2.debugDump(processList.deleteList[processList.deleteIndex], 3, 200, false));
                        processList.deleteIndex++;
                        callback(err)
                    });
                }
                else // Not exists
                {
                    console.log('DELETING: SKIP(NOT-Exists): '
                        + util2.debugDump(processList.deleteList[processList.deleteIndex], 3, 200, false));
                    processList.deleteIndex++;
                    callback(null);
                }
            }
            else
            {
                callback(err);
            }
        });
    }
    catch (exception)
    {
        console.log('doDeleteTask try-catch exception:' + exception);
        callback(exception);
    }
}