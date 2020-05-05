const mongodb = require('mongodb');
const ObjectId = require('mongodb').ObjectId;
const MongoMemoryReplSet = require('mongodb-memory-server').MongoMemoryReplSet;
const microseconds = require('microseconds');

const calc = require('./calc');

const replSet = new MongoMemoryReplSet({
  debug: false,
  replSet: { storageEngine: 'wiredTiger' },
});

const MongoClient = mongodb.MongoClient;
let client;
let db;

// prepare data
// array of teachers { _id: new ObjectId() }
// array of students { _id: new ObjectId(), teacherId: new ObjectId() }
// array of exams { _id: new ObjectId(), passed: true }
const teachers = [];
const students = [];
const exams = [];
for (let i = 0; i < 100; i++) {
  const teacherId = new ObjectId();
  const student1Id = new ObjectId();
  const student2Id = new ObjectId();
  const student3Id = new ObjectId();
  teachers.push({ _id: teacherId });
  students.push({ _id: student1Id, teacherId: teacherId });
  students.push({ _id: student2Id, teacherId: teacherId });
  students.push({ _id: student3Id, teacherId: teacherId });
  exams.push({ _id: new ObjectId(), studentId: student1Id, passed: true });
  exams.push({ _id: new ObjectId(), studentId: student1Id, passed: false });
  exams.push({ _id: new ObjectId(), studentId: student1Id, passed: true });
  exams.push({ _id: new ObjectId(), studentId: student2Id, passed: true });
  exams.push({ _id: new ObjectId(), studentId: student2Id, passed: false });
  exams.push({ _id: new ObjectId(), studentId: student2Id, passed: true });
  exams.push({ _id: new ObjectId(), studentId: student3Id, passed: true });
  exams.push({ _id: new ObjectId(), studentId: student3Id, passed: false });
  exams.push({ _id: new ObjectId(), studentId: student3Id, passed: true });
}

// prepare mongoDB and create collections 'teachers', 'students', 'exams'
const createMongoInstance = async () => {
  await replSet.waitUntilRunning();
  const connectionString = await replSet.getConnectionString();
  const dbName = await replSet.getDbName();
  client = new MongoClient(connectionString, { useNewUrlParser: true, useUnifiedTopology: true });
  await client.connect();
  db = client.db(dbName);
  await db.createCollection('teachers');
  await db.createCollection('students');
  await db.createCollection('exams');
  return db;
};

// get data by multiple requests to DB (students by each teacher and exams by each student)
const multipleRequests = async () => {
  const before = microseconds.now();

  const teachers = await db.collection('teachers').find({}).toArray();
  for (const teacher of teachers) {
    teacher.examsPassed = 0;
    const students = await db.collection('students').find({ teacherId: teacher._id }).toArray();
    for (const student of students) {
      const exams = await db.collection('exams').find({ studentId: student._id }).toArray();
      teacher.examsPassed += exams.filter((exam) => exam.passed).length;
    }
  }

  const after = before + microseconds.since(before);
  return (after - before) / 1000;
};

// get all entities from collections 'teachers', 'students' and 'exams' and filter students by teacher and exams by student
const getCollections = async () => {
  const before = microseconds.now();

  const teachers = await db.collection('teachers').find({}).toArray();
  const students = await db.collection('students').find({}).toArray();
  const exams = await db.collection('exams').find({}).toArray();
  for (const teacher of teachers) {
    teacher.examsPassed = 0;
    for (const student of students.filter((student) => student.teacherId.toString() === teacher._id.toString())) {
      teacher.examsPassed += exams.filter((exam) => exam.studentId.toString() === student._id.toString()).filter((exam) => exam.passed).length;
    }
  }

  const after = before + microseconds.since(before);
  return (after - before) / 1000;
};

// use MongoDB aggregation function to get data from different collections
const aggregation = async () => {
  const before = microseconds.now();

  const teachers = await db.collection('teachers').aggregate([
    { $match: {} },
    { $lookup: { from: 'students', localField: '_id', foreignField: 'teacherId', as: 'students' } },
    { $lookup: { from: 'exams', localField: 'students._id', foreignField: 'studentId', as: 'exams' } },
    { $project: { students: 0 } },
    { $addFields: {
        examsPassedArray: {
          $filter: {
            input: '$exams',
            as: 'exams',
            cond: { $eq: ['$$exams.passed', true] },
          },
        },
      } },
    { $project: { examsPassed: { $size: '$examsPassedArray' } } },
  ]).toArray();

  const after = before + microseconds.since(before);
  return (after - before) / 1000;
};

// function to get array of results [{ _id: ObjectId(), examsPassed: 6 }, ...]
(async () => {
  try {
    await createMongoInstance();
    await db.collection('teachers').insertMany(teachers);
    await db.collection('students').insertMany(students);
    await db.collection('exams').insertMany(exams);

    // some indexes added
    // await db.collection('students').createIndex({ 'teacherId': 1 });
    // await db.collection('exams').createIndex({ 'studentId': 1 });

    const numberOfIterations = Number(process.env.ITERATIONS_NUMBER);
    console.log('Number of iterations: ', numberOfIterations);

    const multipleRequestsResults = [];
    const getCollectionsResults = [];
    const aggregationResults = [];

    for (const i of Array(numberOfIterations)) {
      const multipleRequestsResult = await multipleRequests();
      const getCollectionsResult = await getCollections();
      const aggregationResult = await aggregation();

      multipleRequestsResults.push(multipleRequestsResult);
      getCollectionsResults.push(getCollectionsResult);
      aggregationResults.push(aggregationResult);
    }

    const filteredMultipleRequests = calc.filterOutliers(multipleRequestsResults);
    const filteredGetCollections = calc.filterOutliers(getCollectionsResults);
    const filteredAggregationResults = calc.filterOutliers(aggregationResults);

    const meanMultipleRequests = calc.getMean(filteredMultipleRequests);
    const meanGetCollections = calc.getMean(filteredGetCollections);
    const meanAggregationResults = calc.getMean(filteredAggregationResults);

    const SDMultipleRequests = calc.getSD(filteredMultipleRequests);
    const SDGetCollections = calc.getSD(filteredGetCollections);
    const SDAggregationResults = calc.getSD(filteredAggregationResults);

    console.log(`Multiple Requests: ${meanMultipleRequests.toFixed(2)} +- ${SDMultipleRequests.toFixed(2)} milliseconds`);
    console.log(`Get collections: ${meanGetCollections.toFixed(2)} +- ${SDGetCollections.toFixed(2)} milliseconds`);
    console.log(`Aggregation: ${meanAggregationResults.toFixed(2)} +- ${SDAggregationResults.toFixed(2)} milliseconds`);

    await db.collection('teachers').drop();
    await db.collection('students').drop();
    await db.collection('exams').drop();
    await client.close();
    await replSet.stop();
    process.exit(0);
  } catch (e) {
    console.log(e);
    process.exit(1);
  }
})();
