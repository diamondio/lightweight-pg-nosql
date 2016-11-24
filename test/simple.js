var assert  = require('assert');
var uuid    = require('uuid')
var pg      = require('pg');
var PostgresDB = require('../');

var testConfiguration = {
  host:'localhost',
  user: '',
  password: '',
  database: 'pgnosqltest',
  port:5432,
  lockExpirationTime: 60
};

describe('Simple Tests', function () {
  before(function (done) {
    var db = new pg.Client('postgres://localhost/template1');
    db.connect(function (err) {
      if (err) return done();
      db.query(`DROP DATABASE "pgnosqltest"`, function (err) {
        db.end(function (err) {
          done();
        });
      });
    });
  })

  it('Create a db object', function (done) {
    var db = new PostgresDB(testConfiguration);
    db._afterInitialization(done);
  });

  it('Insert and retrieve object', function (done) {
    var db = new PostgresDB(testConfiguration);
    var objectId = uuid.v4();
    db.upsertObject('testTable2', objectId, {key: 'value'}, function (err) {
      assert.equal(err, null);
      db.getObject('testTable2', objectId, function (err, result) {
        assert.equal(err, null);
        assert.equal(result.key, 'value');
        done();
      })
    });
  });

  it('Insert and the empty object', function (done) {
    var db = new PostgresDB(testConfiguration);
    var objectId = uuid.v4();
    db.upsertObject('testTable20', objectId, {}, function (err) {
      assert.equal(err, null);
      db.getObject('testTable20', objectId, function (err, result) {
        assert.equal(err, null);
        assert.deepEqual(result, {});
        done();
      })
    });
  });

  it('Insert and retrieve object with capitalized keys', function (done) {
    var db = new PostgresDB(testConfiguration);
    var objectId = uuid.v4();
    db.upsertObject('testTable3', objectId, {Key: 'value'}, function (err) {
      assert.equal(err, null);
      db.getObject('testTable3', objectId, function (err, result) {
        assert.equal(err, null);
        assert.equal(result.Key, 'value');
        done();
      })
    });
  });

  it('Upsert several into the same object', function (done) {
    var db = new PostgresDB(testConfiguration);
    var objectId = uuid.v4();
    db.upsertObject('testTable4', objectId, {Key: 'value'}, function (err) {
      assert.equal(err, null);
      db.upsertObject('testTable4', objectId, {key: 'value'}, function (err) {
        assert.equal(err, null);
        db.getObject('testTable4', objectId, function (err, result) {
          assert.equal(err, null);
          assert.equal(result.Key, 'value');
          assert.equal(result.key, 'value');
          done();
        });
      });
    });
  });

  it('Upsert several types', function (done) {
    var db = new PostgresDB(testConfiguration);
    var objectId = uuid.v4();

    var ob = {
      Key: {
        somethingDeep: 'bloop',
      },
      num: 12,
      othernum: '12',
      alist: [1, 2, 3, '4']
    }

    db.upsertObject('testTable5', objectId, ob, function (err) {
      assert.equal(err, null);
      db.getObject('testTable5', objectId, function (err, result) {
        assert.equal(err, null);
        assert.deepEqual(result, ob);
        done();
      });
    });
  });

  it('Upsert several types with new object', function (done) {
    var db = new PostgresDB(testConfiguration);
    var objectId = uuid.v4();

    var ob = {
      Key: {
        somethingDeep: 'bloop',
      },
      num: 12,
      othernum: '12',
      alist: [1, 2, 3, '4']
    }

    db.upsertObject('testTable7', objectId, ob, function (err) {
      assert.equal(err, null);
      db = new PostgresDB(testConfiguration);
      db.getObject('testTable7', objectId, function (err, result) {
        assert.equal(err, null);
        assert.deepEqual(result, ob);
        done();
      });
    });
  });

  it('Remove an object', function (done) {
    var db = new PostgresDB(testConfiguration);
    var objectId = uuid.v4();
    db.upsertObject('testTable6', objectId, {key: 'value'}, function (err) {
      assert.equal(err, null);
      db.removeObject('testTable6', objectId, function (err) {
        assert.equal(err, null);
        db.getObject('testTable6', objectId, function (err, result) {
          assert.equal(err, null);
          assert.equal(result, null);
          done();
        });
      });
    });
  });

  var trouble = { created_time: '2016-11-23T23:12:53.983Z',
    service: 'outlook',
    uri: 'ca81a3e2-9e66-4d3d-b13c-22c7aaf5c92c',
    emailDescription: 'an attachment! ooooooo',
    messageId: 'AQMkADAwATNiZmYAZC0wMTlkLWQ1NjktMDACLTAwCgBGAAADTWCDb_AeaU2aAmthBvoOHgcAFp9IUDCaPEG5aPJFjPaf3AAAAgk3AAAAFp9IUDCaPEG5aPJFjPaf3AAAATEirAAAAA==',
    createdDateTime: '2016-11-23T22:06:16Z',
    lastModifiedDateTime: '2016-11-23T22:16:54Z',
    changeKey: 'CQAAABYAAAAWn0hQMJo8Qblo8kWM9p/cAAAAMX3+',
    receivedDateTime: '2016-11-23T22:16:52Z',
    sentDateTime: '2016-11-23T22:16:52Z',
    hasAttachments: true,
    internetMessageId: '<DM5PR02MB23960D2933B5BFE874784636D2B70@DM5PR02MB2396.namprd02.prod.outlook.com>',
    subject: 'lets send something cool',
    importance: 'Normal',
    sender: { EmailAddress: { Name: 'Diamond Ops', Address: 'opsdiamondio@outlook.com' } },
    from: { EmailAddress: { Name: 'Diamond Ops', Address: 'opsdiamondio@outlook.com' } },
    toRecipients: 
     [ { EmailAddress: 
          { Name: 'ert.mcscrad@gmail.com',
            Address: 'ert.mcscrad@gmail.com' } } ],
    ccRecipients: [],
    bccRecipients: [],
    conversationId: 'AQQkADAwATNiZmYAZC0wMTlkLWQ1NjktMDACLTAwCgAQACUNRHASD89LpvyXdyPS1N4=',
    isDraft: false,
    webLink: 'https://outlook.live.com/owa/?ItemID=AQMkADAwATNiZmYAZC0wMTlkLWQ1NjktMDACLTAwCgBGAAADTWCDb%2BAeaU2aAmthBvoOHgcAFp9IUDCaPEG5aPJFjPaf3AAAAgk3AAAAFp9IUDCaPEG5aPJFjPaf3AAAATEirAAAAA%3D%3D&exvsurl=1&viewModel=ReadMessageItem',
    emailContent: 'an attachment! ooooooo',
    fileName: 'Screen Shot 2016-11-16 at 4.36.01 PM.png',
    filePath: 'outlook/Screen Shot 2016-11-16 at 4.36.01 PM.png',
    attachmentExtraProperties: 
     { type: '#Microsoft.OutlookServices.FileAttachment',
       id: 'AQMkADAwATNiZmYAZC0wMTlkLWQ1NjktMDACLTAwCgBGAAADTWCDb_AeaU2aAmthBvoOHgcAFp9IUDCaPEG5aPJFjPaf3AAAAgk3AAAAFp9IUDCaPEG5aPJFjPaf3AAAATEirAAAAAESABAA1BXDVSX6CEyhJgNjg1lDyw==',
       lastModifiedDateTime: '2016-11-23T22:16:52Z',
       name: 'Screen Shot 2016-11-16 at 4.36.01 PM.png',
       contentType: 'image/png',
       size: 215708,
       isInline: false },
    storageDescriptor: 
     { dbname: '58361aee9d0c0b3d6dd2b4e3',
       accountId: '0003bffd-019d-d569-0000-000000000000@84df9e7f-e9f6-40af-b435-aaaaaaaaaaaa',
       messageId: 'AQMkADAwATNiZmYAZC0wMTlkLWQ1NjktMDACLTAwCgBGAAADTWCDb_AeaU2aAmthBvoOHgcAFp9IUDCaPEG5aPJFjPaf3AAAAgk3AAAAFp9IUDCaPEG5aPJFjPaf3AAAATEirAAAAA==',
       attachmentId: 'AQMkADAwATNiZmYAZC0wMTlkLWQ1NjktMDACLTAwCgBGAAADTWCDb_AeaU2aAmthBvoOHgcAFp9IUDCaPEG5aPJFjPaf3AAAAgk3AAAAFp9IUDCaPEG5aPJFjPaf3AAAATEirAAAAAESABAA1BXDVSX6CEyhJgNjg1lDyw==' },
    dbname: undefined }

  it('Insert particularly troublesome object', function (done) {
    var db = new PostgresDB(testConfiguration);
    var objectId = uuid.v4();
    // make things awkward.
    db.upsertObject('outlook', objectId, trouble, function (err) {});
    db.upsertObject('outlook', objectId, trouble, function (err) {});
    db.upsertObject('outlook', objectId, trouble, function (err) {});
    db.upsertObject('outlook', objectId, trouble, function (err) {});
    db.upsertObject('outlook', objectId, trouble, function (err) {});
    db.upsertObject('outlook', objectId, trouble, function (err) {
      assert.equal(err, null);
      db.upsertObject('outlook2', objectId, trouble, function (err) {
        assert.equal(err, null);
        db.getObject('outlook', objectId, function (err, result) {
          assert.equal(err, null);
          assert.deepEqual(result, trouble);
          done();
        });
      });
    });
  });
});
