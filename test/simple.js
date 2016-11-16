var assert  = require('assert');
var uuid    = require('node-uuid')
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
});
