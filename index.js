var _           = require('lodash');
var async       = require('async');
var pg          = require('pg');

function PostgresDB (opts) {
  var self = this;
  if (!opts.host)                   throw new Error('PostgresDB must be initialized with a host in the options argument');
  if  (opts.user === undefined)     throw new Error('PostgresDB must be initialized with a user in the options argument');
  if  (opts.password === undefined) throw new Error('PostgresDB must be initialized with a password in the options argument');
  if (!opts.database)               throw new Error('PostgresDB must be initialized with a database in the options argument');
  if (!opts.port)                   throw new Error('PostgresDB must be initialized with a port in the options argument');
  
  self._pgConnectionConfig = {
    host: opts.host,
    user: opts.user,
    password: opts.password,
    database: opts.database,
    port: opts.port,
    max: 4,
    idleTimeoutMillis: 30000,
  };

  self._typeMapping = {};
  self._initialization_callbacks = [];
  self._initialize(5);
}

var CreateDB = function (config, cb) {
  var database = config.database;
  var pgUrl = `postgres://${config.user}:${config.password}@${config.host}:${config.port}/template1`;
  var db = new pg.Client(pgUrl);

  db.connect(function (err) {
    if (err) return cb(err);
    db.query(`CREATE DATABASE "${database}"`, function (err) {
      db.end();
      cb();
    })
  })
}

PostgresDB.prototype._loadTypeMapping = function (cb) {
  var self = this;
  self._db.query(`CREATE TABLE IF NOT EXISTS NoSQLPostgresTypeMapping ("tableName" TEXT, "columnName" TEXT, "type" TEXT);`, function (err) {
    if (err) return cb({ message: `Error while initializing PostgresDB ${err}`, details: err });
    self._db.query(`SELECT * FROM NoSQLPostgresTypeMapping`, function (err, results) {
      if (err) return cb({ message: `Error while initializing PostgresDB ${err}`, details: err });
      results.rows.forEach(function (row) {
        if (!self._typeMapping[row.tableName]) self._typeMapping[row.tableName] = {postgresId: 'string'};
        self._typeMapping[row.tableName][row.columnName] = row.type;
      });
      cb();
    });
  });
};

PostgresDB.prototype._initialize = function () {
  var self = this;

  var createDatabase = function (cb) {
    CreateDB(self._pgConnectionConfig, cb);
  }

  createDatabase(function (err) {
    //ignore the error and just keep going
    self._db = new pg.Pool(self._pgConnectionConfig);
    var db = self._db;

    self._loadTypeMapping(function (err) {
      if (err) throw new Error(err);
      self._initialized = true;
      self._initialization_callbacks.forEach(function(fn) {
        fn();
      });
      delete self._initialization_callbacks;
    });
  });
}

PostgresDB.prototype._prepTable = function (tableName, cb) {
  var self = this;
  if (self._typeMapping[tableName]) return cb();

  self._db.query(`CREATE TABLE IF NOT EXISTS ${tableName} ("postgresId" TEXT);
    CREATE UNIQUE INDEX IF NOT EXISTS ${tableName}Index ON ${tableName} ("postgresId");`, function (err) {
    self._db.query(`INSERT INTO NoSQLPostgresTypeMapping ("tableName", "columnName", "type") VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`, [tableName, 'postgresId', 'string'], function (err) {
      if (err) return cb(err);
      if (!self._typeMapping[tableName]) self._typeMapping[tableName] = {postgresId: 'string'};
      cb();
    });
  });
}

var determineStorageType = function (object) {
  if (object instanceof Date) return 'date';
  if (_.isString(object)) return 'string';
  if (_.isNumber(object)) return 'number';
  return 'object';
}

var renderType = function (object, type) {
  switch (type) {
    case 'date': return object.toISOString();
    case 'string': return object;
    case 'number': return JSON.stringify(object);
    case 'object': return JSON.stringify(object);
    default: throw new Error(`Unknown type ${type}`)
  }
}

var reconstructType = function (object, type) {
  switch (type) {
    case 'date': return new Date(object);
    case 'string': return object;
    case 'number': return JSON.parse(object);
    case 'object': return JSON.parse(object);
    default: throw new Error(`Unknown type ${type}`)
  }
}

var typeMap = {
  'date': 'TEXT',
  'string': 'TEXT',
  'number': 'TEXT',
  'object': 'TEXT',
}

PostgresDB.prototype._prepColumn = function (tableName, colName, type, cb) {
  if (!tableName) return setImmediate(function () { cb('tableName undefined') });
  var self = this;
  self._db.query(`ALTER TABLE ${tableName} ADD COLUMN "${colName}" ${typeMap[type]}`, function (err) {
    self._db.query(`INSERT INTO NoSQLPostgresTypeMapping ("tableName", "columnName", "type") VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`, [tableName, colName, type], function (err) {
      if (err) return cb(err);
      if (!self._typeMapping[tableName]) self._typeMapping[tableName] = {postgresId: 'string'};
      self._typeMapping[tableName][colName] = type;
      cb();
    });
  });
}

PostgresDB.prototype._prepareForTableAndObject = function (tableName, object, cb) {
  if (!tableName) return setImmediate(function () { cb('tableName undefined') });
  var self = this;
  self._prepTable(tableName, function (err) {
    if (err) return cb(err);
    async.each(Object.keys(object), function (key, asyncCB) {
      if (self._typeMapping[tableName][key]) return asyncCB();
      self._prepColumn(tableName, key, determineStorageType(object[key]), asyncCB);
    }, cb);
  });
}

PostgresDB.prototype._afterInitialization = function (cb) {
  if (this._initialized) return setImmediate(cb);
  this._initialization_callbacks.push(cb);
}



PostgresDB.prototype.upsertObject = function (tableName, id, object, cb) {
  if (!tableName) return setImmediate(function () { cb('tableName undefined') });
  var self = this;
  self._afterInitialization(function () {
    self._prepareForTableAndObject(tableName, object, function (err) {
      if (err) return cb(err);
      var keys = Object.keys(object);
      if (keys.length === 0) {
        return self._db.query(`INSERT INTO ${tableName} ("postgresId") VALUES ($1) ON CONFLICT DO NOTHING`, [id], cb);
      }
      var d = 0;
      var dollar = function () {
        d++;
        return '$' + d;
      }
      var query = `INSERT INTO ${tableName} ("postgresId", ${keys.map(k => '"' + k + '"').join(',')}) VALUES (${dollar()}, ${keys.map(k => dollar()).join(',')})
      ON CONFLICT ("postgresId") DO UPDATE SET ${keys.map(k => '"' + k + '"' + ' = ' + dollar()).join(',')};`;
      var renderedValues = keys.map(k => renderType(object[k], self._typeMapping[tableName][k]));
      var queryValues = [id];
      renderedValues.forEach(function (value) {
        queryValues.push(value);
      })
      renderedValues.forEach(function (value) {
        queryValues.push(value);
      })
      self._db.query(query, queryValues, cb);
    });
  });
}

PostgresDB.prototype.getObject = function (tableName, id, cb, shouldFailOnUnknownMapping) {
  if (!tableName) return setImmediate(function () { cb('tableName undefined') });
  var self = this;
  self._afterInitialization(function () {
    if (!self._typeMapping[tableName]) {
      if (shouldFailOnUnknownMapping) return setImmediate(function () { cb('unknown tableName') });
      self._loadTypeMapping(function (err) {
        if (err) return cb(err);
        return setImmediate(function () {
          self.getObject(tableName, id, cb, true);
        });
      });
    }
    self._db.query(`SELECT * FROM ${tableName} WHERE "postgresId" = $1`, [id], function (err, results) {
      if (err) return cb(err);
      var result = results.rows[0];
      if (!result) return cb(null, null);
      var finalResult = {};
      if (_.some(Object.keys(result).map(key => self._typeMapping[tableName][key]), x => x === undefined)) {
        return setImmediate(function () {
          self.getObject(tableName, id, cb, true);
        });
      }
      Object.keys(result).forEach(function (key) {
        if (key === 'postgresId') return;
        finalResult[key] = reconstructType(result[key], self._typeMapping[tableName][key]);
      });
      return cb(null, finalResult);
    });
  });
}

PostgresDB.prototype.removeObject = function (tableName, id, cb) {
  if (!tableName) return setImmediate(function () { cb('tableName undefined') });
  var self = this;
  self._afterInitialization(function () {
    self._db.query(`DELETE FROM ${tableName} WHERE "postgresId" = $1`, [id], cb);
  });
}


module.exports = PostgresDB;

