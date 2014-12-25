var _ = require('lodash');



module.exports = dbPool;


var dbPool = function(opts) {
	var defaults = {
		globalMax: 0,
		schemaMax: 0,
		schemaMin: 1,
		prefer: 'new', // or 'convert' to change schema of idle connections
		lazy: true, // don't create until needed, don't close until forced
		connectTimeout: 15000,
		
		// user supplied values
		dbCreds: {},
		db: null,
	};
	
	this.opts = _.extend({}, defaults, opts);
	
	this.connections = Object.create(null);
	this.freeConnections = Object.create(null);
	
	this.queue = [];
	
	this.ready = false;
	
	this.totalConnections = 0;
	
};

// needed:
// fetchAll
// _.groupBy type functionality
// fancy transactions
// cursors
// create/find/get info on tables
// multi-host support
// maybe streaming support

// this doesn't check the limits. it does what it's told.
dbPool.prototype.newConnection = function(schema, cb) {
	var that = this;
	
	var pool = this.connections[schema];
	if(!pool) pool = this.connections[schema] = [];
	
	var dbConn = new this.db(this.opts.dbCreds);
	dbConn.connect(schema, withConnection); // this wrapper needs to set the schema
	
	function withConnection(err, dbConn) {
		if(err) return process.nextTick(function() { cb(err) });
		
		var conn = {
			schema: schema,
			conn: dbConn,
			inUse: false,
			error: null,
		};
		
		// listen for strange events.
		dbConn.onError(function(err) {
			conn.ready = err;
			conn.dbConn.release();
		});
		
		dbConn.onRelease(function(err) {
			that.removeConnection(conn);
		});
		
		pool.push(conn);
		
		that.totalConnections++;
		
		process.nextTick(function() { cb(null, conn); }
	};
};

// purges a connection object from the pools
dbPool.prototype.removeConnection = function(conn) {
	this.connections[conn.schema] = _.filter(
		this.connections[conn.schema], 
		function(q) { return q != conn }
	);
	
	this.totalConnections--;
};


	// prolly need some fancy variadic argument support for escaping values
dbPool.prototype.query = function(schema, query, cb) {
	var that = this;
	
	// run or queue query
	getConnection(function(err, conn) {
		if(err) return cb(err);
		if(!conn) return that.queueQuery(schema, query, cb)
	
		// we have a connection, actually run it
		that.runQuery(conn, query, cb);
	});
};



dbPool.prototype.setSchema = function(conn, schema, cb) {
	var that = this;
	
	conn.conn.setSchema(schema, function(err) {
		if(err) return process.nextTick(function() { cb(err) });
		
		// now make sure to move the connection to the new pool
		that.connections[conn.schema] = _.filter(that.connections[conn.schema], function(q) { return q != conn });
		
		var pool = that.connections[schema];
		if(!pool) pool = that.connections[schema] = [];
					
		conn.schema = schema;
		pool.push(conn);
		
		process.nextTick(null, conn);
	});
};


// fetches a connection from the pool and marks it in use
dbPool.prototype.getConnection = function(schema, cb) {
	
	// try to use an idle connection
	if(this.connections[schema]) {
	
		var conn = _.first(this.connections[schema], function(q) {
			return q.connected && !q.inUse;
		});
		
		// we got one
		if(conn) {
			conn.inUse = true;
			return cb(null, conn);
		}
	}
	
	// try to make a new connection
	
	// check max connections
	if(this.totalConnections >= this.opts.globalMax)
		return cb(null, null);
	
	// messy code
	var pool = this.connections[schema];
	if(!pool) return this.newConnection(schema, cb);
	
	if(pool.length >= this.opts.schemaMax)
		return cb(null, null);
	
	this.newConnection(schema, cb);
};


// underlying raw function. not generally to be called externally
dbPool.prototype.runQuery = function(conn, query, cb) {
	
	var sql = this.processQuery(query, conn);
	if(sql == null) return cb('invalid query');
	
	conn.conn.query(sql, function(err, rows) {
		
		conn.inUse = false;
		
		cb(err, rows);
	});
	
};


dbPool.prototype.processQuery = function(query, conn) {
	if(typeof query == 'string') return query;
	if(typeof query == 'function') return query();
	
	// something wrong has happened...
	if(typeof query != 'object') return null;
	
	var raw = query instanceof Array ? query[0] : query.sql;
	var args = query instanceof Array ? query[1] : query.args;
	
	
	
}

dbPool.prototype.escape = function(conn) {
	
	
	
	
}


// checks the queue for 

dbPool.prototype.processQueue = function(cb) {
	
}


dbPool.prototype.waitForConnection = function(schema, cb) {
	
	
};



























