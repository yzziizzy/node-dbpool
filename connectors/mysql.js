var mysql = require('mysql');




function MysqlConnector(opts) {
	this.conn = null;
	
	this.errorHandler = function(err) { console.log(err) };
	this.releaseCallback = function(err) { console.log('unassigned MysqlConnector release callback'); };
	
	this.opts = _.extend({}, opts);
}


// creates a new connection and returns it in the callback
MysqlConnector.prototype.connect = function(schema, cb) {
	var that = this;
	
	this.conn = mysql.createConnection({
		host: this.opts.host,
		user: this.opts.user,
		password: this.opts.pass,
		database: schema || this.opts.defaultDB
	});
	
	this.conn.connect(onConnect);
	
	
	function onConnect(err) {
		cb(err, that);
	};
}


// run arbitrary sql. no escaping, no magic.
MysqlConnector.prototype.query = function(sql, cb) {
	this.conn.query(sql, cb);
}

// sets the schema
MysqlConnector.prototype.setSchema = function(schema, cb) {
	this.conn.query("USE " + mysql.escapeId(schema), cb);
}


// called when there is an error on the connection. 
// the connection is assumed to be in an invalid state and will be released.
// TODO: add non-fatal error flag
MysqlConnector.prototype.onError = function(handler) {
	this.errorHandler = handler;
	this.conn.on('error', this.errorHandler);
}

// called when a connection is ended, gracefully or not
MysqlConnector.prototype.onRelease = function(callback) {
	this.releaseCallback = callback;
}


// graceful shutdown
MysqlConnector.prototype.release = function() {
	this.conn.end(this.releaseCallback);
};

// not so graceful shutdown
MysqlConnector.prototype.terminate = function() {
	this.conn.destroy();
	this.releaseCallback();
};



MysqlConnector.prototype.escape = mysql.escape;
MysqlConnector.prototype.escapeID = mysql.escapeId;




module.exports = function() {
	return new MysqlConnector();
};

