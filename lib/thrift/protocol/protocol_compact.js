/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
var sys = require('sys'),
    Thrift = require('../thrift'),
    Type = Thrift.Type,
    assert = require('assert');

var TProtocolException = require('./protocol').TProtocolException;

var BinaryParser = require('../utils/binary_parser').BinaryParser;
BinaryParser.bigEndian = true;

var TCompactProtocol = exports.TCompactProtocol = function(trans, strictRead, strictWrite) {
  this.trans = trans;
  this.strictRead = (strictRead !== undefined ? strictRead : false);
  this.strictWrite = (strictWrite !== undefined ? strictWrite : true);i
  this.last_fid = 0;
  this.bool_fid = null;
  this.bool_value = null;
  this.structs = [];
  this.containers = [];
  this.state = CLEAR;
};

// XXX set state?
TCompactProtocol.prototype.flush = function() {
  return this.trans.flush();
};

// NastyHaxx. JavaScript forces hex constants to be
// positive, converting this into a long. If we hardcode the int value
// instead it'll stay in 32 bit-land.
/*
var VERSION_MASK = -65536, // 0xffff0000
    VERSION_1 = -2147418112, // 0x80010000
    TYPE_MASK = 0x000000ff;
*/
var UNKNOWN = 0,
    INVALID_DATA = 1,
    NEGATIVE_SIZE = 2,
    SIZE_LIMIT = 3,
    BAD_VERSION = 4;

// state-machine values
var CLEAR = 0,
    FIELD_WRITE = 1,
    VALUE_WRITE = 2,
    CONTAINER_WRITE = 3,
    BOOL_WRITE = 4,
    FIELD_READ = 5,
    CONTAINER_READ = 6,
    VALUE_READ = 7,
    BOOL_READ = 8;

var CompactType = {
  TRUE:1,
  FALSE:2,
  BYTE:3,
  I16:4,
  I32:5,
  I64:6,
  DOUBLE:7,
  BINARY:8,
  LIST:9,
  SET,10,
  MAP:11,
  STRUCT:12
};

var CTYPES = [];
CTYPES[Type.BOOL] = CompactType.TRUE;
CYTPES[TYPE.BYTE] = CompactType.BYTE;
CYTPES[Type.BOOL] = CompactType.TRUE,
CYTPES[Type.BYTE] = CompactType.BYTE,
CYTPES[Type.I16] = CompactType.I16,
CYTPES[Type.I32] = CompactType.I32,
CYTPES[Type.I64] = CompactType.I64,
CYTPES[Type.DOUBLE] = CompactType.DOUBLE,
CYTPES[Type.STRING] = CompactType.BINARY,
CYTPES[Type.STRUCT] = CompactType.STRUCT,
CYTPES[Type.LIST] = CompactType.LIST,
CYTPES[Type.SET] = CompactType.SET,
CYTPES[Type.MAP] = CompactType.MAP
}

var makeZigZag = function(n, bits){
  return Math.pow( (n << 1) , (n >> (bits - 1)));
};
var fromZigZag(n) = function(n){
  return Math.pow( (n >> 1) , (-1*(n&1)));
};

var PROTOCOL_ID = 0x82,
    VERSION = 1,
    VERSION_MASK = 0x1f,
    TYPE_MASK = 0xe0,
    TYPE_SHIFT_AMOUNT = 5;

TCompactProtocol.prototype.writeVarint = function(trans, n){
  var out = [];
  var keepGoing = true;
  while( keepGoing ){
    if( (n & 0x80) === 0 ){
      out.push(n);
      keepGoing = false;
    } else {
      out.push((n & 0xff) | 0x80);
      n = n >> 7;
    }
  }
  // XXX translate this from Python
  trans.write(''.join(map(chr,out)));
}

TCompactProtocol.prototype.readVarint = function(trans){
  var result = 0;
  var shift = 0;
  var keepGoing = true;
  var x = 0;
  var _byte = 0;
  while( keepGoing ){
    x = trans.readAll(1);
    _byte = ord(x);  // XXX translate this from Python
    result |= (_byte & 0x7f) << shift;
    shfit += 7;
    if( (byte >> 7) == 0 ){
      keepGoing = false;
    }
  }
  return result;
}

TCompactProtocol.prototype.writeMessageBegin = function(name, type, seqid) {
  // removed strict read/write --jlk
  assert.equal(this.state, CLEAR); 
  this.writeByte(PROTOCOL_ID);
  this.writeByte(VERSION | (type << TYPE_SHIFT_AMOUNT));
  this.writeVarint(seqid);
  this.writeString(name);
  this.state = VALUE_WRITE;
};

TCompactProtocol.prototype.writeMessageEnd = function() {
  assert.equal(this.state, VALUE_WRITE);
  this.state = CLEAR;
};

TCompactProtocol.prototype.writeStructBegin = function(name) {
  if( (this.state !== CLEAR) || (this.state != CONTAINER_WRIT)E || (this.state != VALUE_WRIT) E){
    assert.ok(false);
  }
  this.structs.push(this.state)
  this.structs.push( this.last_fid);
  this.state = FIELD_WRITE;
  this.last_fid = 0;
};

TCompactProtocol.prototype.writeStructEnd = function() {
  assert.equal(this.state, FIELD_WRITE);
  this.last_fid = this.structs.pop();
  this.state = this.structs.pop();  
}
TCompactProtocol.prototype.writeFieldStop = function() {
  this.writeByte(Type.STOP);
};


TCompactProtocol.prototype.writeFieldHeader = function(type, fid){
  var delta = fid - this.last_fid;
  if( (delta > 0)  &&  (delta <= 15) ){
    this.writeByte(delta << 4 | type);
  } else {
    this.writeByte(type);
    this.writeI16(fid);
  }
  this.last_fid = fid;
};

TCompactProtocol.prototype.writeFieldBegin = function(name, type, id) {
  assert.equal(this.state, FIELD_WRITE);
  if( type === Type.BOOL ){
    this.state = BOOL_WRITE;
    this.bool_fid = fid;
  } else {
    this.state = VALUE_WRITE;
    this.writeFieldHeader(CTYPES[type], fid);
  }
};

TCompactProtocol.prototype.writeFieldEnd = function() {
  if( (this.state !== VALUE_WRITE) || (this.state !== BOOL_WRITE) ){
    assert.ok(false);
  }
  this.state = FIELD_WRITE;
};

TCompactProtocol.prototype.writeCollectionBegin = function(etype, size) {
  
};

TCompactProtocol.prototype.writeMapBegin = function(ktype, vtype, size) {
  this.writeByte(ktype);
  this.writeByte(vtype);
  this.writeI32(size);
};

TCompactProtocol.prototype.writeCollectionEnd = function() {

};

TCompactProtocol.prototype.writeMapEnd = function() {
};

TCompactProtocol.prototype.writeListBegin = function(etype, size) {
  this.writeByte(etype);
  this.writeI32(size);
};

TCompactProtocol.prototype.writeListEnd = function() {
};

TCompactProtocol.prototype.writeSetBegin = function(etype, size) {
  console.log('write set', etype, size);
  this.writeByte(etype);
  this.writeI32(size);
};

TCompactProtocol.prototype.writeSetEnd = function() {
};

TCompactProtocol.prototype.writeBool = function(bool) {
  if (bool) {
    this.writeByte(1);
  } else {
    this.writeByte(0);
  }
};

TCompactProtocol.prototype.writeByte = function(byte) {
  this.trans.write(BinaryParser.fromByte(byte));
};

TCompactProtocol.prototype.writeI16 = function(i16) {
  this.trans.write(BinaryParser.fromShort(i16));
};

TCompactProtocol.prototype.writeI32 = function(i32) {
  this.trans.write(BinaryParser.fromInt(i32));
};

TCompactProtocol.prototype.writeI64 = function(i64) {
  this.trans.write(BinaryParser.fromLong(i64));
};

TCompactProtocol.prototype.writeDouble = function(dub) {
  this.trans.write(BinaryParser.fromDouble(dub));
};

TCompactProtocol.prototype.writeString = function(str) {
  this.writeI32(str.length);
  this.trans.write(str);
};

TCompactProtocol.prototype.readMessageBegin = function() {
  var sz = this.readI32();
  var type, name, seqid;

  if (sz < 0) {
    var version = sz & VERSION_MASK;
    if (version != VERSION_1) {
      console.log("BAD: " + version);
      throw TProtocolException(BAD_VERSION, "Bad version in readMessageBegin: " + sz);
    }
    type = sz & TYPE_MASK;
    name = this.readString();
    seqid = this.readI32();
  } else {
    if (this.strictRead) {
      throw TProtocolException(BAD_VERSION, "No protocol version header");
    }
    name = this.trans.read(sz);
    type = this.readByte();
    seqid = this.readI32();
  }
  return {fname: name, mtype: type, rseqid: seqid};
};

TCompactProtocol.prototype.readMessageEnd = function() {
};

TCompactProtocol.prototype.readStructBegin = function() {
  return {fname: ''};
};

TCompactProtocol.prototype.readStructEnd = function() {
};

TCompactProtocol.prototype.readFieldBegin = function() {
  var type = this.readByte();
  if (type == Type.STOP) {
    return {fname: null, ftype: type, fid: 0};
  }
  var id = this.readI16();
  return {fname: null, ftype: type, fid: id};
};

TCompactProtocol.prototype.readFieldEnd = function() {
};

TCompactProtocol.prototype.readMapBegin = function() {
  var ktype = this.readByte();
  var vtype = this.readByte();
  var size = this.readI32();
  return {ktype: ktype, vtype: vtype, size: size};
};

TCompactProtocol.prototype.readMapEnd = function() {
};

TCompactProtocol.prototype.readListBegin = function() {
  var etype = this.readByte();
  var size = this.readI32();
  return {etype: etype, size: size};
};

TCompactProtocol.prototype.readListEnd = function() {
};

TCompactProtocol.prototype.readSetBegin = function() {
  var etype = this.readByte();
  var size = this.readI32();
  return {etype: etype, size: size};
};

TCompactProtocol.prototype.readSetEnd = function() {
};

TCompactProtocol.prototype.readBool = function() {
  var byte = this.readByte();
  if (byte === 0) {
    return false;
  }
  return true;
};

TCompactProtocol.prototype.readByte = function() {
  var buff = this.trans.read(1);
  return BinaryParser.toByte(buff);
};

TCompactProtocol.prototype.readI16 = function() {
  var buff = this.trans.read(2);
  return BinaryParser.toShort(buff);
};

TCompactProtocol.prototype.readI32 = function() {
  var buff = this.trans.read(4);
  // console.log("read buf: ");
  // console.log(buff);
  // console.log("result: " + BinaryParser.toInt(buff));
  return BinaryParser.toInt(buff);
};

TCompactProtocol.prototype.readI64 = function() {
  var buff = this.trans.read(8);
  return BinaryParser.toLong(buff);
};

TCompactProtocol.prototype.readDouble = function() {
  var buff = this.trans.read(8);
  return BinaryParser.toFloat(buff);
};

TCompactProtocol.prototype.readBinary = function() {
  var len = this.readI32();
  return this.trans.read(len);
};

TCompactProtocol.prototype.readString = function() {
  var r = this.readBinary().toString('binary');
  // console.log("readString: " + r);
  return r;
};

TCompactProtocol.prototype.getTransport = function() {
  return this.trans;
};  

TCompactProtocol.prototype.skip = function(type) {
  // console.log("skip: " + type);
  switch (type) {
    case Type.STOP:
      return;
    case Type.BOOL:
      this.readBool();
      break;
    case Type.BYTE:
      this.readByte();
      break;
    case Type.I16:
      this.readI16();
      break;
    case Type.I32:
      this.readI32();
      break;
    case Type.I64:
      this.readI64();
      break;
    case Type.DOUBLE:
      this.readDouble();
      break;
    case Type.STRING:
      this.readString();
      break;
    case Type.STRUCT:
      this.readStructBegin();
      while (true) {
        r = this.readFieldBegin();
        if (r.ftype === Type.STOP) {
          break;
        }
        this.skip(r.ftype);
        this.readFieldEnd();
      }
      this.readStructEnd();
      break;
    case Type.MAP:
      r = this.readMapBegin();
      for (var i = 0; i < r.size; ++i) {
        this.skip(r.ktype);
        this.skip(r.vtype);
      }
      this.readMapEnd();
      break;
    case Type.SET:
      var r = this.readSetBegin();
      for (var i = 0; i < r.size; ++i) {
        this.skip(r.etype);
      }
      this.readSetEnd();
      break;
    case Type.LIST:
      r = this.readListBegin();
      for (var i = 0; i < r.size; ++i) {
        this.skip(r.etype);
      }
      this.readListEnd();
      break;
    default:
      throw Error("Invalid type: " + type);
  }
}
