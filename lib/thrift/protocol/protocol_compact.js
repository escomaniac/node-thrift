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
    Type = require('../thrift').Type,
    assert = require('assert');

var TProtocolException = require('./protocol').TProtocolException;

var BinaryParser = require('../utils/binary_parser').BinaryParser;
BinaryParser.bigEndian = true;

var TCompactProtocol = exports.TCompactProtocol = function(trans) {
  this.trans = trans;
  this.last_fid = 0;
  this.bool_fid = null;
  this.bool_value = null;
  this.structs = [];
  this.containers = [];
  this.state = CLEAR;
};

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
  SET:10,
  MAP:11,
  STRUCT:12
};

var CTYPES = [];
CTYPES[Type.BOOL] = CompactType.TRUE;
CTYPES[Type.BYTE] = CompactType.BYTE;
CTYPES[Type.I16] = CompactType.I16;
CTYPES[Type.I32] = CompactType.I32;
CTYPES[Type.I64] = CompactType.I64;
CTYPES[Type.DOUBLE] = CompactType.DOUBLE;
CTYPES[Type.STRING] = CompactType.BINARY;
CTYPES[Type.STRUCT] = CompactType.STRUCT;
CTYPES[Type.LIST] = CompactType.LIST;
CTYPES[Type.SET] = CompactType.SET;
CTYPES[Type.MAP] = CompactType.MAP;

var TTYPES = [];
TTYPES[CompactType.TRUE] = CTYPES[Type.BOOL];
TTYPES[CompactType.BYTE] = CTYPES[Type.BYTE];
TTYPES[CompactType.I16] = CTYPES[Type.I16];
TTYPES[CompactType.I32] = CTYPES[Type.I32];
TTYPES[CompactType.I64] = CTYPES[Type.I64];
TTYPES[CompactType.DOUBLE] = CTYPES[Type.DOUBLE];
TTYPES[CompactType.BINARY] = CTYPES[Type.BINARY];
TTYPES[CompactType.STRUCT] = CTYPES[Type.STRUCT];
TTYPES[CompactType.LIST] = CTYPES[Type.LIST];
TTYPES[CompactType.SET] = CTYPES[Type.SET];
TTYPES[CompactType.MAP] = CTYPES[Type.MAP];
TTYPES[CompactType.FALSE] = CompactType.FALSE;

var PROTOCOL_ID = 0x82,
    VERSION = 1,
    VERSION_MASK = 0x1f,
    TYPE_MASK = 0xe0,
    TYPE_SHIFT_AMOUNT = 5;

// XXX zigZags are highly un-optimized, not sure if this works for large ints
var fromZigZag = TCompactProtocol.prototype.makeZigZag = function(n, bits){

console.log('makeZigZag: '+n);
  // XXX need to handle 64-bit case, js assumes 32-bit everything
  var odd = n%2;
  if( odd ){
    return ((n+1)/(-2));
  } else {
    return n/2;
  }
  //return Math.pow( (n << 1) , (n >> (bits - 1)));
};

var makeZigZag = TCompactProtocol.prototype.fromZigZag = function(n){
console.log('fromZigZag: '+n);
  if (n < 0) {
    return ( (-2*n) -1);
  } else {
    return 2*n;
  }
  //return Math.pow( (n >> 1) , (-1*(n&1)));
};

TCompactProtocol.prototype.writeVarint = function(n){
console.log('writeVarint: '+n);

  var target = [];
  var value = n;
  target[0] = (value | 0x0080);
  if (value >= (1 << 7)) {
    target[1] = ((value >>  7) | 0x0080);
    if (value >= (1 << 14)) {
      target[2] = ((value >> 14) | 0x0080);
      if (value >= (1 << 21)) {
        target[3] = ((value >> 21) | 0x0080);
        if (value >= (1 << 28)) {
          target[4] = (value >> 28);
          //target = target + 5;
        } else {
          target[3] &= 0x007F;
          //target = target + 4;
        }
      } else {
        target[2] &= 0x007F;
        //target = target + 3;
      }
    } else {
      target[1] &= 0x007F;
      //target = target + 2;
    }
  } else {
    target[0] &= 0x007F;
    //target = target + 1;
  }

  var output = target[0] & 0xff;

  for( var jj = 1; jj < target.length; jj++){
    target[jj] &= 0x00ff;
    output = (output << 8) | target[jj];
  }
console.log(' output varint: '+output);

  this.trans.write( BinaryParser.encode_int32(output) );
};

TCompactProtocol.prototype.readVarint = function(){
console.log('readVarint');
  var result = 0;
  var shift = 0;
  var keepGoing = true;
  var x = 0;
  var byte = 0;
  while( keepGoing ){
    x = this.trans.readAll(1);  // read next byte
    //byte = ord(x);  // XXX translate this from Python
    byte = x.charCodeAt(0);
    result |= (byte & 0x7f) << shift;
    shift += 7;
    if( (byte >> 7) == 0 ){
      keepGoing = false;
    }
  }
  return result;
};

TCompactProtocol.prototype.writeMessageBegin = function(name, type, seqid) {
  // removed strict read/write --jlk
  assert.equal(this.state, CLEAR); 
  this.writeByte(PROTOCOL_ID);
  this.writeByte(VERSION | (type << TYPE_SHIFT_AMOUNT));
  this.writeVarint(this.trans, seqid);
  this.writeString(name);
  this.state = VALUE_WRITE;
};

TCompactProtocol.prototype.writeMessageEnd = function() {
  assert.equal(this.state, VALUE_WRITE);
  this.state = CLEAR;
};

TCompactProtocol.prototype.writeStructBegin = function(name) {
  if( (this.state != CLEAR) && (this.state != CONTAINER_WRITE) && (this.state != VALUE_WRITE) ){
    assert.ok(false, 'Invalid State: ' + this.state + ' Expected Either: ' + CLEAR + ' ' + CONTAINER_WRITE + ' ' + VALUE_WRITE);
  }
  this.structs.push({state:this.state, last_fid:this.last_fid});
  this.state = FIELD_WRITE;
  this.last_fid = 0;
};

TCompactProtocol.prototype.writeStructEnd = function() {
  assert.equal(this.state, FIELD_WRITE);
  var structs_pop = this.structs.pop();
  this.last_fid = structs_pop.last_fid;;
  this.state = structs_pop.state;  
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

TCompactProtocol.prototype.writeFieldBegin = function(name, type, fid) {
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
  if( (this.state != VALUE_WRITE) && (this.state != BOOL_WRITE) ){
    assert.ok(false);
  }
  this.state = FIELD_WRITE;
};

// Set and List both are written as Collection
TCompactProtocol.prototype.writeCollectionBegin = function(etype, size) {
  if( (this.state != VALUE_WRITE) && (this.state != CONTAINER_WRITE) ){
    assert.ok(false);
  } 
  if( size <= 14 ){
    this.writeUByte( size << 4 | CTYPES[etype] );
  } else {
    this.writeUByte( 0xf0 | CTYPES[etype] )
    this.writeSize(size);
  }
  this.containers.push(this.state);
  this.state = CONTAINER_WRITE;
};
TCompactProtocol.prototype.writeSetBegin = function(etype, size){
  this.writeCollectionBegin(etype,size);
};
TCompactProtocol.prototype.writeListBegin = function(etype, size){
  this.writeCollectionBegin(etype,size);
};

TCompactProtocol.prototype.writeCollectionEnd = function() {
  assert.equal(this.state, CONTAINER_WRITE);
  this.state = this.containers.pop();
};
TCompactProtocol.prototype.writeListEnd = function() {
  this.writeCollectionEnd();
};
TCompactProtocol.prototype.writeSetEnd = function() {
  this.writeCollectionEnd();
};

TCompactProtocol.prototype.writeMapBegin = function(ktype, vtype, size) {
  if( (this.state != VALUE_WRITE) && (this.state != CONTAINER_WRITE) ){
    assert.ok(false);
  } else {
    this.writeSize(size);
    this.writeUByte(CTYPES[ktype] << 4 | CTYPES[vtype]);
  }
  this.containers.push(this.state);
  this.state = CONTAINER_WRITE;
};

TCompactProtocol.prototype.writeMapEnd = function() {
  this.writeCollectionEnd();
};

TCompactProtocol.prototype.writeBool = function(bool) {
  if ( this.state === BOOL_WRITE ){
    this.writeFieldHeader(types[bool], this.bool_fid);
  } else if ( this.state === CONTAINER_WRITE ){
    if (bool) {
      this.writeByte(1);
    } else {
      this.writeByte(0);
    }
  }
};

TCompactProtocol.prototype.writeUByte = function(byte){
  this.trans.write(BinaryParser.encodeInt( byte, 8, false, true));
};

TCompactProtocol.prototype.writeByte = function(byte){
  this.trans.write(BinaryParser.encodeInt( byte, 8, true, true));
};

TCompactProtocol.prototype.writeSize = function(i32){
console.log('writeSize: '+i32);
  this.writeVarint( i32 );
};

TCompactProtocol.prototype.writeI16 = function(i16) {
console.log('writeI16: '+i16);
  this.writeVarint( makeZigZag(i16,16) );
};

TCompactProtocol.prototype.writeI32 = function(i32) {
console.log('writeI32: '+i32);
  this.writeVarint( makeZigZag(i32,32) );
};

TCompactProtocol.prototype.writeDouble = function(dub) {
  this.trans.write(BinaryParser.fromDouble(dub));
};

TCompactProtocol.prototype.writeString = function(str) {
  this.writeVarint(str.length);
  this.trans.write(str);
};

TCompactProtocol.prototype.readMessageBegin = function() {
console.log('readMessageBegin');
  assert.equal(this.state, CLEAR);
  var proto_id = this.readByte();
  if( proto_id != PROTOCOL_ID ){
    throw TProtocolException( BAD_VERSION, "Bad version in readMessageBegin: " + proto_id);
  }
  var ver_type = this.readUByte();
  var type = (ver_type & TYPE_MASK) >> TYPE_SHIFT_AMOUNT;
  var version = ver_type & VERSION;
  if( version != VERSION ){
    throw TProtocolException( BAD_VERSION, 'Bad version: ' + version + ' expected: ' + VERSION);
  }
  var seqid = this.readVarint();
  var name = this.readString();
  return {fname: name, mtype: type, rseqid: seqid};
};
TCompactProtocol.prototype.readMessageEnd = function() {
  assert.equal(this.state, VALUE_READ);
  assert.equal(this.structs.length, 0);
  this.state = CLEAR;
};

TCompactProtocol.prototype.readStructBegin = function() {
  if( (this.state != CLEAR) && (this.state != CONTAINER_READ) && (this.state != VALUE_READ) ){
    assert.ok(false);
  }
  this.structs.push({state:this.state, last_fid:this.last_fid});
  this.state = FIELD_READ;
  this.last_fid = 0;
};

TCompactProtocol.prototype.readStructEnd = function() {
  assert.equal(this.state, FIELD_READ);
  var structs_pop = this.structs.pop();
  this.state = structs_pop.state;
  this.last_fid = structs_pop.last_fid;
};

var getTType = function(byte){
  return TTYPES[byte & 0x0f];
}

TCompactProtocol.prototype.readFieldBegin = function() {
  assert.equal(this.state, FIELD_READ);
  var type = this.readUByte();
  if (type == Type.STOP) {
    return {fname: null, ftype: type, fid: 0};
  }
  var delta = type >> 4;
  var fid = null;
  if( delta === 0 ){
    fid = this.readI16();
  } else {
    fid = this.last_fid + delta;
  }
  this.last_fid = fid;
  type = type & 0x0f;
  if( type === CompactType.TRUE ){
    this.state = BOOL_READ;
    this.bool_value = true;
  } else if( type === CompactType.FALSE ){
    this.state = BOOL_READ;
    this.bool_value = false;
  } else {
    this.state = VALUE_READ;
  }
  return {fname: null, ftype: getTType(type), fid: fid};
};

TCompactProtocol.prototype.readFieldEnd = function() {
  if( (this.state != VALUE_READ) && (this.state != BOOL_READ) ){
    assert.ok(false);
  }
  this.state = FIELD_READ;
};

TCompactProtocol.prototype.readCollectionBegin = function(){
  if( (this.state != VALUE_READ) && (this.state != CONTAINER_READ) ){
    assert.ok(false);
  }
  var size_type = this.readUByte();
  var size = size_type >> 4;
  var type = this.getTType(size_type);
  if( size === 15){
    size = this.readSize();
  }
  this.containers.push(this.state);
  this.state = CONTAINER_READ;
  return {etype: type, size: size};
};
TCompactProtocol.prototype.readListBegin = function(){
  return this.readCollectionBegin();
}
TCompactProtocol.prototype.readSetBegin = function(){
  return this.readCollectionBegin();
}

TCompactProtocol.prototype.readMapBegin = function() {
  if( (this.state != VALUE_READ) && (this.state != CONTAINER_READ) ){
    assert.ok(false);
  }
  var size = this.readSize();
  var types = 0;
  if( size > 0 ){
    types = this.readUByte();
  }
  var vtype = this.getTType(types);
  var ktype = this.getTType(types >> 4);
  this.containers.append(this.state);
  this.state = CONTAINER_READ;
  return {ktype: ktype, vtype: vtype, size: size};
};

TCompactProtocol.prototype.readCollectionEnd = function() {
  assert.equal(this.state, CONTAINER_READ);
  this.state = this.containers.pop();
}
TCompactProtocol.prototype.readListEnd = function() {
  this.readCollectionEnd();
};
TCompactProtocol.prototype.readSetEnd = function() {
  this.readCollectionEnd();
};
TCompactProtocol.prototype.readMapEnd = function() {
  this.readCollectionEnd();
};

TCompactProtocol.prototype.readBool = function() {
  if( this.state === BOOL_READ ){
    return this.bool_value;
  } else if( this.state === CONTAINER_READ){
    var byte = this.readByte();
    if (byte === 0) {
      return false;
    }
    return true;
  } else {
    assert.ok(false,'Invalid State');
  }
};

TCompactProtocol.prototype.readUByte = function(){
  var buff = this.trans.read(1);
  return BinaryParser.toSmall(buff); 
}

TCompactProtocol.prototype.readByte = function() {
  var buff = this.trans.read(1);
  return BinaryParser.toByte(buff);
};

var readZigZag = function(buff){
  return(fromZigZag(this.readVarInt()));
}

TCompactProtocol.prototype.readI16 = function() {
  return readZigZag();
};

TCompactProtocol.prototype.readI32 = function() {
  return readZigZag();
};

TCompactProtocol.prototype.readI64 = function() {
  return readZigZag();
};

TCompactProtocol.prototype.readDouble = function() {
  var buff = this.trans.read(8);
  return BinaryParser.toFloat(buff);
};

TCompactProtocol.prototype.readSize = function() {
  // XXX throw error for 0 size.
  return this.readVarint();
};

TCompactProtocol.prototype.readString = function() {
  var sz = this.readSize();
  //return this.trans.read(sz);
  var r = [];
  for( var ii = 0; ii < sz; ii++){
    r[ii] = this.trans.read(1);  
  }
  console.log("readString: " + r);
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

