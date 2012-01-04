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

var BinaryParser = new require('../utils/binary_parser').BinaryParser;
BinaryParser.bigEndian = true;

var TCompactProtocol = exports.TCompactProtocol = function(trans) {
  this.trans = trans;
  this.last_fid = 0;
  this.bool_fid = null;
  this.bool_value = null;
  this.structs = [];
  this.containers = [];
  this.setState(CLEAR);
};

TCompactProtocol.prototype.flush = function() {
  return this.trans.flush();
};

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

var STATES = [];
STATES[CLEAR] = 'CLEAR';
STATES[FIELD_WRITE] = 'FIELD_WRITE';
STATES[VALUE_WRITE] = 'VALUE_WRITE';
STATES[CONTAINER_WRITE] = 'CONTAINER_WRITE';
STATES[BOOL_WRITE] = 'BOOL_WRITE';
STATES[FIELD_READ] = 'FIELD_READ';
STATES[CONTAINER_READ] = 'CONTAINER_READ';
STATES[VALUE_READ] = 'VALUE_READ';
STATES[BOOL_READ] = 'BOOL_READ';

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
CTYPES[Type.BOOL]   = CompactType.TRUE;
CTYPES[Type.BYTE]   = CompactType.BYTE;
CTYPES[Type.I16]    = CompactType.I16;
CTYPES[Type.I32]    = CompactType.I32;
CTYPES[Type.I64]    = CompactType.I64;
CTYPES[Type.DOUBLE] = CompactType.DOUBLE;
CTYPES[Type.STRING] = CompactType.BINARY;
CTYPES[Type.STRUCT] = CompactType.STRUCT;
CTYPES[Type.LIST]   = CompactType.LIST;
CTYPES[Type.SET]    = CompactType.SET;
CTYPES[Type.MAP]    = CompactType.MAP;

var TTYPES = [];
TTYPES[CompactType.TRUE]    = Type.BOOL;
TTYPES[CompactType.BYTE]    = Type.BYTE;
TTYPES[CompactType.I16]     = Type.I16;
TTYPES[CompactType.I32]     = Type.I32;
TTYPES[CompactType.I64]     = Type.I64;
TTYPES[CompactType.DOUBLE]  = Type.DOUBLE;
TTYPES[CompactType.BINARY]  = Type.STRING;
TTYPES[CompactType.STRUCT]  = Type.STRUCT;
TTYPES[CompactType.LIST]    = Type.LIST;
TTYPES[CompactType.SET]     = Type.SET;
TTYPES[CompactType.MAP]     = Type.MAP;
TTYPES[CompactType.FALSE]   = Type.BOOL;

var TTNAME = [];
TTNAME[Type.BOOL] = 'BOOL';
TTNAME[Type.BYTE] = 'BYTE';
TTNAME[Type.I16] = 'I16';
TTNAME[Type.I32] = 'I32';
TTNAME[Type.I64] = 'I64';
TTNAME[Type.DOUBLE] = 'DOUBLE';
TTNAME[Type.STRING] = 'STRING';
TTNAME[Type.STRUCT] = 'STRUCT';
TTNAME[Type.LIST] = 'LIST';
TTNAME[Type.SET] = 'SET';
TTNAME[Type.MAP] = 'MAP';

var PROTOCOL_ID = 0x82,
    VERSION_1 = 0x01,
    VERSION_MASK = 0x1f,
    TYPE_MASK = 0xe0,
    TYPE_SHIFT_AMOUNT = 5;

TCompactProtocol.prototype.makeZigZag = function(n, bits){
 return Math.pow( (n << 1) , (n >> (bits - 1)));
};

TCompactProtocol.prototype.fromZigZag = function(n){
  return (n>>1)-(n&1)*n;  
};

TCompactProtocol.prototype.writeVarint = function(n){
  var out = []                                                                                                                                                       
  while(true){ 
    if ( (n & 0x80) === 0){
      out.push(n);
      break;
    } else {
      out.push((n & 0xff) | 0x80)
      n = n >> 7;
    } 
  }
  var nbytes = out.length;
  for( var ii = 0; ii < nbytes; ii++){
    this.writeUByte(out[ii]);
  }

};

// from http://code.google.com/p/protobuf-js/
TCompactProtocol.prototype.readVarint = function(){
    var ret = 0; 
    for (var i = 0; ; i++) {
      if( i > 7){
        this.readUByte();
        sys.debug('WARNING: 64-bit varint encountered');
        continue;
      }
      var input = this.readUByte();
      var byte = input; //.charCodeAt(0);
      ret |= (byte & 0x7F) << (7 * i);
      if ((byte >> 7) == 0){
         break;
      }
    } 
    return ret; 
};

TCompactProtocol.prototype.writeMessageBegin = function(name, type, seqid) {
  // removed strict read/write --jlk
  assert.equal(this.state, CLEAR); 
  this.writeByte(PROTOCOL_ID);
  var typeByte =  (VERSION_1 & VERSION_MASK) | ((type << TYPE_SHIFT_AMOUNT) & TYPE_MASK);
  this.writeByte(typeByte);
  this.writeVarint(seqid);
  this.writeString(name);
  this.setState(VALUE_WRITE);
  //this.state = VALUE_WRITE;
};

TCompactProtocol.prototype.writeMessageEnd = function() {
  assert.equal(this.state, VALUE_WRITE);
  this.setState(CLEAR);
  //this.state = CLEAR;
};

TCompactProtocol.prototype.writeStructBegin = function(name) {
  if( (this.state !== CLEAR) && (this.state !== CONTAINER_WRITE) && (this.state !== VALUE_WRITE) ){
    assert.ok(false, 'Invalid State: ' + this.state + ' Expected Either: ' + CLEAR + ' ' + CONTAINER_WRITE + ' ' + VALUE_WRITE);
  }
  this.structs.push({state:this.state, last_fid:this.last_fid});
  this.setState(FIELD_WRITE);
  //this.state = FIELD_WRITE;
  this.last_fid = 0;
};

TCompactProtocol.prototype.writeStructEnd = function() {
  assert.equal(this.state, FIELD_WRITE);
  var structs_pop = this.structs.pop();
  this.last_fid = structs_pop.last_fid;
  this.setState(structs_pop.state);
  //this.state = structs_pop.state;  
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
    this.setState(BOOL_WRITE);
    //this.state = BOOL_WRITE;
    this.bool_fid = fid;
  } else {
    this.setState(VALUE_WRITE);
    //this.state = VALUE_WRITE;
    this.writeFieldHeader(CTYPES[type], fid);
  }
};

TCompactProtocol.prototype.writeFieldEnd = function() {
  if( (this.state !== VALUE_WRITE) && (this.state !== BOOL_WRITE) ){
    assert.ok(false);
  }
  this.setState(FIELD_WRITE);
  //this.state = FIELD_WRITE;
};

// Set and List both are written as Collection
TCompactProtocol.prototype.writeCollectionBegin = function(etype, size) {
  if( (this.state !== VALUE_WRITE) && (this.state !== CONTAINER_WRITE) ){
    assert.ok(false);
  } 
  if( size <= 14 ){
    this.writeUByte( size << 4 | CTYPES[etype] );
  } else {
    this.writeUByte( 0xf0 | CTYPES[etype] )
    this.writeSize(size);
  }
  this.containers.push(this.state);
  this.setState(CONTAINER_WRITE);
  //this.state = CONTAINER_WRITE;
};
TCompactProtocol.prototype.writeSetBegin = function(etype, size){
  this.writeCollectionBegin(etype,size);
};
TCompactProtocol.prototype.writeListBegin = function(etype, size){
  this.writeCollectionBegin(etype,size);
};

TCompactProtocol.prototype.writeCollectionEnd = function() {
  assert.equal(this.state, CONTAINER_WRITE);
  var newState = this.containers.pop();
  this.setState(newState);
  //this.state = this.containers.pop();
};
TCompactProtocol.prototype.writeListEnd = function() {
  this.writeCollectionEnd();
};
TCompactProtocol.prototype.writeSetEnd = function() {
  this.writeCollectionEnd();
};

TCompactProtocol.prototype.writeMapBegin = function(ktype, vtype, size) {
  if( (this.state !== VALUE_WRITE) && (this.state !== CONTAINER_WRITE) ){
    assert.ok(false);
  } else {
    this.writeSize(size);
    this.writeUByte(CTYPES[ktype] << 4 | CTYPES[vtype]);
  }
  this.containers.push(this.state);
  this.setState(CONTAINER_WRITE);
  //this.state = CONTAINER_WRITE;
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
  this.writeVarint( i32 );
};

TCompactProtocol.prototype.writeI16 = function(i16) {
  this.writeVarint( this.makeZigZag(i16,16) );
};

TCompactProtocol.prototype.writeI32 = function(i32) {
  this.writeVarint( this.makeZigZag(i32,32) );
};

TCompactProtocol.prototype.writeI64 = function(i64){
  this.writeVarint(this.makeZigZag(i64,64));
};
TCompactProtocol.prototype.writeDouble = function(dub) {
  this.trans.write(BinaryParser.fromDouble(dub));
};

TCompactProtocol.prototype.writeString = function(str) {
  this.writeVarint(str.length);
  this.trans.write(str);
};

TCompactProtocol.prototype.readMessageBegin = function() {
  assert.equal(this.state, CLEAR);
  var proto_id = this.readByte();
  if( proto_id !== PROTOCOL_ID ){
    throw TProtocolException( BAD_VERSION, "Bad version in readMessageBegin: " + proto_id);
  }
  var ver_type = this.readUByte();
  var type = (ver_type & TYPE_MASK) >> TYPE_SHIFT_AMOUNT;
  var version = ver_type & VERSION_1;
  if( version !== VERSION_1 ){
    throw TProtocolException( BAD_VERSION, 'Bad version: ' + version + ' expected: ' + VERSION_1);
  }
  var seqid = this.readVarint();
  var name = this.readString();
  var ret ={fname: name, ftype: type, rseqid: seqid};
  return ret;
};

TCompactProtocol.prototype.readMessageEnd = function() {
  if( (this.state !== CLEAR) && (this.state !== VALUE_READ) ){
    assert.ok(false);
  } 
  assert.equal(this.structs.length, 0);
  this.setState(CLEAR);
};

TCompactProtocol.prototype.readStructBegin = function() {
  if( (this.state !== CLEAR) && (this.state !== CONTAINER_READ) && (this.state !== VALUE_READ) ){
    assert.ok(false);
  }
  this.structs.push({state:this.state, last_fid:this.last_fid});
  this.setState(FIELD_READ);
  //this.state = FIELD_READ;
  this.last_fid = 0;
};

TCompactProtocol.prototype.readStructEnd = function() {
  assert.equal(this.state, FIELD_READ);
  var structs_pop = this.structs.pop();
  this.setState(structs_pop.state);
  //this.state = structs_pop.state;
  this.last_fid = structs_pop.last_fid;
};

var getTType = function(inbyte){
  return TTYPES[inbyte & 0x0f];
}

TCompactProtocol.prototype.readFieldBegin = function() {
  assert.equal(this.state, FIELD_READ);
  var type = this.readUByte();
  if ( (type & 0x0f) == Type.STOP) {
    return {fname: null, ftype: Type.STOP, fid: 0};
  }
  var delta = (type >> 4) & 0x0f;
  var fid = null;
  if( delta === 0 ){
    fid = this.readI16();
  } else {
    fid = this.last_fid + delta;
  }
  this.last_fid = fid;
  type = type & 0x0f;
  if( type === CompactType.TRUE ){
    this.setState(BOOL_READ);
    //this.state = BOOL_READ;
    this.bool_value = true;
  } else if( type === CompactType.FALSE ){
    this.setState(BOOL_READ);
    //this.state = BOOL_READ;
    this.bool_value = false;
  } else {
    this.setState(VALUE_READ);
  }
  var ret = {fname: null, ftype: getTType(type), fid: fid};
  return ret;// {fname: null, ftype: getTType(type), fid: fid};
};

TCompactProtocol.prototype.readFieldEnd = function() {
  if( (this.state !== VALUE_READ) && (this.state !== BOOL_READ) ){
    assert.ok(false);
  }
  this.setState(FIELD_READ);
  //this.state = FIELD_READ;
};

TCompactProtocol.prototype.readCollectionBegin = function(){
  if( (this.state !== VALUE_READ) && (this.state !== CONTAINER_READ) ){
    assert.ok(false);
  }
  var size_type = this.readUByte();
  var size = (size_type >> 4) & 0x0f;
  var type = getTType(size_type);
  if( size === 15){
    size = this.readSize();
  }
  this.containers.push(this.state);
  this.setState(CONTAINER_READ);
  //this.state = CONTAINER_READ;
  return {etype: type, size: size};
};
TCompactProtocol.prototype.readListBegin = function(){
  var ret = this.readCollectionBegin();
  return ret;
}
TCompactProtocol.prototype.readSetBegin = function(){
  var ret = this.readCollectionBegin();
  return ret;
}

TCompactProtocol.prototype.readMapBegin = function() {
  if( (this.state !== VALUE_READ) && (this.state !== CONTAINER_READ) ){
    assert.ok(false);
  }
  var size = this.readSize();
  var types = 0;
  if( size > 0 ){
    types = this.readUByte();
  }
  var vtype = getTType(types & 0x0f);
  var ktype = getTType( (types >> 4) & 0x0f);
  this.containers.push(this.state);
  this.setState(CONTAINER_READ);
  //this.state = CONTAINER_READ;
  var ret ={ktype: ktype, vtype: vtype, size: size};
  return ret;
};

TCompactProtocol.prototype.readCollectionEnd = function() {
  assert.equal(this.state, CONTAINER_READ);
  var containers_pop = this.containers.pop();
  this.setState(containers_pop);
  //this.state = this.containers.pop();
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
  var ret = buff[0];
  //return BinaryParser.fromByte(buff);
  return ret;
}

TCompactProtocol.prototype.readByte = function() {
  var buff = this.trans.read(1);
  var ret = buff[0];
  return ret;  
  //return BinaryParser.fromSmall(buff);
};

TCompactProtocol.prototype.readZigZag = function(){
  return(this.fromZigZag(this.readVarint()));
}

TCompactProtocol.prototype.readI16 = function() {
  return this.readZigZag();
};

TCompactProtocol.prototype.readI32 = function() {
  return this.readZigZag();
};

TCompactProtocol.prototype.readI64 = function() {
  return this.readZigZag();
};

TCompactProtocol.prototype.readDouble = function() {
  var buff = this.trans.read(8);
  var revBuff = new Buffer(buff.length);
  revBuff[7] = buff[0];
  revBuff[6] = buff[1];
  revBuff[5] = buff[2];
  revBuff[4] = buff[3];
  revBuff[3] = buff[4];
  revBuff[2] = buff[5];
  revBuff[1] = buff[6];
  revBuff[0] = buff[7];
  return BinaryParser.toDouble(revBuff);
};

TCompactProtocol.prototype.readSize = function() {
  // XXX throw error for 0 size.
  var size = this.readVarint();
  if( size < 0 ){
    assert.ok(false, 'Read size < 0');
  }
  return size;
};

TCompactProtocol.prototype.readString = function() {
  var sz = this.readSize();
   var r = new Buffer(sz);
  for( var ii = 0; ii < sz; ii++){
    r[ii] = this.readUByte();
  }
  var ret = r.toString('utf8');
  return ret;
};

TCompactProtocol.prototype.getTransport = function() {
  return this.trans;
};  

TCompactProtocol.prototype.setState = function( newState ){
  this.state = newState;
};

TCompactProtocol.prototype.skip = function(type) {
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

