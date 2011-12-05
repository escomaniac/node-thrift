/*
*
*  Testing makeZigZag and read/writeVarint
*
*/




var tcompact = require('../lib/thrift/protocol/protocol_compact').TCompactProtocol;

var cp = new tcompact();

cp.writeVarint(300);


