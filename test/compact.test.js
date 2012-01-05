/*
*
*  Testing makeZigZag and read/writeVarint
*
*/

var tcompact = require('../lib/thrift/protocol/protocol_compact').TCompactProtocol;

var cp = new tcompact();

cp.writeVarint(1);   // 1 
cp.writeVarint(300); // output is 44034d 0xAC02



