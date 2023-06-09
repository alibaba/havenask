/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-04-27 10:12
 * Author Name: beifei
 * Author Email: beifei@taobao.com
 */

#ifndef KMONITOR_CLIENT_NET_THRIFT_THRIFTTYPE_H_
#define KMONITOR_CLIENT_NET_THRIFT_THRIFTTYPE_H_

#include "kmonitor/client/common/Common.h"
BEGIN_KMONITOR_NAMESPACE(kmonitor);

enum PROTOCOL_TYPES {
  T_BINARY_PROTOCOL = 0,
  T_JSON_PROTOCOL = 1,
  T_COMPACT_PROTOCOL = 2,
};

/**
 * Enumerated definition of the message types that the Thrift protocol
 * supports.
 */
enum TMessageType {
  T_CALL       = 1,
  T_REPLY      = 2,
  T_EXCEPTION  = 3,
  T_ONEWAY     = 4
};

/**
 * Enumerated definition of the types that the Thrift protocol supports.
 * Take special note of the T_END type which is used specifically to mark
 * the end of a sequence of fields.
 */
enum TType {
  T_STOP       = 0,
  T_VOID       = 1,
  T_BOOL       = 2,
  T_BYTE       = 3,
  T_I08        = 3,
  T_I16        = 6,
  T_I32        = 8,
  T_U64        = 9,
  T_I64        = 10,
  T_DOUBLE     = 4,
  T_STRING     = 11,
  T_UTF7       = 11,
  T_STRUCT     = 12,
  T_MAP        = 13,
  T_SET        = 14,
  T_LIST       = 15,
  T_UTF8       = 16,
  T_UTF16      = 17,
  T_UNKNOWN    = 18
};

enum Types {
  CT_STOP           = 0x00,
  CT_BOOLEAN_TRUE   = 0x01,
  CT_BOOLEAN_FALSE  = 0x02,
  CT_BYTE           = 0x03,
  CT_I16            = 0x04,
  CT_I32            = 0x05,
  CT_I64            = 0x06,
  CT_DOUBLE         = 0x07,
  CT_BINARY         = 0x08,
  CT_LIST           = 0x09,
  CT_SET            = 0x0A,
  CT_MAP            = 0x0B,
  CT_STRUCT         = 0x0C
};


const int8_t TTypeToCType[16] = {
  CT_STOP, // T_STOP
  0, // unused
  CT_BOOLEAN_TRUE, // T_BOOL
  CT_BYTE, // T_BYTE
  CT_DOUBLE, // T_DOUBLE
  0, // unused
  CT_I16, // T_I16
  0, // unused
  CT_I32, // T_I32
  0, // unused
  CT_I64, // T_I64
  CT_BINARY, // T_STRING
  CT_STRUCT, // T_STRUCT
  CT_MAP, // T_MAP
  CT_SET, // T_SET
  CT_LIST, // T_LIST
};

template <typename To, typename From>
static inline To bitwise_cast(From from) {
  // Technically undefined, but almost universally supported,
  // and the most efficient implementation.
  union {
    From f;
    To t;
  } u;
  u.f = from;
  return u.t;
}



END_KMONITOR_NAMESPACE(kmonitor);

#endif  // KMONITOR_CLIENT_NET_THRIFT_THRIFTTYPE_H_
