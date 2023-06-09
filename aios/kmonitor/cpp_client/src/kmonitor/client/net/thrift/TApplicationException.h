/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-06-13 18:01
 * Author Name: beifei
 * Author Email: beifei@taobao.com
 */

#ifndef KMONITOR_CLIENT_NET_THRIFT_TAPPLICATIONEXCEPTION_H_
#define KMONITOR_CLIENT_NET_THRIFT_TAPPLICATIONEXCEPTION_H_

#include <string>
#include "kmonitor/client/common/Common.h"
BEGIN_KMONITOR_NAMESPACE(kmonitor);

class TCompactProtocol;

class TApplicationException {
 public:
    enum TApplicationExceptionType {
      UNKNOWN = 0,
      UNKNOWN_METHOD = 1,
      INVALID_MESSAGE_TYPE = 2,
      WRONG_METHOD_NAME = 3,
      BAD_SEQUENCE_ID = 4,
      MISSING_RESULT = 5,
      INTERNAL_ERROR = 6,
      PROTOCOL_ERROR = 7,
      INVALID_TRANSFORM = 8,
      INVALID_PROTOCOL = 9,
      UNSUPPORTED_CLIENT_TYPE = 10
    };

 public:
    TApplicationException();
    ~TApplicationException();

    uint32_t Read(TCompactProtocol* prot);
    const char* What() const;

 private:
    TApplicationException(const TApplicationException &);
    TApplicationException& operator=(const TApplicationException &);

 private:
    TApplicationExceptionType type_;
    std::string message_;
};

END_KMONITOR_NAMESPACE(kmonitor);

#endif  // KMONITOR_CLIENT_NET_THRIFT_TAPPLICATIONEXCEPTION_H_
