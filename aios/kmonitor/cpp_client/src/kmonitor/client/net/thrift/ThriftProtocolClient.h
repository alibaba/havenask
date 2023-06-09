/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-05-11 15:24
 * Author Name: beifei
 * Author Email: beifei@taobao.com
 */

#ifndef KMONITOR_CLIENT_NET_THRIFT_THRIFTPROTOCOLCLIENT_H_
#define KMONITOR_CLIENT_NET_THRIFT_THRIFTPROTOCOLCLIENT_H_

#include <vector>
#include "autil/Log.h"
#include "kmonitor/client/common/Common.h"

BEGIN_KMONITOR_NAMESPACE(kmonitor);

class TCompactProtocol;
class ThriftFlumeEvent;

struct Status {
  enum type {
    OK = 0,
    FAILED = 1,
    ERROR = 2,
    UNKNOWN = 3
  };
};

class ThriftProtocolClient {
 public:
    ThriftProtocolClient(TCompactProtocol *compact_protocol, uint32_t perBatchMaxEvents = 1000);
    ~ThriftProtocolClient();

    // Status::type Append(const ThriftFlumeEvent *event);
    Status::type AppendBatch(const std::vector<ThriftFlumeEvent*>& events);

 private:
    ThriftProtocolClient(const ThriftProtocolClient &);
    ThriftProtocolClient& operator=(const ThriftProtocolClient &);

 private:
    void SendBatch(const std::vector<ThriftFlumeEvent*> & events);
    Status::type RecvBatch();

 private:
    TCompactProtocol *compact_protocol_;
    uint32_t perBatchMaxEvents_;

 private:
    AUTIL_LOG_DECLARE();
};

END_KMONITOR_NAMESPACE(kmonitor);

#endif  // KMONITOR_CLIENT_NET_THRIFT_THRIFTPROTOCOLCLIENT_H_
