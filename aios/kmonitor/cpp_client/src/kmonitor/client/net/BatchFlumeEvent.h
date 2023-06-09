/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-02-24 15:01
 * Author Name: xsank.mz
 * Author Email: xsank.mz@alibaba-inc.com
 */

#ifndef KMONITOR_CLIENT_NET_BATCH_FLUME_EVENT_H_
#define KMONITOR_CLIENT_NET_BATCH_FLUME_EVENT_H_

#include <vector>
#include <string>
#include "kmonitor/client/common/Common.h"
#include "autil/Log.h"
#include "kmonitor/client/net/thrift/ThriftFlumeEvent.h"

BEGIN_KMONITOR_NAMESPACE(kmonitor);

class ThriftFlumeEvent;

class BatchFlumeEvent {
 public:
    BatchFlumeEvent() {
       events = new std::vector<ThriftFlumeEvent*>;
    }
    ~BatchFlumeEvent() {
      // clean thrift event
      for (auto event : *events) {
         delete event;
      }
      delete events;
    }

    bool IsEmpty() {
      return events->empty();
    }

    size_t Size() {
       return events->size();
    }

    void AppendFlumeEvent(ThriftFlumeEvent* event) {
      events->push_back(event);
    }

    std::vector<ThriftFlumeEvent*>* Get() {
       return events;
    }

 private:
    std::vector<ThriftFlumeEvent*>* events;
 private:
    AUTIL_LOG_DECLARE();
};

TYPEDEF_PTR(BatchFlumeEvent)

END_KMONITOR_NAMESPACE(kmonitor);

#endif  // KMONITOR_CLIENT_NET_BASEAGENTCLIENT_H_
