/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-02-27 16:43
 * Author Name: xsank.mz
 * Author Email: xsank.mz@alibaba-inc.com
 * */

#ifndef KMONITOR_CLIENT_NET_EVENT_EVENTBUILDER_H_
#define KMONITOR_CLIENT_NET_EVENT_EVENTBUILDER_H_

#include <map>
#include <string>
#include "kmonitor/client/common/Common.h"

BEGIN_KMONITOR_NAMESPACE(kmonitor);

class ThriftFlumeEvent;

class EventBuilder {
 public:
    EventBuilder();
    ~EventBuilder();
    static ThriftFlumeEvent *Build(const std::map<std::string, std::string>& header, const std::string& body);
    static ThriftFlumeEvent *Build(const std::string& body);     
 private:
    EventBuilder(const EventBuilder &);
    EventBuilder &operator=(const EventBuilder &);
};

END_KMONITOR_NAMESPACE(kmonitor);

#endif  // KMONITOR_CLIENT_NET_EVENT_EVENTBUILDER_H_
