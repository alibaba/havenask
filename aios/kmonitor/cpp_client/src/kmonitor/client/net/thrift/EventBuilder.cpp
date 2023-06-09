/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-02-27 16:43
 * Author Name: xsank.mz
 * Author Email: xsank.mz@alibaba-inc.com
 * */

#include <map>
#include <string>
#include "kmonitor/client/net/thrift/ThriftFlumeEvent.h"
#include "kmonitor/client/net/thrift/EventBuilder.h"

BEGIN_KMONITOR_NAMESPACE(kmonitor);

using std::string;
using std::map;

EventBuilder::EventBuilder() {
}


EventBuilder::~EventBuilder() {
}

ThriftFlumeEvent* EventBuilder::Build(const map<string, string>& headers, const string& body) {
    ThriftFlumeEvent *event = new ThriftFlumeEvent();
    event->SetHeader(headers);
    event->SetBody(body);
    return event;
}

ThriftFlumeEvent* EventBuilder::Build(const string& body) {
    ThriftFlumeEvent *event = new ThriftFlumeEvent();
    event->SetBody(body);
    return event;
}

END_KMONITOR_NAMESPACE(kmonitor);

