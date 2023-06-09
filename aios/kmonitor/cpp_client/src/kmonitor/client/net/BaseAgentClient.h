/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-02-24 15:01
 * Author Name: xsank.mz
 * Author Email: xsank.mz@alibaba-inc.com
 */

#ifndef KMONITOR_CLIENT_NET_BASEAGENTCLIENT_H_
#define KMONITOR_CLIENT_NET_BASEAGENTCLIENT_H_

#include <vector>
#include <string>
#include "kmonitor/client/common/Common.h"
#include "autil/Log.h"
#include "kmonitor/client/net/BatchFlumeEvent.h"

BEGIN_KMONITOR_NAMESPACE(kmonitor);

class BaseAgentClient {
 public:
    virtual ~BaseAgentClient() {}

    virtual bool AppendBatch(const BatchFlumeEventPtr &events) = 0;
    virtual bool Started() const = 0;
    virtual bool Init() = 0;
    virtual void Close() = 0;
    virtual bool ReConnect() = 0;

 private:
    AUTIL_LOG_DECLARE();
};

END_KMONITOR_NAMESPACE(kmonitor);

#endif  // KMONITOR_CLIENT_NET_BASEAGENTCLIENT_H_
