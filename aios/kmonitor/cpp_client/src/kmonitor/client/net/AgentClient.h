/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-02-24 15:01
 * Author Name: xsank.mz
 * Author Email: xsank.mz@alibaba-inc.com
 */

#ifndef KMONITOR_CLIENT_NET_AGENTCLIENT_H_
#define KMONITOR_CLIENT_NET_AGENTCLIENT_H_

#include <string>
#include <vector>

#include "autil/Log.h"
#include "kmonitor/client/common/Common.h"
#include "kmonitor/client/net/BaseAgentClient.h"
#include "kmonitor/client/net/BatchFlumeEvent.h"

BEGIN_KMONITOR_NAMESPACE(kmonitor);

class TSocket;
class TFastFramedTransport;
class TCompactProtocol;
class ThriftProtocolClient;
class ThriftFlumeEvent;

class AgentClient : public BaseAgentClient {
public:
    AgentClient(const std::string &host, int port, int32_t timeOutMs);
    AgentClient(const std::string &address, int32_t timeOutMs);
    ~AgentClient();

    bool AppendBatch(const BatchFlumeEventPtr &events) override;
    bool Started() const override;
    bool Init() override;
    void Close() override;
    bool ReConnect() override;

private:
    AgentClient(const AgentClient &);
    AgentClient &operator=(const AgentClient &);
    void CheckSocket();
    void init(const std::string &host, int port, int32_t timeOutMs);

private:
    std::string host_;
    int port_;
    std::string address_;
    int32_t timeOutMs_;
    TSocket *socket_;
    TFastFramedTransport *transport_;
    TCompactProtocol *protocol_;
    ThriftProtocolClient *thrift_client_;
    bool started_;

private:
    AUTIL_LOG_DECLARE();
};

END_KMONITOR_NAMESPACE(kmonitor);

#endif // KMONITOR_CLIENT_NET_AGENTCLIENT_H_
