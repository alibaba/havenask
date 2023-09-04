/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-02-24 15:01
 * Author Name: xsank.mz
 * Author Email: xsank.mz@alibaba-inc.com
 */

#include "kmonitor/client/net/AgentClient.h"

#include <iostream>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "kmonitor/client/common/Common.h"
#include "kmonitor/client/common/Util.h"
#include "kmonitor/client/net/thrift/TCompactProtocol.h"
#include "kmonitor/client/net/thrift/TFastFramedTransport.h"
#include "kmonitor/client/net/thrift/TSocket.h"
#include "kmonitor/client/net/thrift/ThriftFlumeEvent.h"
#include "kmonitor/client/net/thrift/ThriftProtocolClient.h"

BEGIN_KMONITOR_NAMESPACE(kmonitor);
AUTIL_LOG_SETUP(kmonitor, AgentClient);

using std::iostream;
using std::string;
using std::vector;

#ifndef DELETE_AND_SET_NULL
#define DELETE_AND_SET_NULL(pointer)                                                                                   \
    do {                                                                                                               \
        delete pointer;                                                                                                \
        pointer = NULL;                                                                                                \
    } while (0)
#endif

AgentClient::AgentClient(const string &address, int32_t timeOutMs) {
    socket_ = nullptr;
    transport_ = nullptr;
    protocol_ = nullptr;
    thrift_client_ = nullptr;
    address_ = address;
    timeOutMs_ = timeOutMs;
    started_ = false;
}

AgentClient::~AgentClient() {
    Close();
    DELETE_AND_SET_NULL(thrift_client_);
    DELETE_AND_SET_NULL(protocol_);
    DELETE_AND_SET_NULL(transport_);
    DELETE_AND_SET_NULL(socket_);
}

bool AgentClient::Init() {
    vector<string> hostPort = autil::StringUtil::split(address_, ":");
    if (hostPort.size() != 2) {
        AUTIL_LOG(ERROR, "kmon agent init(split) fail, address is [%s]", address_.c_str());
        return false;
    }

    if (!Util::HostnameToIp(hostPort[0], host_)) {
        AUTIL_LOG(ERROR, "kmon agent init(HostnameToIp) fail, address is [%s]", address_.c_str());
        return false;
    }

    if (!autil::StringUtil::fromString(hostPort[1].c_str(), port_)) {
        AUTIL_LOG(ERROR, "kmon agent init(fromString) fail, address is [%s]", address_.c_str());
        return false;
    }

    AUTIL_LOG(INFO,
              "kmon agent init success, address is [%s], ip is [%s], port is [%d]",
              address_.c_str(),
              host_.c_str(),
              port_);
    socket_ = new TSocket(host_, port_, timeOutMs_);
    transport_ = new TFastFramedTransport(socket_);
    protocol_ = new TCompactProtocol(transport_);
    thrift_client_ = new ThriftProtocolClient(protocol_);
    return true;
}

bool AgentClient::Started() const { return started_; }

void AgentClient::Close() { started_ = false; }

bool AgentClient::AppendBatch(const BatchFlumeEventPtr &events) {
    if (!Started()) {
        ReConnect();
    }
    if (thrift_client_ == nullptr) {
        AUTIL_LOG(ERROR, "thrift_client is nullptr, host[%s] port[%d]", host_.c_str(), port_);
        return false;
    }
    Status::type ret = thrift_client_->AppendBatch(*(events->Get()));
    if (ret != Status::OK) {
        AUTIL_LOG(WARN, "agent client send fail, host[%s] port[%d]", host_.c_str(), port_);
        return false;
    }
    return true;
}

void AgentClient::CheckSocket() {}

bool AgentClient::ReConnect() {
    Close();
    if (socket_ != nullptr && socket_->ReConnect()) {
        started_ = true;
        AUTIL_LOG(INFO, "agent client reconnect OK, host[%s] port[%d]", host_.c_str(), port_);
        return true;
    } else {
        AUTIL_LOG(WARN, "agent client failed, host[%s] port[%d]", host_.c_str(), port_);
        return false;
    }
}

END_KMONITOR_NAMESPACE(kmonitor);
