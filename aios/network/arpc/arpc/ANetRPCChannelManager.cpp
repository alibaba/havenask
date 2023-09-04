/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "aios/network/arpc/arpc/ANetRPCChannelManager.h"

#include <cstddef>
#include <string>

#include "aios/network/anet/connectionpriority.h"
#include "aios/network/anet/ilogger.h"
#include "aios/network/anet/runnable.h"
#include "aios/network/arpc/arpc/ANetRPCChannel.h"
#include "aios/network/arpc/arpc/CommonMacros.h"
#include "aios/network/arpc/arpc/RPCChannelManagerBase.h"
#include "aios/network/arpc/arpc/anet/ANetRPCMessageCodec.h"
#include "aios/network/arpc/arpc/util/Log.h"

using namespace std;
using namespace anet;
ARPC_BEGIN_NAMESPACE(arpc)
ARPC_DECLARE_AND_SETUP_LOGGER(ANetRPCChannelManager);

ANetRPCChannelManager::ANetRPCChannelManager(Transport *transport) : _anetApp(transport) {}

ANetRPCChannelManager::~ANetRPCChannelManager() {}

RPCChannel *ANetRPCChannelManager::OpenChannel(
    const std::string &address, bool block, size_t queueSize, int timeout, bool autoReconn, anet::CONNPRIORITY prio) {
    Connection *pConn = Connect(address, block, queueSize, timeout, autoReconn, prio);

    if (pConn == NULL) {
        ARPC_LOG(ERROR, "connection is NULL, address: [%s]", address.c_str());
        return NULL;
    }

    ANetRPCMessageCodec *messageCodec = new ANetRPCMessageCodec(_anetApp.GetPacketFactory());
    ANetRPCChannel *pChannel = new ANetRPCChannel(pConn, messageCodec, block);
    pChannel->SetMetricReporter(_metricReporter);
    return pChannel;
}

Connection *ANetRPCChannelManager::Connect(
    const string &address, bool block, size_t queueSize, int timeout, bool autoReconn, anet::CONNPRIORITY prio) {
    return Connect(address, string(""), queueSize, timeout, autoReconn, prio);
}

Connection *ANetRPCChannelManager::Connect(const string &address,
                                           const string &bindAddr,
                                           size_t queueSize,
                                           int timeout,
                                           bool autoReconn,
                                           anet::CONNPRIORITY prio) {
    Connection *pConn = NULL;

    if (!bindAddr.empty())
        pConn = _anetApp.Connect(address, bindAddr, autoReconn, prio);
    else
        pConn = _anetApp.Connect(address, autoReconn, prio);

    if (pConn == NULL) {
        ARPC_LOG(ERROR, "connect to %s with bind address %s failed", address.c_str(), bindAddr.c_str());
        return NULL;
    }

    pConn->setQueueLimit(queueSize);
    pConn->setQueueTimeout(timeout);
    return pConn;
}

bool ANetRPCChannelManager::StartPrivateTransport() { return _anetApp.StartPrivateTransport(); }

bool ANetRPCChannelManager::StartPrivateTransport(const std::string &name) {
    return _anetApp.StartPrivateTransport(name);
}

bool ANetRPCChannelManager::StopPrivateTransport() { return _anetApp.StopPrivateTransport(); }

anet::Transport *ANetRPCChannelManager::GetTransport() { return _anetApp.GetTransport(); }

void ANetRPCChannelManager::SetMetricReporter(const std::shared_ptr<ClientMetricReporter> &metricReporter) {
    _metricReporter = metricReporter;
}

ARPC_END_NAMESPACE(arpc)
