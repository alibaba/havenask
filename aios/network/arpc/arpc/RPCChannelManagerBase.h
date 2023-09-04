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
#ifndef ARPC_RPCCHANNELMANAGERBASE_H
#define ARPC_RPCCHANNELMANAGERBASE_H
#include <assert.h>
#include <stddef.h>
#include <string>

#include "aios/network/anet/connectionpriority.h"
#include "aios/network/arpc/arpc/CommonMacros.h"
#include "aios/network/arpc/arpc/RPCInterface.h"
#include "aios/network/arpc/arpc/util/Log.h"

ARPC_BEGIN_NAMESPACE(arpc);

/**
 * @ingroup ClientClasses
 * Base class for ARPCChannelManager.
 * Implemented useful functions.
 */
class RPCChannelManagerBase : public RPCChannelManager {
public:
    /**
     * Constructor
     */
    RPCChannelManagerBase();
    virtual ~RPCChannelManagerBase();

public:
    /**
     * Empty virtual base function, implementation is in ARPCChannelManager.
     */
    virtual RPCChannel *OpenChannel(const std::string &address,
                                    bool block = false,
                                    size_t queueSize = 50ul,
                                    int timeout = 5000,
                                    bool autoReconn = true,
                                    anet::CONNPRIORITY prio = anet::ANET_PRIORITY_NORMAL) override {
        assert(false);
        return NULL;
    }

    /**
     * No use for now.
     */
    virtual void Close();
};

TYPEDEF_PTR(RPCChannelManagerBase);
ARPC_END_NAMESPACE(arpc);

#endif // ARPC_RPCCHANNELMANAGERBASE_H
