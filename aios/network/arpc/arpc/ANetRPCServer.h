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
#ifndef ARPC_ANET_RPC_SERVER_H__
#define ARPC_ANET_RPC_SERVER_H__
#include <stddef.h>
#include <stdint.h>
#include <utility>
#include <unordered_map>
#include <string>

#include "aios/network/anet/anet.h"
#include "autil/LockFreeThreadPool.h"
#include "aios/network/arpc/arpc/CommonMacros.h"
#include "aios/network/arpc/arpc/RPCServer.h"
#include "aios/network/arpc/arpc/anet/ANetApp.h"
#include "aios/autil/autil/Lock.h"

ARPC_BEGIN_NAMESPACE(arpc);

class ANetRPCServer : public RPCServer
{
public:
    /**
     * @param: transport instance for ANet.
     * If transport is NULL, ANetRPCServer will
     * create a private transport object.
     * Otherwise, ANetRPCServer will
     * share transport object with out world.
     */
    ANetRPCServer(anet::Transport *transport = NULL,
                  size_t  threadNum = 1,
                  size_t queueSize = 50);
    /**
     * If there is a private transport instance,
     * it will be free.
     */
    virtual ~ANetRPCServer();

public:
    virtual bool Listen(const std::string &address,
                        int timeout = 5000,
                        int maxIdleTime = MAX_IDLE_TIME,
                        int backlog = LISTEN_BACKLOG) override;
    virtual bool Close() override;
    virtual void dump(std::ostringstream &out) override;
    virtual int GetListenPort() override;

public:
    bool OwnTransport() {
        return _anetApp.OwnTransport();
    }

    /**
     * Make the private transport spin. IO thread and time thread will be activated.
     * @return bool, true if the transport is started successfully.
     */
    bool StartPrivateTransport() {
        return _anetApp.StartPrivateTransport();
    }

    /**
     * Stop the private transport and destroy all the threads created.
     * This function must be called if StartPrivateTransport() is called.
     * @return true if the transport is stopped successfully.
     */
    bool StopPrivateTransport() {
        return _anetApp.StopPrivateTransport();
    }



public:
    friend class ANetRPCServerTest;

private:
    ANetApp _anetApp;
    std::list<anet::IOComponent *> _ioComponentList;
    autil::ThreadMutex _ioComponentMutex;
};

ARPC_END_NAMESPACE(arpc);

#endif
