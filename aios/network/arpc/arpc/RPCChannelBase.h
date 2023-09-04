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
#ifndef ARPC_RPCCHANNELBASE_H
#define ARPC_RPCCHANNELBASE_H
#include <assert.h>
#include <stdint.h>
#include <string>
#include <unistd.h>

#include "aios/network/anet/connectionpriority.h"
#include "aios/network/arpc/arpc/ANetRPCController.h"
#include "aios/network/arpc/arpc/CommonMacros.h"
#include "aios/network/arpc/arpc/MessageCodec.h"
#include "aios/network/arpc/arpc/proto/rpc_extensions.pb.h"
#include "aios/network/arpc/arpc/util/Log.h"

ARPC_BEGIN_NAMESPACE(arpc);

/**
 * @addtogroup ClientClasses RPC Client Classes
 */

class RPCChannelBase : public RPCChannel {
public:
    RPCChannelBase();
    virtual ~RPCChannelBase();

public:
    virtual void CallMethod(const RPCMethodDescriptor *method,
                            RPCController *controller,
                            const RPCMessage *request,
                            RPCMessage *response,
                            RPCClosure *done) {
        assert(false);
    }

    /**
     * Check if the underlying socket connection is broken(closed) for
     * some reason.
     * @return Returns true if the connection is broken.
     */
    virtual bool ChannelBroken() = 0;

    /**
     * Check if the underlying socket connection is established.
     * @return Returns true if the connection is already
     * established.
     */
    virtual bool ChannelConnected() = 0;

    version_t GetVersion() { return _version; };
    void SetVersion(version_t version) { _version = version; };

    void SetTraceFlag(bool flag) { _enableTrace = flag; }
    bool GetTraceFlag() const { return _enableTrace; }

    void SetTraceFlag(ANetRPCController *controller);

protected:
    void RunClosure(RPCClosure *pClosure);
    void SetError(ANetRPCController *pController, ErrorCode errorCode);

private:
    friend class RPCChannelBaseTest;
    friend class RPCServerAdapterTest;
    friend class ANetRPCChannelCloseBugTest;

protected:
    version_t _version;
    bool _enableTrace;
};

ARPC_END_NAMESPACE(arpc);

#endif // ARPC_RPCCHANNELBASE_H
