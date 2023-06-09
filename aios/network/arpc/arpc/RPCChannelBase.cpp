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
#include <unistd.h>
#include <stdint.h>
#include <cstddef>

#include "aios/network/arpc/arpc/RPCChannelBase.h"
#include "aios/network/arpc/arpc/PacketArg.h"
#include "aios/network/anet/advancepacket.h"
#include "aios/network/anet/connection.h"
#include "aios/network/anet/connectionpriority.h"
#include "aios/network/anet/ilogger.h"
#include "aios/network/anet/ipackethandler.h"
#include "aios/network/anet/packet.h"
#include "aios/network/anet/runnable.h"
#include "aios/network/arpc/arpc/ANetRPCController.h"
#include "aios/network/arpc/arpc/CommonMacros.h"
#include "aios/network/arpc/arpc/MessageCodec.h"
#include "aios/network/arpc/arpc/anet/SharedClientPacketHandler.h"
#include "aios/network/arpc/arpc/Tracer.h"
#include "aios/network/arpc/arpc/proto/rpc_extensions.pb.h"
#include "aios/network/arpc/arpc/util/Log.h"

using namespace std;
using namespace anet;
ARPC_BEGIN_NAMESPACE(arpc);
ARPC_DECLARE_AND_SETUP_LOGGER(RPCChannelBase);

RPCChannelBase::RPCChannelBase()
{
    _version = ARPC_VERSION_CURRENT;
    _enableTrace = false;
}

RPCChannelBase::~RPCChannelBase()
{
}

void RPCChannelBase::SetError(ANetRPCController *pController,
                              ErrorCode errorCode)
{
    pController->SetErrorCode(errorCode);
    pController->SetFailed(errorCodeToString(errorCode));
    return;
}

void RPCChannelBase::RunClosure(RPCClosure *pClosure)
{
    if (pClosure != NULL) {
        pClosure->Run();
    }
}

void RPCChannelBase::SetTraceFlag(ANetRPCController *controller)
{
    if (GetTraceFlag() && controller) {
        controller->SetTraceFlag(true);
    }
}

ARPC_END_NAMESPACE(arpc);

