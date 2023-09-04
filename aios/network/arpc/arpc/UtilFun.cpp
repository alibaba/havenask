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
#include "aios/network/arpc/arpc/UtilFun.h"

#include <assert.h>
#include <cstddef>
#include <google/protobuf/arena.h>
#include <google/protobuf/descriptor.h>
#include <memory>
#include <new>
#include <stdint.h>
#include <string>

#include "aios/network/anet/delaydecodepacket.h"
#include "aios/network/anet/runnable.h"
#include "aios/network/arpc/arpc/ANetRPCController.h"
#include "aios/network/arpc/arpc/CommonMacros.h"
#include "aios/network/arpc/arpc/MessageSerializable.h"
#include "aios/network/arpc/arpc/RPCMessageSerializable.h"
#include "aios/network/arpc/arpc/proto/msg_header.pb.h"
#include "aios/network/arpc/arpc/proto/rpc_extensions.pb.h"
#include "aios/network/arpc/arpc/util/Log.h"

using namespace std;
using namespace anet;

ARPC_BEGIN_NAMESPACE(arpc);

ErrorMsg *
BuildErrorMsg(const string &errMsg, const ErrorCode errCode, const std::shared_ptr<google::protobuf::Arena> &arena) {
    assert(arena);
    ErrorMsg *pErrMsg = google::protobuf::Arena::CreateMessage<ErrorMsg>(arena.get());

    if (pErrMsg == NULL) {
        return NULL;
    }

    pErrMsg->set_error_msg(errMsg);
    pErrMsg->set_error_code(errCode);
    return pErrMsg;
}

// pSeri must be newed
DelayDecodePacket *BuildPacket(PacketHeader *header, MessageSerializable *pSeri, bool ownSeri) {
    DelayDecodePacket *resPacket = new (nothrow) DelayDecodePacket;

    if (resPacket == NULL) {
        return NULL;
    }

    resPacket->setContent(pSeri, ownSeri);
    resPacket->setPacketHeader(header);

    return resPacket;
}

void DecodePacket(ANetRPCController *pController,
                  DelayDecodePacket *pPacket,
                  RPCMessage *message,
                  const std::shared_ptr<google::protobuf::Arena> &arenaPtr,
                  bool bResponse) {
    DataBufferSerializable *pSeri = NULL;
    ErrorMsg *pErrMsg = NULL;
    int32_t pcode = pPacket->getPcode();
    version_t version = pPacket->getPacketVersion();
    RpcMsgHeader *msgHeader = NULL;

    auto arena = getArena(message, arenaPtr);
    assert(arena);
    if (bResponse && (pcode != ARPC_ERROR_NONE)) {
        pErrMsg = google::protobuf::Arena::CreateMessage<ErrorMsg>(arena);

        if (pErrMsg == NULL) {
            pController->SetErrorCode(ARPC_ERROR_NEW_NOTHROW);
            pController->SetFailed("new ErrorMsg return NULL when DecodePacket");
            pPacket->free();
            return;
        }

        pSeri = new (nothrow) MessageSerializable(pErrMsg, arenaPtr);

        if (pSeri == NULL) {
            pController->SetErrorCode(ARPC_ERROR_NEW_NOTHROW);
            pController->SetFailed("new MessageSerializable return NULL when DecodePacket");
            pPacket->free();
            return;
        }
    } else {
        if (version == ARPC_VERSION_0) {
            pSeri = new (nothrow) MessageSerializable(message, arenaPtr);
        } else if (version == ARPC_VERSION_1) {
            msgHeader = google::protobuf::Arena::CreateMessage<RpcMsgHeader>(arena);
            pSeri = new (nothrow) RPCMessageSerializable(msgHeader, message, arenaPtr);
        }

        if (pSeri == NULL) {
            pController->SetErrorCode(ARPC_ERROR_NEW_NOTHROW);
            pController->SetFailed("new MessageSerializable return NULL when DecodePacket");
            pPacket->free();
            return;
        }
    }

    pPacket->setContent(pSeri, true);

    bool ret = pPacket->decodeToContent();

    if (!ret) {
        pController->SetErrorCode(ARPC_ERROR_DECODE_PACKET);
        pController->SetFailed(errorCodeToString(ARPC_ERROR_DECODE_PACKET));
    } else if (bResponse && (pcode != ARPC_ERROR_NONE)) {
        pController->SetErrorCode((ErrorCode)pcode);
        pController->SetFailed(pErrMsg->error_msg());
    }

    pPacket->free();

    return;
}

ARPC_END_NAMESPACE(arpc);
