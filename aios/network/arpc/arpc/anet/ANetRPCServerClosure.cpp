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
#include "aios/network/arpc/arpc/anet/ANetRPCServerClosure.h"
#include "aios/network/arpc/arpc/UtilFun.h"

using namespace anet;

ARPC_BEGIN_NAMESPACE(arpc)

ARPC_DECLARE_AND_SETUP_LOGGER(ANetRPCServerClosure);

ANetRPCServerClosure::ANetRPCServerClosure(channelid_t chid,
                                   Connection *connection,
                                   RPCMessage *reqMsg,
                                   RPCMessage *resMsg,
                                   ANetRPCMessageCodec *messageCodec,
                                   version_t version)
        : RPCServerClosure(reqMsg, resMsg)
        , _chid(chid)
        , _connection(connection)
{
    if (_connection != NULL) {
        _connection->addRef();
    }
    _messageCodec = messageCodec;
    _version = version;
}

ANetRPCServerClosure::~ANetRPCServerClosure()
{
    if (_connection != NULL) {
        _connection->subRef();
    }
}

void ANetRPCServerClosure::doPostPacket() {
    Packet *packet = buildResponsePacket();
    if (packet == NULL) {
        return;
    }

    assert(_connection);
    if (!_connection->postPacket(packet)) {
        ARPC_LOG(ERROR, "post packet error, when running ANetRPCServerClosure");
        delete packet;
    }
}

std::string ANetRPCServerClosure::getIpAndPortAddr() {
    if (!_connection) {
        return nullptr;
    }
    char addr[128] = {0};
    _connection->getIpAndPortAddr(addr, 128);
    return std::string(addr);
}

Packet *ANetRPCServerClosure::buildResponsePacket()
{
    uint32_t pcode = ARPC_ERROR_NONE;
    Packet *packet = NULL;

    if (_controller.Failed()) {
        ErrorCode err =  _controller.GetErrorCode();

        /* We tell the difference of app error and ARPC error from the error code.
         * ARPC_ERROR_NONE means user does not set the error code while setting
         * failed. */
        if (err > ARPC_ERROR_APP_MIN || err == ARPC_ERROR_NONE) {
            pcode = ARPC_ERROR_APPLICATION;
        } else {
            pcode = err;
        }

        _resMsg = BuildErrorMsg(_controller.ErrorText(), err, _arena);

        if (_resMsg == NULL) {
            ARPC_LOG(ERROR, "build error msg return NULL");
            return NULL;
        }
    }

    if (_messageCodec == NULL) {
        ARPC_LOG(ERROR, "_messageCodec is NULL, it is abnormal in none-UT cases. ");
    } else {
        packet = _messageCodec->EncodeResponse(_resMsg, _tracer, _version, _arena);

        if (packet == NULL) {
            ARPC_LOG(ERROR, "build packet return NULL");
            return NULL;
        }

        packet->setChannelId(_chid);
        pcode |= packet->getPcode();
        packet->setPcode(pcode);
    }
    return packet;
}

ARPC_END_NAMESPACE(arpc)