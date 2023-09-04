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
#include "aios/network/gig/multi_call/service/ProtocolCallBack.h"

#include "aios/network/anet/httppacket.h"
#include "aios/network/gig/multi_call/service/ArpcConnection.h"
#include "aios/network/gig/multi_call/service/HttpConnection.h"
#include "aios/network/gig/multi_call/service/SearchServiceResource.h"
#include "aios/network/gig/multi_call/service/TcpConnection.h"
#include "aios/network/gig/multi_call/util/ProtobufUtil.h"
#include "autil/StringUtil.h"
#include "autil/ThreadPool.h"
#include "autil/legacy/base64.h"

using namespace std;
using namespace autil;

namespace multi_call {

AUTIL_LOG_SETUP(multi_call, HttpCallBack);
AUTIL_LOG_SETUP(multi_call, TcpCallBack);
AUTIL_LOG_SETUP(multi_call, ArpcCallBack);
AUTIL_LOG_SETUP(multi_call, ArpcCallBackWorkItem);

anet::IPacketHandler::HPRetCode TcpCallBack::handlePacket(anet::Packet *packet, void *args) {
    assert(_callBack);
    CallBackWorkItem *item = new CallBackWorkItem(packet, _callBack);
    if (!_callBackThreadPool ||
        autil::ThreadPool::ERROR_NONE != _callBackThreadPool->pushWorkItem(item, false)) {
        item->process();
        item->destroy();
    }

    delete this;
    return anet::IPacketHandler::FREE_CHANNEL;
}

anet::IPacketHandler::HPRetCode HttpCallBack::handlePacket(anet::Packet *packet, void *args) {
    assert(_callBack);
    assert(_httpConnection);
    assert(_anetConnection);
    HttpCallBackWorkItem *item = new HttpCallBackWorkItem(packet, _callBack);
    auto httpPacket = dynamic_cast<anet::HTTPPacket *>(packet);
    bool keepAlive = true;
    if (httpPacket) {
        keepAlive = httpPacket->isKeepAlive();
    }
    if (!_callBackThreadPool ||
        autil::ThreadPool::ERROR_NONE != _callBackThreadPool->pushWorkItem(item, false)) {
        item->process();
        item->destroy();
    }

    if (keepAlive) {
        _httpConnection->recycleConnection(_anetConnection);
    } else {
        _httpConnection->closeConnection(_anetConnection);
    }
    delete this;
    return anet::IPacketHandler::FREE_CHANNEL;
}

void CallBackWorkItem::process() {
    if (!_callBack->isCopyRequest()) {
        _callBack->run(_packet, MULTI_CALL_ERROR_NONE, string(), string());
    } else {
        _packet->free();
    }
}

void CallBackWorkItem::destroy() {
    delete this;
}

void CallBackWorkItem::drop() {
    process();
    destroy();
}

void HttpCallBackWorkItem::process() {
    if (!_callBack->isCopyRequest()) {
        auto ec = MULTI_CALL_ERROR_NONE;
        std::string errorString;
        std::string responseInfo;
        auto httpPacket = dynamic_cast<anet::HTTPPacket *>(_packet);
        if (httpPacket) {
            const char *headData = httpPacket->getHeader(GIG_DATA.c_str());
            if (!headData) { // for compatible
                headData = httpPacket->getHeader(GIG_DATA_C.c_str());
            }
            if (headData) {
                std::string gigData(headData);
                responseInfo = autil::legacy::Base64DecodeFast(gigData);
            }
            errorString = httpPacket->getReasonPhrase();

            const auto &span = _callBack->getSpan();
            if (span) {
                span->setAttribute("gig.http.status_code",
                                   StringUtil::toString(httpPacket->getStatusCode()));
            }
        }
        _callBack->run(_packet, ec, errorString, responseInfo);
    } else {
        _packet->free();
    }
}

void ArpcCallBack::Run() {
    if (_callBackThreadPool) {
        _callBack->rpcEnd();
        if (autil::ThreadPool::ERROR_NONE == _callBackThreadPool->pushWorkItem(&_item, false)) {
            return;
        }
    }
    callBack();
    delete this;
}

void ArpcCallBack::callBack() {
    if (!_callBack->isCopyRequest()) {
        auto ec = MULTI_CALL_ERROR_NONE;
        auto arpcErrorCode = _controller.GetErrorCode();
        if (_controller.Failed()) {
            if (arpc::ARPC_ERROR_TIMEOUT == arpcErrorCode) {
                ec = MULTI_CALL_REPLY_ERROR_TIMEOUT;
            } else {
                ec = MULTI_CALL_REPLY_ERROR_RESPONSE;
            }
            AUTIL_LOG(WARN, "%s", _controller.ErrorText().c_str());
        }
        assert(_callBack);
        const auto &span = _callBack->getSpan();
        if (span) {
            span->setAttribute("gig.arpc.error_code", StringUtil::toString(arpcErrorCode));
        }
        std::string responseInfo;
        ec = getCompatibleInfo(ec, responseInfo);
        _callBack->run(_response, ec, _controller.ErrorText(), responseInfo);
    } else {
        freeProtoMessage(_response);
        _response = NULL;
    }
}

MultiCallErrorCode ArpcCallBack::getCompatibleInfo(MultiCallErrorCode currentEc,
                                                   std::string &responseInfo) {
    auto ec = currentEc;
    auto traceInfo = _controller.GetTraceInfo();
    if (traceInfo) {
        responseInfo = traceInfo->userpayload();
    }
    if (responseInfo.empty()) {
        const auto &resource = _callBack->getResource();
        const auto &flowControlStrategy = resource->getFlowControlConfig();
        if (flowControlStrategy) {
            if (MULTI_CALL_ERROR_NONE == currentEc) {
                const auto &ecField = flowControlStrategy->compatibleFieldInfo.ecField;
                ProtobufCompatibleUtil::getErrorCodeField(_response, ecField, ec);
            }
            const auto &responseInfoField =
                flowControlStrategy->compatibleFieldInfo.responseInfoField;
            ProtobufCompatibleUtil::getGigMetaField(_response, responseInfoField, responseInfo);
        }
    }
    return ec;
}

void ArpcCallBackWorkItem::process() {
    _callBack->callBack();
}

void ArpcCallBackWorkItem::destroy() {
    delete _callBack;
}

void ArpcCallBackWorkItem::drop() {
    _callBack->callBack();
    destroy();
}

} // namespace multi_call
