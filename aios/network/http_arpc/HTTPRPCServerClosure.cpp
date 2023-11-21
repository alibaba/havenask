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
#include "aios/network/http_arpc/HTTPRPCServerClosure.h"

#include "aios/network/anet/anet.h"
#include "aios/network/http_arpc/Log.h"
#include "aios/network/http_arpc/ProtoJsonizer.h"
#include "autil/ZlibCompressor.h"

using namespace std;
using namespace anet;
using namespace autil;

namespace http_arpc {

HTTPRPCServerClosure::HTTPRPCServerClosure(Connection *connection,
                                           RPCMessage *requestMessage,
                                           RPCMessage *responseMessage,
                                           bool keepAlive,
                                           const string &encoding,
                                           const ProtoJsonizerPtr &protoJsonizer,
                                           const EagleInfo &eagleInfo,
                                           const std::string &aiosDebugType)
    : _connection(connection)
    , _requestMessage(requestMessage)
    , _responseMessage(responseMessage)
    , _keepAlive(keepAlive)
    , _encoding(encoding)
    , _protoJsonizer(protoJsonizer)
    , _eagleInfo(eagleInfo)
    , _aiosDebugType(aiosDebugType)
{
    if (_connection != NULL) {
        _connection->addRef();
    }
}

HTTPRPCServerClosure::~HTTPRPCServerClosure() {
    delete _requestMessage;
    _requestMessage = NULL;
    delete _responseMessage;
    _responseMessage = NULL;
    if (_connection != NULL) {
        _connection->subRef();
    }
}

HTTPPacket *HTTPRPCServerClosure::buildPacket() {
    HTTPPacket *reply = new HTTPPacket();
    reply->setPacketType(HTTPPacket::PT_RESPONSE);
    if (_httpController.Failed()) {
        HTTP_ARPC_LOG(WARN, "[%s]", _httpController.ErrorText().c_str());
        reply->setStatusCode(400);
        reply->setBody(_httpController.ErrorText().c_str(), _httpController.ErrorText().size());
    } else {
        string response = _protoJsonizer->toJson(*_responseMessage);
        int code = _protoJsonizer->getStatusCode(*_responseMessage);
        reply->setStatusCode(code);
        if (string::npos == _encoding.find("deflate")) {
            reply->setBody(response.c_str(), response.size());
        } else {
            string compressResult;
            ZlibCompressor compressor;
            if (compressor.compress(response, compressResult)) {
                reply->addHeader("Content-Encoding", "deflate");
                reply->setBody(compressResult.c_str(), compressResult.size());
            } else {
                reply->setBody(response.c_str(), response.size());
            }
        }
    }
    for (auto iter = _responseHeader.begin(); iter != _responseHeader.end(); ++iter) {
        reply->addHeader(iter->first.c_str(), iter->second.c_str());
    }
    reply->setKeepAlive(_keepAlive);
    return reply;
}

void HTTPRPCServerClosure::Run() {
    if (_connection != NULL) {
        HTTPPacket *reply = buildPacket();
        if (!reply->isKeepAlive()) {
            _connection->setWriteFinishClose(true);
        }
        if (!_connection->postPacket(reply)) {
            reply->free();
        }
    }
    delete this;
}

// for case
void HTTPRPCServerClosure::setProtoJsonizer(ProtoJsonizerPtr protoJsonizer) { _protoJsonizer = protoJsonizer; }

ProtoJsonizerPtr HTTPRPCServerClosure::getProtoJsonizer() const { return _protoJsonizer; }

} // namespace http_arpc
