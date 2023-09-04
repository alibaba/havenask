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
#include "HTTPRPCServerWorkItem.h"

#include "HTTPRPCController.h"
#include "HTTPRPCServerClosure.h"
#include "Log.h"
#include "aios/network/anet/anet.h"
#include "autil/TimeUtility.h"

using namespace std;

namespace http_arpc {

HTTPRPCServerWorkItem::HTTPRPCServerWorkItem(RPCService *service,
                                             RPCMethodDescriptor *method,
                                             anet::Connection *connection,
                                             bool keepAlive,
                                             const string &encoding,
                                             const ProtoJsonizerPtr &protoJsonizer,
                                             const EagleInfo &eagleInfo,
                                             string requestStr)
    : _service(service)
    , _method(method)
    , _connection(NULL)
    , _keepAlive(keepAlive)
    , _encoding(encoding)
    , _protoJsonizer(protoJsonizer)
    , _eagleInfo(eagleInfo)
    , _requestStr(requestStr)
    , _beginTime(0) {
    connection->addRef();
    _connection = connection;
}

HTTPRPCServerWorkItem::~HTTPRPCServerWorkItem() { _connection->subRef(); }

void HTTPRPCServerWorkItem::process() {
    RPCMessage *requestMessage = _service->GetRequestPrototype(_method).New();
    RPCMessage *responseMessage = _service->GetResponsePrototype(_method).New();
    unique_ptr<HTTPRPCServerClosure> closure(new HTTPRPCServerClosure(
        _connection, requestMessage, responseMessage, _keepAlive, _encoding, _protoJsonizer, _eagleInfo));
    HTTPRPCController *controller = closure->getController();

    if (!_protoJsonizer->fromJson(_requestStr, requestMessage)) {
        controller->SetFailed("parse request: " + _requestStr + "failed");
        closure.release()->Run();
        return;
    }
    int64_t queueEnd = autil::TimeUtility::currentTime();
    controller->SetQueueTime(queueEnd - _beginTime);
    controller->SetAddr(_addr);
    _service->CallMethod(_method, controller, requestMessage, responseMessage, closure.release());
}

void HTTPRPCServerWorkItem::destroy() { delete this; }

void HTTPRPCServerWorkItem::drop() {
    HTTP_ARPC_LOG(ERROR, "drop work item");
    delete this;
}

void HTTPRPCServerWorkItem::SetBeginTime(int64_t beginTime) { _beginTime = beginTime; }

void HTTPRPCServerWorkItem::SetAddr(const std::string &addr) { _addr = addr; }

} // namespace http_arpc
