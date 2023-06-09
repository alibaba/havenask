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
#include "ha3/service/QrsRequestLocal.h"

#include "suez/turing/proto/local/GraphServiceLocalStub.h"
#include "suez/sdk/RpcServer.h"

using namespace std;
using namespace multi_call;

namespace isearch {
namespace service {
AUTIL_LOG_SETUP(ha3, QrsRequestLocal);

QrsRequestLocal::QrsRequestLocal(const std::string &methodName,
                       google::protobuf::Message *message,
                       uint32_t timeout,
                       const std::shared_ptr<google::protobuf::Arena> &arena,
                       suez::RpcServer *rpcServer)
    : QrsRequest(methodName, message, timeout, arena)
    , _localStub(nullptr)
    , _rpcServer(rpcServer)
{}

QrsRequestLocal::~QrsRequestLocal() {
    DELETE_AND_SET_NULL(_localStub);
}


google::protobuf::Service *QrsRequestLocal::createStub(google::protobuf::RpcChannel *channel) {
    if (_localStub) {
         delete _localStub;
    }
    _localStub = new ::suez::turing::GraphServiceLocalStub(_rpcServer);
    return _localStub;
}


} // namespace service
} // namespace isearch
