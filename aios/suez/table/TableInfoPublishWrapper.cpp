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
#include "suez/table/TableInfoPublishWrapper.h"

#include "aios/network/gig/multi_call/rpc/GigRpcServer.h"

using namespace std;
using namespace autil;

namespace suez {

TableInfoPublishWrapper::TableInfoPublishWrapper(multi_call::GigRpcServer *gigRpcServer) : _gigRpcServer(gigRpcServer) {
    assert(_gigRpcServer && "must have gig rpc server");
}

TableInfoPublishWrapper::~TableInfoPublishWrapper() {}

bool TableInfoPublishWrapper::publish(const multi_call::ServerBizTopoInfo &info, multi_call::SignatureTy &signature) {
    if (!_gigRpcServer->publish(info, signature)) {
        return false;
    } else {
        autil::ScopedLock lock(_lock);
        auto iter = _counterMap.find(signature);
        if (_counterMap.end() == iter) {
            _counterMap.emplace(signature, 1);
        } else {
            _counterMap[signature]++;
        }
    }
    return true;
}

bool TableInfoPublishWrapper::unpublish(multi_call::SignatureTy signature) {
    {
        autil::ScopedLock lock(_lock);
        auto iter = _counterMap.find(signature);
        assert(_counterMap.end() != iter && "signature should be published before unpublish");
        iter->second--;
        if (0 == iter->second) {
            _counterMap.erase(signature);
        } else {
            return true;
        }
    }
    return _gigRpcServer->unpublish(signature);
}

bool TableInfoPublishWrapper::updateVolatileInfo(multi_call::SignatureTy signature,
                                                 const multi_call::BizVolatileInfo &info) {
    return _gigRpcServer->updateVolatileInfo(signature, info);
}

} // namespace suez
