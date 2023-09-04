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
#include "aios/network/gig/multi_call/rpc/GigRpcWorker.h"

using namespace autil;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, GigRpcWorker);

GigRpcWorker::GigRpcWorker() {
}

GigRpcWorker::~GigRpcWorker() {
}

bool GigRpcWorker::addMethod(const std::string &methodName, const GigRpcMethodArgPtr &arg) {
    if (!arg->validate()) {
        AUTIL_LOG(ERROR, "register rpc failed: invalid arg, method[%s]", methodName.c_str());
        return false;
    }
    ScopedReadWriteLock lock(_serviceLock, 'w');
    if (_serviceMap.end() != _serviceMap.find(methodName)) {
        AUTIL_LOG(ERROR, "methodName[%s] already exist", methodName.c_str());
        return false;
    } else {
        _serviceMap.insert(make_pair(methodName, arg));
        return true;
    }
}

GigRpcMethodArgPtr GigRpcWorker::getMethodArg(const std::string &methodName) const {
    ScopedReadWriteLock lock(_serviceLock, 'r');
    auto it = _serviceMap.find(methodName);
    if (_serviceMap.end() == it) {
        return GigRpcMethodArgPtr();
    } else {
        return it->second;
    }
}

bool GigRpcWorker::registerStreamService(const GigServerStreamCreatorPtr &creator) {
    const auto &methodName = creator->methodName();
    ScopedReadWriteLock lock(_serviceLock, 'w');
    if (_streamServiceMap.end() != _streamServiceMap.find(methodName)) {
        AUTIL_LOG(ERROR, "methodName[%s] already exist", methodName.c_str());
        return false;
    } else {
        _streamServiceMap.insert(make_pair(methodName, creator));
        return true;
    }
}

bool GigRpcWorker::unRegisterStreamService(const GigServerStreamCreatorPtr &creator) {
    const auto &methodName = creator->methodName();
    ScopedReadWriteLock lock(_serviceLock, 'w');
    auto it = _streamServiceMap.find(methodName);
    if (_streamServiceMap.end() == it) {
        AUTIL_LOG(ERROR, "methodName[%s] not exist", methodName.c_str());
        return false;
    }
    const auto &registry = it->second;
    if (registry == creator) {
        _streamServiceMap.erase(it);
        return true;
    } else {
        AUTIL_LOG(ERROR, "methodName[%s] is not registered by creator [%p], but by creator [%p]",
                  methodName.c_str(), creator.get(), registry.get());
        return false;
    }
}

GigServerStreamCreatorPtr GigRpcWorker::getStreamService(const std::string &method) const {
    ScopedReadWriteLock lock(_serviceLock, 'r');
    auto it = _streamServiceMap.find(method);
    if (_streamServiceMap.end() == it) {
        return GigServerStreamCreatorPtr();
    } else {
        return it->second;
    }
}

} // namespace multi_call
