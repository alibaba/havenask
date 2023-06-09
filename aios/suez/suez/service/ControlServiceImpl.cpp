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
#include "suez/service/ControlServiceImpl.h"

#include "autil/EnvUtil.h"
#include "suez/sdk/RpcServer.h"

namespace suez {

ControlServiceImpl::ControlServiceImpl() {
    _init = autil::EnvUtil::getEnv("control_init", true);
    _update = autil::EnvUtil::getEnv("control_update", true);
    _stop = autil::EnvUtil::getEnv("control_stop", true);
}

ControlServiceImpl::~ControlServiceImpl() {}

bool ControlServiceImpl::init(const SearchInitParam &initParam) {
    bool ret = initParam.rpcServer->RegisterService(this);
    return ret && _init;
}

UPDATE_RESULT ControlServiceImpl::update(const UpdateArgs &updateArgs,
                                         UpdateIndications &updateIndications,
                                         SuezErrorCollector &errorCollector) {
    if (!_update) {
        return UR_NEED_RETRY;
    }
    autil::ScopedLock lock(_mutex);
    _indexProvider = updateArgs.indexProvider;
    return UR_REACH_TARGET;
}

void ControlServiceImpl::stopService() {
    if (!_stop) {
        return;
    }
    autil::ScopedLock lock(_mutex);
    _indexProvider.reset();
}

void ControlServiceImpl::stopWorker() { stopService(); }

void ControlServiceImpl::control(google::protobuf::RpcController *controller,
                                 const ControlParam *request,
                                 ControlParam *response,
                                 google::protobuf::Closure *done) {
    if (request->has_init()) {
        _init = request->init();
    }
    if (request->has_update()) {
        _update = request->update();
    }
    if (request->has_stop()) {
        _stop = request->stop();
    }
    response->set_init(_init.load());
    response->set_update(_update.load());
    response->set_stop(_stop.load());
    if (done) {
        done->Run();
    }
}

} // namespace suez
