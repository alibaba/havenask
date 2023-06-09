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
#include "suez/table/RealtimeBuilderWrapper.h"

namespace suez {

RealtimeBuilderWrapper::RealtimeBuilderWrapper(const build_service::proto::PartitionId &pid,
                                               const indexlib::partition::IndexPartitionPtr &indexPartition,
                                               const build_service::workflow::RealtimeBuilderResource &resource,
                                               const std::string &configPath)
    : _pid(pid)
    , _builder(std::make_unique<build_service::workflow::RealtimeBuilder>(configPath, indexPartition, resource)) {}

RealtimeBuilderWrapper::~RealtimeBuilderWrapper() { stop(); }

bool RealtimeBuilderWrapper::start() { return _builder->start(_pid); }

void RealtimeBuilderWrapper::stop() {
    if (_builder) {
        _builder->stop();
        _builder.reset();
    }
}

void RealtimeBuilderWrapper::suspend() {
    if (_builder) {
        _builder->suspendBuild();
    }
}

void RealtimeBuilderWrapper::resume() {
    if (_builder) {
        _builder->resumeBuild();
    }
}

void RealtimeBuilderWrapper::skip(int64_t timestamp) {
    if (_builder) {
        _builder->setTimestampToSkip(timestamp);
    }
}

void RealtimeBuilderWrapper::forceRecover() {
    if (_builder) {
        _builder->forceRecover();
    }
}

bool RealtimeBuilderWrapper::isRecovered() { return _builder && _builder->isRecovered(); }

bool RealtimeBuilderWrapper::needCommit() const { return false; }

std::pair<bool, TableVersion> RealtimeBuilderWrapper::commit() { return {false, TableVersion()}; }

} // namespace suez
