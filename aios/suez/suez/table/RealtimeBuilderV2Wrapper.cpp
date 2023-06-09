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
#include "suez/table/RealtimeBuilderV2Wrapper.h"

#include "autil/Log.h"
#include "build_service/workflow/RealtimeBuilderV2.h"

namespace suez {
AUTIL_DECLARE_AND_SETUP_LOGGER(suez, RealtimeBuilderV2Wrapper);

RealtimeBuilderV2Wrapper::RealtimeBuilderV2Wrapper(const build_service::proto::PartitionId &pid,
                                                   const std::shared_ptr<indexlibv2::framework::ITablet> &tablet,
                                                   const build_service::workflow::RealtimeBuilderResource &resource,
                                                   const std::string &configPath)
    : _pid(pid), _builder(std::make_unique<build_service::workflow::RealtimeBuilderV2>(configPath, tablet, resource)) {}

RealtimeBuilderV2Wrapper::~RealtimeBuilderV2Wrapper() { stop(); }

bool RealtimeBuilderV2Wrapper::start() { return _builder->start(_pid); }

void RealtimeBuilderV2Wrapper::stop() {
    if (_builder) {
        _builder->stop();
        _builder.reset();
    }
}

void RealtimeBuilderV2Wrapper::suspend() {
    if (_builder) {
        _builder->suspendBuild();
    }
}

void RealtimeBuilderV2Wrapper::resume() {
    if (_builder) {
        _builder->resumeBuild();
    }
}

void RealtimeBuilderV2Wrapper::skip(int64_t timestamp) {
    if (_builder) {
        _builder->setTimestampToSkip(timestamp);
    }
}

void RealtimeBuilderV2Wrapper::forceRecover() {
    if (_builder) {
        _builder->forceRecover();
    }
}

bool RealtimeBuilderV2Wrapper::isRecovered() { return _builder && _builder->isRecovered(); }

bool RealtimeBuilderV2Wrapper::needCommit() const { return _allowCommit && _builder->needCommit(); }

std::pair<bool, TableVersion> RealtimeBuilderV2Wrapper::commit() {
    AUTIL_LOG(INFO, "%s begin commit", _pid.DebugString().c_str());
    auto result = _builder->commit();
    AUTIL_LOG(INFO, "%s finish commit", _pid.DebugString().c_str());

    const auto &s = result.first;
    if (!s.IsOK()) {
        AUTIL_LOG(ERROR, "%s commit failed, error: %s", _pid.DebugString().c_str(), s.ToString().c_str());
        return {false, TableVersion()};
    }
    const auto &versionMeta = result.second;
    auto finished = versionMeta.IsSealed();
    return {true, TableVersion(versionMeta.GetVersionId(), versionMeta, finished)};
}

} // namespace suez
