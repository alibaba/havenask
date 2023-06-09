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
#include "aios/network/gig/multi_call/common/TopoInfoBuilder.h"
#include <sstream>

using namespace std;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, TopoInfoBuilder);

TopoInfoBuilder::TopoInfoBuilder() : _grpcPort(INVALID_PORT) {}

TopoInfoBuilder::TopoInfoBuilder(int32_t grpcPort) : _grpcPort(grpcPort) {}

TopoInfoBuilder::~TopoInfoBuilder() {}

void TopoInfoBuilder::addBiz(const std::string &bizName, PartIdTy partCnt,
                             PartIdTy partId, VersionTy version,
                             WeightTy targetWeight, VersionTy protocolVersion,
                             int32_t gigRpcPort) {
    BizTopoInfo bizInfo;
    bizInfo.bizName = bizName;
    bizInfo.partCnt = partCnt;
    bizInfo.partId = partId;
    bizInfo.version = version;
    bizInfo.targetWeight = targetWeight;
    bizInfo.protocolVersion = protocolVersion;
    bizInfo.gigRpcPort = gigRpcPort;

    _infos.emplace_back(std::move(bizInfo));
}

void TopoInfoBuilder::addBiz(const std::string &bizName, PartIdTy partCnt,
                             PartIdTy partId, VersionTy version,
                             WeightTy targetWeight, VersionTy protocolVersion) {
    addBiz(bizName, partCnt, partId, version, targetWeight, protocolVersion,
           _grpcPort);
}

void TopoInfoBuilder::flushAllBizVersion(VersionTy version) {
    for (auto &bizInfo : _infos) {
        bizInfo.version = version;
    }
}

std::string TopoInfoBuilder::build() const {
    std::stringstream ss;
    for (const auto &bizInfo : _infos) {
        ss << bizInfo.bizName << ":" << bizInfo.partCnt << ":" << bizInfo.partId << ":"
           << bizInfo.version << ":" << bizInfo.targetWeight << ":" << bizInfo.protocolVersion
           << ":" << bizInfo.gigRpcPort << ":false:" << bizInfo.gigRpcPort
           << "|"; // support heartbeat
    }
    auto ret = ss.str();
    if (ret.empty()) {
        ss << "invalid_topo_info:0:0:0:false:" << _grpcPort;
        return ss.str();
    } else {
        return ret;
    }
}
const std::vector<BizTopoInfo> &TopoInfoBuilder::getBizTopoInfo() const {
    return _infos;
}
} // namespace multi_call
