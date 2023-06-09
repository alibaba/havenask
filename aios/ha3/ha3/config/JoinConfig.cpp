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
#include "ha3/config/JoinConfig.h"

#include <cstddef>
#include <memory>

#include "autil/legacy/jsonizable.h"
#include "suez/turing/common/JoinConfigInfo.h"

#include "ha3/util/TypeDefine.h"
#include "autil/Log.h"

using namespace std;
using namespace suez::turing;

namespace isearch {
namespace config {

AUTIL_LOG_SETUP(ha3, JoinConfig);

JoinConfig::JoinConfig() {
}

JoinConfig::~JoinConfig() {
}

void JoinConfig::Jsonize(
        autil::legacy::Jsonizable::JsonWrapper& json)
{
    JSONIZE(json, "scan_join_cluster", _scanJoinCluster);
    JSONIZE(json, "join_infos", _joinConfigInfos);
    doCompatibleWithOldFormat(json);
}

void JoinConfig::doCompatibleWithOldFormat(autil::legacy::Jsonizable::JsonWrapper& json) {
    bool use_join_cache = false;
    JSONIZE(json, "use_join_cache", use_join_cache);
    if (use_join_cache) {
        for (size_t i = 0; i < _joinConfigInfos.size(); ++i) {
            _joinConfigInfos[i].useJoinCache = true;
        }
    }
}

void JoinConfig::getJoinClusters(vector<string> &joinClusters) const {
    vector<JoinConfigInfo>::const_iterator it;
    for (it = _joinConfigInfos.begin();
         it != _joinConfigInfos.end(); it++)
    {
        joinClusters.push_back((*it).getJoinCluster());
    }
}

void JoinConfig::getJoinFields(vector<string> &joinFields) const {
    vector<JoinConfigInfo>::const_iterator it;
    for (it = _joinConfigInfos.begin();
         it != _joinConfigInfos.end(); it++)
    {
        joinFields.push_back((*it).getJoinField());
    }
}

} // namespace config
} // namespace isearch
