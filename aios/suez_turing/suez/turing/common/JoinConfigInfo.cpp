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
#include "suez/turing/common/JoinConfigInfo.h"

#include <iosfwd>

using namespace std;

namespace suez {
namespace turing {

JoinConfigInfo::JoinConfigInfo() : strongJoin(false), useJoinCache(false), useJoinPK(true), checkClusterConfig(true) {}

JoinConfigInfo::JoinConfigInfo(bool strongJoin_,
                               const std::string &joinCluster_,
                               const std::string &joinField_,
                               bool useJoinCache_,
                               bool useJoinPK_,
                               bool checkClusterConfig_,
                               const std::string &mainTable_)
    : strongJoin(strongJoin_)
    , joinField(joinField_)
    , joinCluster(joinCluster_)
    , mainTable(mainTable_)
    , useJoinCache(useJoinCache_)
    , useJoinPK(useJoinPK_)
    , checkClusterConfig(checkClusterConfig_) {}

JoinConfigInfo::~JoinConfigInfo() {}

void JoinConfigInfo::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("strong_join", strongJoin, strongJoin);
    json.Jsonize("join_field", joinField, joinField);
    json.Jsonize("join_cluster", joinCluster, joinCluster);
    json.Jsonize("main_table", mainTable, mainTable);
    json.Jsonize("use_join_cache", useJoinCache, useJoinCache);
    json.Jsonize("use_join_pk", useJoinPK, useJoinPK);
    json.Jsonize("check_cluster_config", checkClusterConfig, checkClusterConfig);
}

} // namespace turing
} // namespace suez
