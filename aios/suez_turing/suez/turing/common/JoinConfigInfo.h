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
#pragma once

#include <string>

#include "autil/legacy/jsonizable.h"

namespace suez {
namespace turing {

class JoinConfigInfo : public autil::legacy::Jsonizable {
public:
    JoinConfigInfo();
    JoinConfigInfo(bool strongJoin_,
                   const std::string &joinCluster_,
                   const std::string &joinField_,
                   bool useJoinCache_,
                   bool useJoinPK_ = true,
                   bool checkClusterConfig_ = true,
                   const std::string &mainTable_ = "");
    ~JoinConfigInfo();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json);

public:
    bool isStrongJoin() const { return strongJoin; }
    const std::string &getJoinField() const { return joinField; }
    const std::string &getJoinCluster() const { return joinCluster; }
    bool getCheckClusterConfig() const { return checkClusterConfig; }

    bool operator==(const JoinConfigInfo &other) const {
        return strongJoin == other.strongJoin && joinField == other.joinField && useJoinCache == other.useJoinCache &&
               joinCluster == other.joinCluster && mainTable == other.mainTable && useJoinPK == other.useJoinPK &&
               checkClusterConfig == other.checkClusterConfig;
    }

public:
    bool strongJoin;
    std::string joinField;
    std::string joinCluster;
    std::string mainTable;
    bool useJoinCache;
    bool useJoinPK;
    bool checkClusterConfig;
};

} // namespace turing
} // namespace suez
