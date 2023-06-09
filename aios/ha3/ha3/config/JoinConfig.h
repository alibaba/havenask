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

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "autil/legacy/jsonizable.h"
#include "suez/turing/common/JoinConfigInfo.h"

namespace isearch {
namespace config {

class JoinConfig : public autil::legacy::Jsonizable
{
public:
    JoinConfig();
    ~JoinConfig();
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);    
public:
    bool empty() const { return _joinConfigInfos.empty(); }

    void getJoinClusters(std::vector<std::string> &clusterNames) const;
    void getJoinFields(std::vector<std::string> &joinFields) const;
    const std::vector<suez::turing::JoinConfigInfo> &getJoinInfos() const {
        return _joinConfigInfos;
    }
    std::vector<suez::turing::JoinConfigInfo> &getJoinInfos() {
        return _joinConfigInfos;
    }
    void addJoinInfo(const suez::turing::JoinConfigInfo &joinInfo) {
        _joinConfigInfos.push_back(joinInfo);
    }
    void setScanJoinCluster(const std::string &scanJoinCluster) {
	_scanJoinCluster = scanJoinCluster;
    }
    const std::string &getScanJoinCluster() const { return _scanJoinCluster; }
    bool operator==(const JoinConfig &other) const {
        return _joinConfigInfos == other._joinConfigInfos;
    }
    bool operator!=(const JoinConfig &other) const {
        return !(*this == other);
    }    
private:
    void doCompatibleWithOldFormat(autil::legacy::Jsonizable::JsonWrapper& json);
private:
    std::string _scanJoinCluster;
    std::vector<suez::turing::JoinConfigInfo> _joinConfigInfos;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<JoinConfig> JoinConfigPtr;

} // namespace config
} // namespace isearch

