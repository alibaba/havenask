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

#include <stddef.h>
#include <stdint.h>
#include <map>
#include <memory>
#include <string>

#include "autil/HashFunctionBase.h"
#include "autil/Log.h" // IWYU pragma: keep
#include "autil/legacy/jsonizable.h"
#include "build_service/config/HashMode.h"
#include "ha3/common/QueryInfo.h"
#include "ha3/config/JoinConfig.h"

namespace isearch {
namespace config {
typedef build_service::config::HashMode HashMode;

class ClusterConfigInfo : public autil::legacy::Jsonizable
{
public:
    ClusterConfigInfo();
    ~ClusterConfigInfo();
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);

    void setTableName(const std::string &tableName) {
        _tableName = tableName;
    }
    const std::string& getTableName() const {
        return _tableName;
    }
    const HashMode& getHashMode() const {
        return _hashMode;
    }
    void setQueryInfo(const common::QueryInfo& queryInfo) {
        _queryInfo = queryInfo;
    }
    const common::QueryInfo& getQueryInfo() const {
        return _queryInfo;
    }

    const JoinConfig& getJoinConfig() const {
        return _joinConfig;
    }
    autil::HashFunctionBasePtr getHashFunction() const {
        return _hashFunctionBasePtr;
    }
    bool initHashFunc();
    bool check() const;
public:
    std::string _tableName;
    HashMode _hashMode;
    autil::HashFunctionBasePtr _hashFunctionBasePtr;
    common::QueryInfo _queryInfo;
    JoinConfig _joinConfig;
    uint32_t _returnHitThreshold;
    double _returnHitRatio;
    size_t _poolTrunkSize; // M
    size_t _poolRecycleSizeLimit; // M
    size_t _poolMaxCount;
    size_t _threadInitRoundRobin;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<ClusterConfigInfo> ClusterConfigInfoPtr;

typedef std::map<std::string, ClusterConfigInfo> ClusterConfigMap;
typedef std::shared_ptr<ClusterConfigMap> ClusterConfigMapPtr;

} // namespace config
} // namespace isearch
