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

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "suez/turing/expression/util/TableInfo.h"

namespace suez {
namespace turing {
class AttributeInfo;
class JoinConfigInfo;
}  // namespace turing
}  // namespace suez

namespace isearch {
namespace config {

class ClusterTableInfoManager
{
public:
    typedef std::pair<suez::turing::JoinConfigInfo, suez::turing::TableInfoPtr>
            JoinTableInfo;
    typedef std::map<std::string, JoinTableInfo> JoinTableInfos;
public:
    ClusterTableInfoManager();
    ~ClusterTableInfoManager();
private:
    ClusterTableInfoManager(const ClusterTableInfoManager &);
    ClusterTableInfoManager& operator=(const ClusterTableInfoManager &);
public:
    suez::turing::TableInfoPtr getClusterTableInfo() const;
    void setMainTableInfo(const suez::turing::TableInfoPtr &tableInfoPtr) {
        _mainTableInfoPtr = tableInfoPtr;
    }
    const suez::turing::TableInfoPtr getMainTableInfo() {
        return _mainTableInfoPtr;
    }
    const JoinTableInfos& getJoinTableInfos() const {
        return _joinTableInfos;
    }
    void addJoinTableInfo(const suez::turing::TableInfoPtr &tableInfoPtr,
	const suez::turing::JoinConfigInfo &joinConfigInfo);
    std::string getJoinFieldNameByTableName(const std::string &tableName) const;
    suez::turing::TableInfoPtr getJoinTableInfoByTableName(const std::string &tableName) const;
    const suez::turing::AttributeInfo *getAttributeInfo(const std::string &attributeName) const;
    void setScanJoinClusterName(const std::string &scanJoinClusterName);
    const std::string &getScanJoinClusterName() const;
public:
    static suez::turing::TableInfoPtr getClusterTableInfo(
            const suez::turing::TableInfoPtr &mainTableInfoPtr,
            const std::vector<suez::turing::TableInfoPtr> &auxTableInfos);
private:
    suez::turing::TableInfoPtr _mainTableInfoPtr;
    JoinTableInfos _joinTableInfos;
    std::string _scanJoinClusterName;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<ClusterTableInfoManager> ClusterTableInfoManagerPtr;

class ClusterTableInfoManagerMap : public std::map<std::string, ClusterTableInfoManagerPtr> {
public:
    ClusterTableInfoManagerPtr getClusterTableInfoManager(const std::string &clusterName) const;
private:
    AUTIL_LOG_DECLARE();
};
typedef std::shared_ptr<ClusterTableInfoManagerMap> ClusterTableInfoManagerMapPtr;

} // namespace config
} // namespace isearch

