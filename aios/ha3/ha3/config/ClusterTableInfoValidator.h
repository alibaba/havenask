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

#include <memory>
#include <string>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/config/ClusterTableInfoManager.h"
#include "indexlib/indexlib.h"
#include "suez/turing/expression/util/TableInfo.h"


namespace isearch {
namespace config {

class ClusterTableInfoValidator
{
public:
    ClusterTableInfoValidator();
    ~ClusterTableInfoValidator();
private:
    ClusterTableInfoValidator(const ClusterTableInfoValidator &);
    ClusterTableInfoValidator& operator=(const ClusterTableInfoValidator &);
public:
    static bool validate(const ClusterTableInfoManagerMapPtr &clusterTableInfoManagerMapPtr);
    static bool validateMainTableAgainstJoinTable(const suez::turing::TableInfoPtr &mainTableInfoPtr,
            const suez::turing::TableInfoPtr &joinTableInfoPtr,
	const std::string &joinFieldName, bool usePK);
    static bool validateJoinTables(const std::vector<suez::turing::TableInfoPtr> &joinTables);
private:
    static bool validateFieldInfosBetweenJoinTables(const suez::turing::TableInfoPtr &joinTableInfoPtr1,
            const suez::turing::TableInfoPtr &joinTableInfoPtr2);
    static bool validateSingleClusterTableInfoManager(
            const ClusterTableInfoManagerPtr &clusterTableInfoManagerPtr);
    static bool isSupportedFieldType(FieldType ft);
private:
    typedef ClusterTableInfoManager::JoinTableInfos JoinTableInfos;
    friend class ClusterTableInfoValidatorTest;
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<ClusterTableInfoValidator> ClusterTableInfoValidatorPtr;

} // namespace config
} // namespace isearch

