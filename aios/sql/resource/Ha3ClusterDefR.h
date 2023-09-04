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
#include <stdint.h>
#include <string>
#include <unordered_map>
#include <vector>

#include "navi/common.h"
#include "navi/engine/Resource.h"
#include "navi/engine/ResourceConfigContext.h"
#include "suez/sdk/IndexProviderR.h"

namespace iquan {
class TableDef;
} // namespace iquan
namespace navi {
class ResourceDefBuilder;
class ResourceInitContext;
} // namespace navi

namespace sql {

typedef std::unordered_map<std::string, std::vector<suez::SortDescription>> TableSortDescMap;

class Ha3ClusterDefR : public navi::Resource {
public:
    Ha3ClusterDefR();
    ~Ha3ClusterDefR();
    Ha3ClusterDefR(const Ha3ClusterDefR &) = delete;
    Ha3ClusterDefR &operator=(const Ha3ClusterDefR &) = delete;

public:
    void def(navi::ResourceDefBuilder &builder) const override;
    bool config(navi::ResourceConfigContext &ctx) override;
    navi::ErrorCode init(navi::ResourceInitContext &ctx) override;

public:
    bool fillTableDef(const std::string &zoneName,
                      const std::string &tableName,
                      int32_t tablePartCount,
                      iquan::TableDef &tableDef,
                      TableSortDescMap &tableSortDescMap) const;

private:
    bool getTableDefConfig(const std::string &cluster, suez::TableDefConfig &tableConfig) const;

private:
    static void addDistributeInfo(const suez::TableDefConfig &tableConfig,
                                  iquan::TableDef &tableDef);
    static void addLocationInfo(const suez::TableDefConfig &tableConfig,
                                const std::string &tableGroupName,
                                int32_t tablePartCount,
                                iquan::TableDef &tableDef);
    static void addSortDesc(const suez::TableDefConfig &tableConfig,
                            const std::string &tableName,
                            iquan::TableDef &tableDef);

public:
    static const std::string RESOURCE_ID;

private:
    RESOURCE_DEPEND_DECLARE();

private:
    RESOURCE_DEPEND_ON(suez::IndexProviderR, _indexProviderR);
    std::map<std::string, suez::TableDefConfig> _tableConfigMap;
};

NAVI_TYPEDEF_PTR(Ha3ClusterDefR);

} // namespace sql
