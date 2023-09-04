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
#include "sql/resource/Ha3ClusterDefR.h"

#include <algorithm>
#include <engine/NaviConfigContext.h>
#include <utility>

#include "autil/StringUtil.h"
#include "iquan/common/Common.h"
#include "iquan/common/catalog/LocationDef.h"
#include "iquan/common/catalog/TableDef.h"
#include "navi/builder/ResourceDefBuilder.h"
#include "navi/engine/Resource.h"
#include "navi/log/NaviLogger.h"
#include "navi/proto/KernelDef.pb.h"
#include "suez/sdk/TableReader.h"

namespace navi {
class ResourceInitContext;
} // namespace navi

namespace sql {

const std::string Ha3ClusterDefR::RESOURCE_ID = "sql.cluster_def_r";

Ha3ClusterDefR::Ha3ClusterDefR() {}

Ha3ClusterDefR::~Ha3ClusterDefR() {}

void Ha3ClusterDefR::def(navi::ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID, navi::RS_BIZ);
}

bool Ha3ClusterDefR::config(navi::ResourceConfigContext &ctx) {
    return true;
}

navi::ErrorCode Ha3ClusterDefR::init(navi::ResourceInitContext &ctx) {
    const auto &indexProvider = _indexProviderR->getIndexProvider();
    auto &tableReaders = indexProvider.multiTableReader.getAllTableReaders();
    if (tableReaders.empty()) {
        NAVI_KERNEL_LOG(ERROR, "empty cluster map");
        return navi::EC_ABORT;
    }
    for (const auto &pair : tableReaders) {
        auto &tableName = pair.first;
        for (const auto &readerPair : pair.second) {
            auto &singleReader = readerPair.second;
            _tableConfigMap[tableName] = singleReader->getTableDefConfig();
        }
    }
    return navi::EC_NONE;
}

bool Ha3ClusterDefR::getTableDefConfig(const std::string &tableName,
                                       suez::TableDefConfig &tableConfig) const {
    auto it = _tableConfigMap.find(tableName);
    if (_tableConfigMap.end() == it) {
        return false;
    }
    tableConfig = it->second;
    return true;
}

bool Ha3ClusterDefR::fillTableDef(const std::string &zoneName,
                                  const std::string &tableName,
                                  int32_t tablePartCount,
                                  iquan::TableDef &tableDef,
                                  TableSortDescMap &tableSortDescMap) const {
    suez::TableDefConfig tableConfig;
    if (!getTableDefConfig(tableName, tableConfig)) {
        NAVI_KERNEL_LOG(ERROR, "cluster [%s] not exist", tableName.c_str());
        return false;
    }
    addDistributeInfo(tableConfig, tableDef);
    addLocationInfo(tableConfig, zoneName, tablePartCount, tableDef);
    addSortDesc(tableConfig, tableName, tableDef);
    tableSortDescMap[tableName] = tableConfig.sortDescriptions.sortDescs;
    return true;
}

void Ha3ClusterDefR::addDistributeInfo(const suez::TableDefConfig &tableConfig,
                                       iquan::TableDef &tableDef) {
    tableDef.distribution.partitionCnt = tableConfig.tableDist.partitionCnt;
    auto &hashFields = tableConfig.tableDist.hashMode._hashFields;
    if (hashFields.empty()) {
        hashFields.push_back(tableConfig.tableDist.hashMode._hashField);
    }
    tableDef.distribution.hashFields = hashFields;
    tableDef.distribution.hashFunction = tableConfig.tableDist.hashMode._hashFunction;
    tableDef.distribution.hashParams = tableConfig.tableDist.hashMode._hashParams;

    // hack for kingso hash
    if (autil::StringUtil::startsWith(tableDef.distribution.hashFunction, KINGSO_HASH)) {
        std::vector<std::string> pairStr
            = autil::StringUtil::split(tableDef.distribution.hashFunction, KINGSO_HASH_SEPARATOR);
        if (pairStr.size() == 2) {
            tableDef.distribution.hashParams[KINGSO_HASH_PARTITION_CNT] = pairStr[1];
        }
    }
}

void Ha3ClusterDefR::addLocationInfo(const suez::TableDefConfig &tableConfig,
                                     const std::string &tableGroupName,
                                     int32_t tablePartCount,
                                     iquan::TableDef &tableDef) {
    tableDef.location.partitionCnt = tablePartCount;
    tableDef.location.tableGroupName = tableGroupName;
}

void Ha3ClusterDefR::addSortDesc(const suez::TableDefConfig &tableConfig,
                                 const std::string &tableName,
                                 iquan::TableDef &tableDef) {
    for (const auto &sortDesc : tableConfig.sortDescriptions.sortDescs) {
        iquan::SortDescDef def;
        def.field = sortDesc.field;
        def.order = sortDesc.order;
        tableDef.sortDesc.emplace_back(def);
    }
}

REGISTER_RESOURCE(Ha3ClusterDefR);

} // namespace sql
