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

#include "iquan/common/Common.h"
#include "iquan/common/Status.h"
#include "iquan/common/catalog/CatalogInfo.h"
#include "iquan/common/catalog/FunctionModel.h"
#include "iquan/common/catalog/TableModel.h"
#include "iquan/common/catalog/LayerTableModel.h"
#include "iquan/common/catalog/TvfFunctionModel.h"
#include "iquan/config/ClientConfig.h"
#include "iquan/config/ExecConfig.h"
#include "iquan/config/JniConfig.h"
#include "iquan/config/WarmupConfig.h"
#include "iquan/jni/IquanDqlRequest.h"
#include "iquan/jni/IquanDqlResponse.h"

namespace iquan {

class IquanImpl;

struct PlanCacheStatus {
    bool planPut;
    bool planGet;
    bool planMetaPut;
    bool planMetaGet;

    void reset() {
        planPut = false;
        planGet = false;
        planMetaPut = false;
        planMetaGet = false;
    }
};

class Iquan {
public:
    Iquan();
    ~Iquan();

    Status init(const JniConfig &jniConfig, const ClientConfig &sqlConfig);
    // TODO impl
    // merge updateTables and updateFunctions
    Status updateCatalog(const CatalogInfo &catalog);
    Status query(IquanDqlRequest &request, IquanDqlResponse &response, PlanCacheStatus &planCacheStatus);
    Status dumpCatalog(std::string &result);
    Status warmup(const WarmupConfig &warmupConfig);
    uint64_t getPlanCacheKeyCount();
    uint64_t getPlanMetaCacheKeyCount();
    void resetPlanCache();
    void resetPlanMetaCache();

    // TODO delete
    // 1. update tables
    // table format example:
    // xxxx://invalid/isearch/iquan/tree/iquan-0.3.0-dev/iquan_core/src/test/resources/catalogs/default/db1/tables
    Status updateTables(const TableModels &tables);
    Status updateTables(const std::string &tableContent);
    Status updateLayerTables(const LayerTableModels &tables);
    Status updateLayerTables(const std::string &tableContent);

    // TODO delete
    // 2. update functions
    // fucntion format example:
    // xxxx://invalid/isearch/iquan/tree/iquan-0.3.0-dev/iquan_core/src/test/resources/catalogs/default/db1/functions
    Status updateFunctions(FunctionModels &functions);
    Status updateFunctions(const TvfModels &functions);
    Status updateFunctions(const std::string &functionContent);

    // TODO delete, only for debugging
    // 5. catalog function
    Status listCatalogs(std::string &result);
    Status listDatabases(const std::string &catalogName, std::string &result);
    Status listTables(const std::string &catalogName, const std::string &dbName, std::string &result);
    Status listFunctions(const std::string &catalogName, const std::string &dbName, std::string &result);
    Status getTableDetails(const std::string &catalogName,
                           const std::string &dbName,
                           const std::string &tableName,
                           std::string &result);
    Status getFunctionDetails(const std::string &catalogName,
                              const std::string &dbName,
                              const std::string &functionName,
                              std::string &result);
                            
private:
    std::unique_ptr<IquanImpl> _impl;
};

IQUAN_TYPEDEF_PTR(Iquan);

} // namespace iquan
