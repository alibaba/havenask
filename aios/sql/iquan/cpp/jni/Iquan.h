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
#include "iquan/common/catalog/CatalogDef.h"
#include "iquan/common/catalog/FunctionModel.h"
#include "iquan/common/catalog/LayerTableModel.h"
#include "iquan/common/catalog/TableModel.h"
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
    Status
    query(IquanDqlRequest &request, IquanDqlResponse &response, PlanCacheStatus &planCacheStatus);
    Status dumpCatalog(std::string &result);
    Status warmup(const WarmupConfig &warmupConfig);
    uint64_t getPlanCacheKeyCount();
    uint64_t getPlanMetaCacheKeyCount();
    void resetPlanCache();
    void resetPlanMetaCache();

    Status registerCatalogs(const CatalogDefs &catalogs);

    // TODO delete, only for debugging
    // 5. catalog function
    Status listCatalogs(std::string &result);
    Status listDatabases(const std::string &catalogName, std::string &result);
    Status
    listTables(const std::string &catalogName, const std::string &dbName, std::string &result);
    Status
    listFunctions(const std::string &catalogName, const std::string &dbName, std::string &result);
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
