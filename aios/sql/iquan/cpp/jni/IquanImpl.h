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
#include "iquan/common/catalog/LayerTableModel.h"
#include "iquan/common/catalog/PlanMeta.h"
#include "iquan/common/catalog/TableModel.h"
#include "iquan/common/catalog/TvfFunctionModel.h"
#include "iquan/config/ClientConfig.h"
#include "iquan/config/ExecConfig.h"
#include "iquan/config/JniConfig.h"
#include "iquan/config/WarmupConfig.h"
#include "iquan/jni/DynamicParamsManager.h"
#include "iquan/jni/Iquan.h"
#include "iquan/jni/IquanCache.h"
#include "iquan/jni/IquanDqlRequest.h"
#include "iquan/jni/IquanDqlResponse.h"
#include "iquan/jni/jnipp/jnipp.h"
#include "iquan/jni/wrapper/JIquanClient.h"

namespace iquan {

using RapidDocPtr = std::shared_ptr<autil::legacy::RapidDocument>;

class IquanImpl {
public:
    IquanImpl() {}

    ~IquanImpl() {
        _planCache.reset();
        _planMetaCache.reset();
    }

    Status init(const JniConfig &jniConfig, const ClientConfig &sqlConfig);
    Status updateCatalog(const CatalogInfo &catalog);
    bool getSecHashKeyStr(PlanMetaPtr planMetaPtr,
                          const DynamicParams &dynamicParams,
                          std::string &hashStr);
    Status getFromCache(IquanDqlRequest &request,
                        IquanDqlResponse &response,
                        std::string &hashKeyStr,
                        std::string &secHashKeyStr,
                        std::string &finalHashKeyStr,
                        PlanCacheStatus &PlanCacheStatus);
    void putCache(SqlPlan &sqlPlan,
                  IquanDqlRequest &request,
                  IquanDqlResponse &response,
                  std::string &hashKeyStr,
                  std::string &secHashKeyStr,
                  std::string &finalHashKeyStr,
                  size_t planStrSize,
                  std::shared_ptr<autil::legacy::RapidDocument> documentPtr,
                  PlanCacheStatus &PlanCacheStatus);
    Status
    query(IquanDqlRequest &request, IquanDqlResponse &response, PlanCacheStatus &planCacheStatus);
    Status dumpCatalog(std::string &result);
    Status warmup(const WarmupConfig &warmupConfig);
    uint64_t getPlanCacheKeyCount();
    uint64_t getPlanMetaCacheKeyCount();
    void resetPlanCache();
    void resetPlanMetaCache();

    // TODO delete
    Status updateTables(const TableModels &tables);
    Status updateTables(const std::string &tableContent);

    Status updateLayerTables(const LayerTableModels &tables);
    Status updateLayerTables(const std::string &tableContent);

    Status updateFunctions(FunctionModels &functions);
    Status updateFunctions(const TvfModels &functions);
    Status updateFunctions(const std::string &functionContent);

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
    std::string readValue(const autil::legacy::json::JsonMap &params,
                          const std::string &key,
                          const std::string &defValue);
    bool addLayerTableMeta(const LayerTableModels &models);
    void writeWarnupLog(IquanDqlRequest &request);
    static Status getHashKeyStr(IquanDqlRequest &request, std::string &hashKeyStr);

private:
    JniConfig _jniConfig;
    std::unique_ptr<IquanCache<RapidDocPtr>> _planCache;
    std::unique_ptr<IquanCache<PlanMetaPtr>> _planMetaCache;
    GlobalRef<JIquanClient> _iquanClient;
    GlobalRef<jbyteArray> _sqlConfigByteArray;
    DynamicParamsManager _dynamicParamsManager;

    AUTIL_LOG_DECLARE();
    static alog::Logger *_warmupLogger;
};

} // namespace iquan
