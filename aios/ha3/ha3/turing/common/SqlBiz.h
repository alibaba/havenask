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

#include "autil/mem_pool/Pool.h"
#include "iquan/jni/Iquan.h"
#include "navi/engine/Navi.h"
#include "suez/sdk/BizMeta.h"

#include "ha3/turing/common/Ha3BizBase.h"
#include "ha3/config/ConfigAdapter.h"
#include "ha3/config/SqlConfig.h"
#include "ha3/config/ResourceReader.h"
#include "ha3/sql/ops/agg/AggFuncManager.h"
#include "ha3/sql/ops/tvf/TvfFuncManager.h"
#include "ha3/sql/ops/scan/UdfToQueryManager.h"
#include "ha3/sql/ops/tvf/TvfFuncManager.h"
#include "ha3/sql/ops/agg/AggFuncManager.h"

namespace isearch {
namespace turing {
class Ha3SortDesc;
class Ha3BizMeta;
class Ha3ServiceSnapshot;
class Ha3ClusterDef;
} // namespace turing
} // namespace isearch

namespace iquan {
class TableModels;
class LayerTableModels;
class FunctionModels;
}

namespace isearch {
namespace turing {

class SqlBiz: public Ha3BizBase
{
public:
    SqlBiz(isearch::turing::Ha3ServiceSnapshot *snapshot,
           isearch::turing::Ha3BizMeta *ha3BizMeta);
    virtual ~SqlBiz();
private:
    SqlBiz(const SqlBiz &);
    SqlBiz& operator=(const SqlBiz &);
public:
    suez::turing::ReturnStatus initBiz(const std::string &bizName,
                                       const suez::BizMeta &bizMeta,
                                       suez::turing::BizInitOptionBase &initOptions) override;
    config::SqlConfigPtr getSqlConfig() { return _sqlConfigPtr; }
    navi::Navi *getNavi() const;

protected:
    tensorflow::Status preloadBiz() override;
    void addDistributeInfo(const Ha3ClusterDef &clusterDef, iquan::TableDef &tableDef);
    void addLocationInfo(const turing::Ha3ClusterDef &ha3ClusterDef,
                         const std::string &tableGroupName,
                         iquan::TableDef &tableDef);
    void addSortDesc(const turing::Ha3ClusterDef &ha3ClusterDef,
                     iquan::TableDef &tableDef,
                     const std::string &tableName);
    void addJoinInfo(const std::vector<indexlib::partition::JoinField> &joinFields,
                     iquan::TableDef &tableDef);
    tensorflow::Status getDependTables(const std::string &bizName,
            std::vector<std::string> &dependTables, std::string &dbName);
    tensorflow::Status fillTableModelsFromPartition(iquan::TableModels &tableModels);
    tensorflow::Status fillLogicalTableModels(
            const std::vector<std::string> &clusterNames,
            iquan::TableModels &tableModels);
    tensorflow::Status fillLayerTableModels(
            const std::vector<std::string> &clusterNames,
            iquan::LayerTableModels &layerTableModels);
    tensorflow::Status readClusterFile(const std::string &tableName, Ha3ClusterDef &clusterDef);
    tensorflow::Status fillFunctionModels(const std::vector<std::string> &clusterNames,
            iquan::FunctionModels &functionModels,
            iquan::TvfModels &tvfModels);
    bool initSqlAggFuncManager(const isearch::sql::SqlAggPluginConfig &sqlAggPluginConfig);
    bool initSqlTvfFuncManager(const isearch::sql::SqlTvfPluginConfig &sqlTvfPluginConfig);
    bool initSqlUdfToQueryManager();
    static void addInnerDocId(iquan::TableDef &tableDef);
    std::string getConfigZoneBizName(const std::string &zoneName);

private:
    virtual tensorflow::Status initUserMetadata();
    static std::string getSearchBizInfoFile(const std::string &bizName);
    bool isDefaultBiz(const std::string &bizName) const;
    std::string getSearchDefaultBizJson(const std::string &bizName,
            const std::string &dbName);
    template <typename IquanModels>
    void addUserFunctionModels(const IquanModels &inModels,
                               const std::vector<std::string> &specialCatalogs,
                               const std::string &dbName,
                               IquanModels &functionModels);
    bool getDefaultUdfFunctionModels(iquan::FunctionModels &udfFunctionModels);
    bool mergeUserUdfFunctionModels(const std::string &dbName,
                                    iquan::FunctionModels &udfFunctionModels);
protected:
    std::string clusterNameToDbName(const std::string &clusterName) const;

protected:
    config::ConfigAdapterPtr _configAdapter;
    config::SqlConfigPtr _sqlConfigPtr;
    config::ResourceReaderPtr _pluginResouceReader;
    isearch::sql::AggFuncManagerPtr _aggFuncManager;
    isearch::sql::TvfFuncManagerPtr _tvfFuncManager;
    isearch::sql::UdfToQueryManagerPtr _udfToQueryManager;
    isearch::turing::Ha3ServiceSnapshot *_serviceSnapshot = nullptr;
    isearch::turing::Ha3BizMeta *_ha3BizMeta = nullptr;
    int64_t _tableVersion;
    int64_t _functionVersion;
    std::unordered_map<std::string, std::vector<turing::Ha3SortDesc>> _tableSortDescMap;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SqlBiz> SqlBizPtr;

template <typename IquanModels>
void SqlBiz::addUserFunctionModels(const IquanModels &inModels,
                                      const std::vector<std::string> &specialCatalogs,
                                      const std::string &dbName,
                                      IquanModels &outModels)
{

    for (auto model : inModels.functions) {
        if (!model.catalogName.empty()) {
            model.databaseName = dbName;
            model.functionVersion = _functionVersion;
            outModels.functions.emplace_back(model);
        } else if (specialCatalogs.empty()){
            model.catalogName = SQL_DEFAULT_CATALOG_NAME;
            model.databaseName = dbName;
            model.functionVersion = _functionVersion;
            outModels.functions.emplace_back(model);
        } else {
            AUTIL_LOG(INFO, "use special catalog name: [%s]",
                    autil::StringUtil::toString(specialCatalogs).c_str());
            for (auto const &catalogName : specialCatalogs) {
                model.catalogName = catalogName;
                model.databaseName = dbName;
                model.functionVersion = _functionVersion;
                outModels.functions.emplace_back(model);
            }
        }
    }
}


} // namespace turing
} // namespace isearch
