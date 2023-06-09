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
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "autil/legacy/json.h"
#include "fslib/common/common_type.h"
#include "ha3/config/AnomalyProcessConfig.h"
#include "ha3/config/ClusterConfigInfo.h"
#include "ha3/config/ClusterConfigParser.h"
#include "ha3/config/ClusterTableInfoManager.h"
#include "ha3/config/FinalSorterConfig.h"
#include "ha3/config/VersionConfig.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/misc/common.h"
#include "suez/turing/expression/util/TableInfo.h"

namespace indexlibv2 {
namespace config {
class ITabletSchema;
}
} // namespace indexlibv2

namespace suez {
namespace turing {
class CavaConfig;
class FuncConfig;
}  // namespace turing
}  // namespace suez

namespace isearch {
namespace config {
class AggSamplerConfigInfo;
class QrsConfig;
class RankProfileConfig;
class SearchOptimizerConfig;
class SearcherCacheConfig;
class ServiceDegradationConfig;
class SqlConfig;
class SummaryProfileConfig;
class TuringOptionsInfo;

class ConfigAdapter
{
private:
    typedef ClusterConfigParser::ParseResult ParseResult;
public:
    ConfigAdapter(const std::string &configPath);
    ~ConfigAdapter();
private:
    ConfigAdapter(const ConfigAdapter &);
    ConfigAdapter& operator = (const ConfigAdapter &);
public:
    VersionConfig getVersionConfig() const;
    bool getQrsConfig(QrsConfig &qrsConfig) const;
    bool getSqlConfig(SqlConfig &sqlConfig) const;
    std::string getPluginPath() const;
    std::string getAliTokenizerConfFilePath() const;
    const std::string &getConfigPath() const { return _configPath; }

    //basic config read
    bool readConfigFile(std::string &result,
                        const std::string &relativePath) const;

    //cluster.json related
    bool getClusterNames(std::vector<std::string>& clusterNames) const;
    bool getTuringClusterNames(std::vector<std::string>& clusterNames) const;
    bool getClusterNamesWithExtendBizs(std::vector<std::string>& clusterNames);
    ClusterConfigMapPtr getClusterConfigMap();
    bool getClusterConfigInfo(const std::string &clusterName,
                              ClusterConfigInfo &clusterConfigInfo) const;
    ClusterTableInfoManagerMapPtr getClusterTableInfoManagerMap();
    bool getAggSamplerConfigInfo(const std::string &clusterName,
                                 AggSamplerConfigInfo &aggSamplerInfo) const;
    bool getTuringOptionsInfo(const std::string &clusterName,
                                TuringOptionsInfo &turingOptionsInfo) const;
    bool getRankProfileConfig(const std::string &clusterName,
                              RankProfileConfig &rankProfileConfig) const;
    bool getSummaryProfileConfig(
            const std::string &clusterName,
            SummaryProfileConfig &summaryProfileConfig) const;
    bool getSearchOptimizerConfig(const std::string &clusterName, SearchOptimizerConfig& config) const;
    bool getFuncConfig(const std::string &clusterName,
                       suez::turing::FuncConfig &funcConfig) const;
    bool getSearcherCacheConfig(const std::string &clusterName,
                                SearcherCacheConfig &searcherCacheConfig) const;
    bool getFinalSorterConfig(const std::string &clusterName,
            FinalSorterConfig &finalSorterConfig) const;
    bool getServiceDegradationConfig(const std::string &clusterName,
            ServiceDegradationConfig &serviceDegradationConfig) const;

    bool getAnomalyProcessConfig(const std::string &clusterName,
                                 AnomalyProcessConfig &anomlyProcessConfig) const;
    bool getAnomalyProcessConfigT(const std::string &clusterName,
                                  AnomalyProcessConfig &anomlyProcessConfig) const;
    bool getSqlAnomalyProcessConfig(const std::string &clusterName,
                                    AnomalyProcessConfig &anomlyProcessConfig) const;
    bool getTuringFlowConfigs(const std::string &clusterName,
                              std::map<std::string, autil::legacy::json::JsonMap> &flowConfigs) const;


    bool getCavaConfig(const std::string &clusterName,
                       suez::turing::CavaConfig &cavaConfig) const;
    bool getIndexPartitionOptions(
            const std::string &clusterName,
            indexlib::config::IndexPartitionOptions &indexOptions) const;
    std::vector<std::string> getJoinClusters(const std::string &clusterName) const;
    //table related
    const suez::turing::TableMapPtr& getTableMap();
    const suez::turing::ClusterTableInfoMapPtr& getClusterTableInfoMap();
    bool getSchemaString(const std::string& clusterName,
                         std::string& schemaStr) const;
    bool getTableSchemaConfigString(const std::string &tableName,
                                    std::string &configStr) const;
    bool getTableClusterConfigString(const std::string &tableName,
            std::string &configStr) const;
    bool getTableInfo(const std::string &tableName,
                      suez::turing::TableInfoPtr &tableInfoPtr) const;
    suez::turing::TableInfoPtr getTableInfoWithClusterName(const std::string &clusterName, bool checkClusterConfig);
    ClusterTableInfoManagerPtr getClusterTableInfoManager(
            const std::string &clusterName);
    //analyzer related
    bool getAnalyzerConfigString(std::string &analyzerConfigStr) const;

    // check
    bool check();

    std::shared_ptr<indexlibv2::config::ITabletSchema>
    getIndexSchemaByTable(const std::string table) const;
    std::shared_ptr<indexlibv2::config::ITabletSchema>
    getIndexSchemaByFileName(const std::string &fileName) const;
    std::shared_ptr<indexlibv2::config::ITabletSchema>
    getIndexSchema(const std::string clusterName) const;
    std::string getSqlFunctionConfigFileName(const std::string &clusterName) const;
    std::string getSqlLogicTableConfigFileName(const std::string &clusterName) const;
    std::string getSqlLayerTableConfigFileName(const std::string &clusterName) const;
    std::string getSqlRemoteTableConfigFileName(const std::string &clusterName) const;

    bool getSqlFunctionConfigString(const std::string &clusterName,
                                    std::string &configStr) const;
    bool getSqlLogicTableConfigString(const std::string &clusterName,
            std::string &configStr) const;
    bool getSqlLayerTableConfigString(const std::string &clusterName,
            std::string &configStr) const;
    bool getSqlRemoteTableConfigString(const std::string &clusterName,
            std::string &configStr) const;

public:
    void initCanBeEmptyTable();

private:
    bool listDir(const std::string &path, fslib::FileList &fileList) const;
    std::vector<std::string> splitName(const std::string &name) const;
private:
    std::string getClusterConfigFileName(const std::string &clusterName) const;
    std::string getSchemaConfigFileName(const std::string &tableName) const;
    std::string getTableClusterConfigFileName(const std::string &tableName) const;
    bool convertParseResult(ParseResult parseResult,
                            const std::string &sectionName) const;
    bool isSubTable(const std::string clusterName) const;
    std::string getDefaultAgg();
    std::string getParaWays();

private:
    std::string _configPath;
    ClusterConfigMapPtr _clusterConfigMapPtr;
    suez::turing::TableMapPtr _tableMapPtr;
    suez::turing::ClusterTableInfoMapPtr _clusterTableInfoMapPtr;
    ClusterTableInfoManagerMapPtr _clusterTableInfoManagerMapPtr;
    AnomalyProcessConfigMapPtr _anomalyProcessConfigMapPtr;
private:
    std::map<std::string, bool> _canBeEmptyTable;
private:
    friend class ConfigAdapterTest;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<ConfigAdapter> ConfigAdapterPtr;

} // namespace config
} // namespace isearch
