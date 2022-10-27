#ifndef ISEARCH_CONFIGADAPTER_H
#define ISEARCH_CONFIGADAPTER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/config/ClusterConfigInfo.h>
#include <suez/turing/expression/util/TableInfo.h>
#include <ha3/config/QueryInfo.h>
#include <ha3/config/QrsConfig.h>
#include <ha3/config/SqlConfig.h>
#include <suez/turing/expression/function/FuncConfig.h>
#include <ha3/config/SearcherCacheConfig.h>
#include <ha3/config/AggSamplerConfigInfo.h>
#include <ha3/config/RankProfileConfig.h>
#include <ha3/config/SummaryProfileConfig.h>
#include <ha3/config/FinalSorterConfig.h>
#include <ha3/config/SearchOptimizerConfig.h>
#include <ha3/config/ServiceDegradationConfig.h>
#include <ha3/config/ClusterConfigParser.h>
#include <ha3/config/AnomalyProcessConfig.h>
#include <ha3/config/ClusterTableInfoManager.h>
#include <suez/turing/common/CavaConfig.h>
#include <ha3/config/TuringOptionsInfo.h>
#include <ha3/config/VersionConfig.h>
#include <indexlib/config/index_partition_options.h>
#include <indexlib/config/index_partition_schema.h>
#include <ha3/common/ErrorDefine.h>
#include <worker_framework/DataClientWrapper.h>

BEGIN_HA3_NAMESPACE(config);

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
            IE_NAMESPACE(config)::IndexPartitionOptions &indexOptions) const;
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

    bool getIndexPartitionSchemaByTable(
            const std::string table,
            IE_NAMESPACE(config)::IndexPartitionSchemaPtr &schema) const;
    bool getIndexPartitionSchema(
            const std::string clusterName,
            IE_NAMESPACE(config)::IndexPartitionSchemaPtr &schema) const;
    std::string getSqlFunctionConfigFileName(const std::string &clusterName) const;
    std::string getSqlLogicTableConfigFileName(const std::string &clusterName) const;
    bool getSqlFunctionConfigString(const std::string &binaryPath,
                                    const std::string &clusterName,
                                    std::string &configStr) const;
    bool getSqlLogicTableConfigString(const std::string &clusterName,
                                    std::string &configStr) const;
public:
    static void initCanBeEmptyTable();
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
    ErrorCode WFToHa3ErrorCode(
            WORKER_FRAMEWORK_NS(data_client)::DataClientWrapper::WFErrorCode wfCode);
private:
    std::string _configPath;
    ClusterConfigMapPtr _clusterConfigMapPtr;
    suez::turing::TableMapPtr _tableMapPtr;
    suez::turing::ClusterTableInfoMapPtr _clusterTableInfoMapPtr;
    ClusterTableInfoManagerMapPtr _clusterTableInfoManagerMapPtr;
    AnomalyProcessConfigMapPtr _anomalyProcessConfigMapPtr;
private:
    static std::map<std::string, bool> _canBeEmptyTable;
private:
    friend class ConfigAdapterTest;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(ConfigAdapter);

END_HA3_NAMESPACE(config);

#endif //ISEARCH_CONFIGADAPTER_H
