#ifndef ISEARCH_CLUSTERCONFIGPARSER_H
#define ISEARCH_CLUSTERCONFIGPARSER_H

#include <ha3/common.h>
#include <ha3/util/Log.h>
#include <indexlib/common_define.h>
#include <indexlib/config/index_partition_options.h>
#include <autil/legacy/json.h>
#include <autil/legacy/jsonizable.h>
#include <ha3/config/AnomalyProcessConfig.h>
#include <ha3/config/FinalSorterConfig.h>
#include <suez/turing/expression/function/FuncConfig.h>
#include <suez/turing/common/CavaConfig.h>

IE_NAMESPACE_BEGIN(config)
class IndexPartitionOptions;
IE_NAMESPACE_END(config)

BEGIN_HA3_NAMESPACE(config);

class ClusterConfigInfo;
class AggSamplerConfigInfo;
class SearchOptimizerConfig;
class RankProfileConfig;
class SummaryProfileConfig;
class SearcherCacheConfig;
class ServiceDegradationConfig;
class IndexOptionConfig;
class TuringOptionsInfo;

// parser for XXX_cluster.json
class ClusterConfigParser
{
public:
    enum ParseResult {
        PR_OK,
        PR_FAIL,
        PR_SECTION_NOT_EXIST
    };
public:
    ClusterConfigParser(const std::string &basePath, 
                        const std::string &clusterFilePath);
    ~ClusterConfigParser();
private:
    ClusterConfigParser(const ClusterConfigParser &);
    ClusterConfigParser& operator=(const ClusterConfigParser &);
public:
    // get section config
    ParseResult getClusterConfigInfo(ClusterConfigInfo &clusterConfigInfo) const;
    ParseResult getFuncConfig(suez::turing::FuncConfig &funcConfig) const;
    ParseResult getAggSamplerConfigInfo(AggSamplerConfigInfo &aggSamplerInfo) const;
    ParseResult getFinalSorterConfig(FinalSorterConfig &finalSorterConfig) const;
    ParseResult getSearchOptimizerConfig(SearchOptimizerConfig &searchOptimizerConfig) const;
    ParseResult getRankProfileConfig(RankProfileConfig &rankProfileConfig) const;
    ParseResult getSummaryProfileConfig(
            SummaryProfileConfig &summaryProfileConfig) const;
    ParseResult getSearcherCacheConfig(
            SearcherCacheConfig &searcherCacheConfig) const;
    ParseResult getServiceDegradationConfig(
            ServiceDegradationConfig &serviceDegradationConfig) const;
    ParseResult getAnomalyProcessConfig(
            AnomalyProcessConfig &anomalyProcessConfig) const;
    ParseResult getAnomalyProcessConfigT(
            AnomalyProcessConfig &anomalyProcessConfig) const;
    ParseResult getSqlAnomalyProcessConfig(
            AnomalyProcessConfig &anomalyProcessConfig) const;
    ParseResult getCavaConfig(suez::turing::CavaConfig &cavaConfig) const;
    ParseResult getTuringOptionsInfo(TuringOptionsInfo &turingOptionsInfo) const;
private:
    bool parse(const std::string &jsonFile, 
               autil::legacy::json::JsonMap &jsonMap) const;
    bool merge(autil::legacy::json::JsonMap &jsonMap) const;
    bool mergeJsonMap(const autil::legacy::json::JsonMap &src,
                      autil::legacy::json::JsonMap &dst) const;
    bool mergeJsonArray(autil::legacy::json::JsonArray &dst) const;
    bool mergeWithInherit(autil::legacy::json::JsonMap &jsonMap) const;

    template <typename T>
    ParseResult doGetConfig(const std::string &configFile, 
                            const std::string &sectionName, 
                            T &config) const;
private:
    std::string _basePath;
    std::string _clusterFilePath;
    mutable std::set<uint64_t> _contentSignatures; 
    mutable std::vector<std::string> _configFileNames; // for inherit loop detection
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(ClusterConfigParser);
////////////////////////////////////////////////////

END_HA3_NAMESPACE(config);

#endif //ISEARCH_CLUSTERCONFIGPARSER_H
