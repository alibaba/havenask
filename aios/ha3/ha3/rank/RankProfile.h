#ifndef ISEARCH_RANKPROFILE_H
#define ISEARCH_RANKPROFILE_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <build_service/plugin/PlugInManager.h>
#include <ha3/util/NumericLimits.h>
#include <autil/StringUtil.h>
#include <ha3/rank/GlobalMatchData.h>
#include <suez/turing/expression/util/TableInfo.h>
#include <suez/turing/expression/util/FieldBoostTable.h>
#include <ha3/search/QueryExecutor.h>
#include <suez/turing/expression/framework/RankAttributeExpression.h>
#include <ha3/config/ResourceReader.h>
#include <ha3/config/RankProfileInfo.h>
#include <suez/turing/expression/cava/common/CavaPluginManager.h>
#include <ha3/common/Request.h>

BEGIN_HA3_NAMESPACE(rank);

#define FIELD_BOOST_TOKENIZER "."

class RankProfile
{
public:
    RankProfile(const std::string &profileName);
    RankProfile(const std::string &profileName,
                const config::ScorerInfos& scorerInfos);
    RankProfile(const config::RankProfileInfo &info);
    ~RankProfile();

// for RankProfileManager:
public:
    bool init(build_service::plugin::PlugInManagerPtr &plugInManagerPtr,
              const config::ResourceReaderPtr &resourceReaderPtr,
              const suez::turing::CavaPluginManagerPtr &cavaPluginManagerPtr,
              kmonitor::MetricsReporter *metricsReporter);

    /* merge the 'FieldBoostTable' in 'RankProfile' config and 'TableInfo' config. */
    void mergeFieldBoostTable(const suez::turing::TableInfo &tableInfo);
    void addScorerInfo(const config::ScorerInfo &scorerInfo);

// for MatchDocSearcher:
public:
    suez::turing::RankAttributeExpression* createRankExpression(
            autil::mem_pool::Pool *pool) const;
    uint32_t getRankSize(uint32_t phase) const;
    uint32_t getTotalRankSize(uint32_t phase) const;
    int getPhaseCount() const;
    bool getScorerInfo(uint32_t idx, config::ScorerInfo &scorerInfo) const;
    bool getScorers(std::vector<suez::turing::Scorer *> &scores, uint32_t scorePos) const;
    const std::string& getProfileName() const;

    const suez::turing::FieldBoostTable *getFieldBoostTable() const{
        return &_fieldBoostTable;
    }

    void getCavaScorerCodes(std::vector<std::string> &moduleNames,
                            std::vector<std::string> &fileNames,
                            std::vector<std::string> &codes,
                            common::Request *request) const;

private:
    suez::turing::Scorer* createScorer(const config::ScorerInfo &scorerInfo,
            build_service::plugin::PlugInManagerPtr &plugInManagerPtr,
            const config::ResourceReaderPtr &resourceReaderPtr,
            const suez::turing::CavaPluginManagerPtr &cavaPluginManagerPtr,
            kmonitor::MetricsReporter *metricsReporter);
private:
    std::string _profileName;
    config::ScorerInfos _scorerInfos;
    std::map<std::string, uint32_t> _fieldBoostInfo;
    std::vector<suez::turing::Scorer *> _scorers;
    suez::turing::FieldBoostTable _fieldBoostTable;
private:
    friend class RankProfileTest;
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(rank);

#endif //ISEARCH_RANKPROFILE_H
