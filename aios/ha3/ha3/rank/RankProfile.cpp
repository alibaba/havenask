#include <ha3/rank/RankProfile.h>
#include <assert.h>
#include <build_service/plugin/PlugInManager.h>
#include <ha3/rank/DefaultScorer.h>
#include <ha3/rank/CavaScorerAdapter.h>
#include <ha3/rank/RecordInfoScorer.h>
#include <ha3/rank/TestScorer.h>
#include <build_service/plugin/Module.h>
#include <suez/turing/expression/plugin/ScorerModuleFactory.h>
#include <autil/StringTokenizer.h>
#include <ha3/rank/GeneralScorer.h>
#include <suez/turing/common/CommonDefine.h>

using namespace std;
using namespace autil;
using namespace build_service::plugin;
using namespace suez::turing;
USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(search);
USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(rank);
HA3_LOG_SETUP(rank, RankProfile);

RankProfile::RankProfile(const RankProfileInfo &info)
    : _profileName(info.rankProfileName)
    , _scorerInfos(info.scorerInfos)
    , _fieldBoostInfo(info.fieldBoostInfo)
{
}

RankProfile::RankProfile(const std::string &profileName)
    : _profileName(profileName)
{
}

RankProfile::RankProfile(const std::string &profileName,
                         const ScorerInfos &scorerInfos)
    : _profileName(profileName)
    , _scorerInfos(scorerInfos)
{
}

RankProfile::~RankProfile() {
    for (vector<Scorer*>::iterator it = _scorers.begin();
         it != _scorers.end(); it++)
    {
        if ((*it) != NULL) {
            (*it)->destroy();
        }
    }
}

bool RankProfile::init(PlugInManagerPtr &plugInManagerPtr,
                       const config::ResourceReaderPtr &resourceReaderPtr,
                       const CavaPluginManagerPtr &cavaPluginManagerPtr,
                       kmonitor::MetricsReporter *metricsReporter)
{
    for (ScorerInfos::iterator it = _scorerInfos.begin();
         it != _scorerInfos.end(); it ++)
    {
        Scorer* scorer = createScorer(*it, plugInManagerPtr,
                resourceReaderPtr, cavaPluginManagerPtr, metricsReporter);
        if (!scorer) {
            HA3_LOG(ERROR, "Create scorer failed");
            return false;
        } else {
            HA3_LOG(INFO, "Create scorer success");
            _scorers.push_back(scorer);
        }
    }

    return true;
}

RankAttributeExpression* RankProfile::createRankExpression(
        autil::mem_pool::Pool *pool) const
{
    if (_scorers.size() == 0) {
        return NULL;
    }
    Scorer *scorer = _scorers[0]->clone();
    RankAttributeExpression* expr = POOL_NEW_CLASS(pool,
            RankAttributeExpression, scorer, NULL);
    HA3_LOG(TRACE3, "set scorer [%s] to RankAttributeExpression",
            scorer->getScorerName().c_str());
    return expr;
}

bool RankProfile::getScorers(vector<Scorer*> &scorers, uint32_t scorePos) const{
    scorers.clear();
    for (uint32_t i = scorePos; i < _scorers.size(); ++i) {
        scorers.push_back(_scorers[i]->clone());
    }
    return true;
}

uint32_t RankProfile::getRankSize(uint32_t phase) const{
    if (phase >= _scorerInfos.size()) {
        return 0;
    }
    return _scorerInfos[phase].rankSize;
}

uint32_t RankProfile::getTotalRankSize(uint32_t phase) const {
    if (phase >= _scorerInfos.size()) {
        return 0;
    }
    return _scorerInfos[phase].totalRankSize;
}

int RankProfile::getPhaseCount() const{
    return _scorers.size();
}

bool RankProfile::getScorerInfo(uint32_t idx, ScorerInfo &scorerInfo) const{
    if (idx < _scorerInfos.size()) {
        scorerInfo = _scorerInfos[idx];
        return true;
    }
    return false;
}

void RankProfile::getCavaScorerCodes(std::vector<std::string> &moduleNames,
                                     std::vector<std::string> &fileNames,
                                     std::vector<std::string> &codes,
                                     common::Request *request) const
{
    if (!request) {
        return;
    }
    ConfigClause *configClause = request->getConfigClause();
    if (!configClause) return;
    set<string> codeKeys;
    codeKeys.insert(CAVA_DEFAULT_SCORER_CODE_KEY);
    for (auto &scorerInfo: _scorerInfos) {
        auto it = scorerInfo.parameters.find(CAVA_SCORER_CODE_KEY);
        if (it != scorerInfo.parameters.end()) {
            codeKeys.insert(it->second);
        }
    }

    for (auto &cavaScorerCodeKey : codeKeys) {
        auto &code = configClause->getKVPairValue(cavaScorerCodeKey);
        if (code.empty()) {
            continue;
        }
        moduleNames.push_back("scorerModule");
        fileNames.push_back(cavaScorerCodeKey);
        codes.push_back(autil::legacy::Base64DecodeFast(code));
    }
}

void RankProfile::addScorerInfo(const ScorerInfo &scorerInfo) {
    _scorerInfos.push_back(scorerInfo);
}

const std::string &RankProfile::getProfileName() const {
    return _profileName;
}

Scorer* RankProfile::createScorer(const ScorerInfo &scorerInfo,
                                  PlugInManagerPtr &plugInManagerPtr,
                                  const config::ResourceReaderPtr &resourceReaderPtr,
                                  const CavaPluginManagerPtr &cavaPluginManagerPtr,
                                  kmonitor::MetricsReporter *metricsReporter)
{
    Scorer *scorer = NULL;
    if (PlugInManager::isBuildInModule(scorerInfo.moduleName)) {
        if (scorerInfo.scorerName == "DefaultScorer") {
            scorer = new DefaultScorer();
        } else if (scorerInfo.scorerName == "GeneralScorer") {
            scorer = new GeneralScorer(scorerInfo.parameters,resourceReaderPtr.get());
        } else if (scorerInfo.scorerName == "TestScorer") {
            scorer = new TestScorer();
        } else if (scorerInfo.scorerName == "CavaScorerAdapter" && cavaPluginManagerPtr) {
            scorer = new CavaScorerAdapter(scorerInfo.parameters,
                    cavaPluginManagerPtr, metricsReporter);
        } else if(scorerInfo.scorerName == DEFAULT_DEBUG_SCORER) {
            scorer = new RecordInfoScorer();
        } else {
            HA3_LOG(ERROR, "failed to create scorer[%s] with BuildInModule",
                    scorerInfo.scorerName.c_str());
        }
    } else {
        Module *module = plugInManagerPtr->getModule(scorerInfo.moduleName);
        if (!module) {
            HA3_LOG(TRACE3, "Get module [%s] failed1. module[%p]",
                    scorerInfo.moduleName.c_str(), module);
            return NULL;
        }
        ScorerModuleFactory *factory =
            (ScorerModuleFactory*)module->getModuleFactory();
        scorer = factory->createScorer(scorerInfo.scorerName.c_str(),
                scorerInfo.parameters, resourceReaderPtr.get(),
                cavaPluginManagerPtr.get());
    }
    return scorer;
}

void RankProfile::mergeFieldBoostTable(const TableInfo &tableInfo)
{
    const IndexInfos *indexInfos = tableInfo.getIndexInfos();
    _fieldBoostTable = indexInfos->getFieldBoostTable();

    for (map<string, uint32_t>::const_iterator it = _fieldBoostInfo.begin();
         it != _fieldBoostInfo.end(); ++it)
    {
        //tokenize 'key'
        const string &key = it->first;
        const StringTokenizer st(key, FIELD_BOOST_TOKENIZER,
                           StringTokenizer::TOKEN_TRIM | StringTokenizer::TOKEN_IGNORE_EMPTY);
        if (st.getNumTokens() != 2) {
            HA3_LOG(WARN, "Field Boost Config Error, str:%s, right format example: 'indexName.fieldName'",
                key.c_str());
            continue;
        }

        const string &indexName = st[0];
        const string &fieldName = st[1];
        uint32_t boostValue = it->second;

        const IndexInfo *indexInfo = indexInfos->getIndexInfo(indexName.c_str());
        if (NULL == indexInfo) {
            HA3_LOG(WARN, "the IndexName:%s does not exist", indexName.c_str());
            continue;
        }

        int32_t fieldPosition = indexInfo->getFieldPosition(fieldName.c_str());
        if (-1 == fieldPosition) {
            HA3_LOG(WARN, "the fieldName:%s does not exist", fieldName.c_str());
            continue;
        }
        _fieldBoostTable.setFieldBoostValue(indexInfo->indexId, fieldPosition,
                (fieldboost_t)boostValue);
    }
}

END_HA3_NAMESPACE(rank);
