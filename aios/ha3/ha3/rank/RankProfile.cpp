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
#include "ha3/rank/RankProfile.h"

#include <cstddef>
#include <memory>
#include <set>
#include <utility>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/StringTokenizer.h"
#include "autil/legacy/base64.h"
#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/PoolBase.h"
#include "build_service/plugin/Module.h"
#include "build_service/plugin/ModuleInfo.h"
#include "build_service/plugin/PlugInManager.h"
#include "suez/turing/common/CommonDefine.h"
#include "suez/turing/expression/cava/common/CavaPluginManager.h"
#include "suez/turing/expression/framework/RankAttributeExpression.h"
#include "suez/turing/expression/plugin/Scorer.h"
#include "suez/turing/expression/plugin/ScorerModuleFactory.h"
#include "suez/turing/expression/util/FieldBoostTable.h"
#include "suez/turing/expression/util/FieldInfos.h"
#include "suez/turing/expression/util/IndexInfos.h"
#include "suez/turing/expression/util/TableInfo.h"

#include "ha3/common/ConfigClause.h"
#include "ha3/common/Request.h"
#include "ha3/config/RankProfileInfo.h"
#include "ha3/config/ResourceReader.h"
#include "ha3/isearch.h"
#include "ha3/rank/CavaScorerAdapter.h"
#include "ha3/rank/DefaultScorer.h"
#include "ha3/rank/GeneralScorer.h"
#include "ha3/rank/RecordInfoScorer.h"
#include "ha3/rank/TestScorer.h"

#ifndef AIOS_OPEN_SOURCE
#include "ha3/rank/AdapterScorer.h"
#include "ha3/rank/ScorerAudition.h"
#endif

#include "ha3/search/SearchPluginResource.h"
#include "autil/Log.h"

namespace kmonitor {
class MetricsReporter;
}  // namespace kmonitor

using namespace std;
using namespace autil;
using namespace build_service::plugin;
using namespace suez::turing;
using namespace isearch::config;
using namespace isearch::search;
using namespace isearch::common;

namespace isearch {
namespace rank {
AUTIL_LOG_SETUP(ha3, RankProfile);

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

bool RankProfile::init(PlugInManager *plugInManagerPtr,
                       const config::ResourceReaderPtr &resourceReaderPtr,
                       const CavaPluginManager *cavaPluginManager,
                       kmonitor::MetricsReporter *metricsReporter)
{
    for (ScorerInfos::iterator it = _scorerInfos.begin();
         it != _scorerInfos.end(); it ++)
    {
        Scorer* scorer = createScorer(*it, plugInManagerPtr,
                resourceReaderPtr, cavaPluginManager, metricsReporter);
        if (!scorer) {
            AUTIL_LOG(ERROR, "Create scorer failed");
            return false;
        } else {
            AUTIL_LOG(INFO, "Create scorer success");
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
    AUTIL_LOG(TRACE3, "set scorer [%s] to RankAttributeExpression",
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
                                  PlugInManager *plugInManagerPtr,
                                  const config::ResourceReaderPtr &resourceReaderPtr,
                                  const CavaPluginManager *cavaPluginManager,
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
        } else if (scorerInfo.scorerName == "CavaScorerAdapter" && cavaPluginManager) {
            scorer = new CavaScorerAdapter(scorerInfo.parameters,
                    cavaPluginManager, metricsReporter);
        } else if(scorerInfo.scorerName == DEFAULT_DEBUG_SCORER) {
            scorer = new RecordInfoScorer();
        } else if (scorerInfo.scorerName == "AdapterScorer") {
#ifndef AIOS_OPEN_SOURCE
            scorer = new AdapterScorer("AdapterScorer");
#else
            return nullptr;
#endif
        } else if(scorerInfo.scorerName == "ScorerAudition") {
#ifndef AIOS_OPEN_SOURCE
            scorer = new ScorerAudition(scorerInfo.parameters, scorerInfo.scorerName, resourceReaderPtr.get());
#else
            return nullptr;
#endif
        }
        else {
            AUTIL_LOG(ERROR, "failed to create scorer[%s] with BuildInModule",
                    scorerInfo.scorerName.c_str());
        }
    } else {
        Module *module = plugInManagerPtr->getModule(scorerInfo.moduleName);
        if (!module) {
            AUTIL_LOG(TRACE3, "Get module [%s] failed1. module[%p]",
                    scorerInfo.moduleName.c_str(), module);
            return NULL;
        }
        ScorerModuleFactory *factory =
            (ScorerModuleFactory*)module->getModuleFactory();
        scorer = factory->createScorer(scorerInfo.scorerName.c_str(),
                scorerInfo.parameters, resourceReaderPtr.get(),
                cavaPluginManager);
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
            AUTIL_LOG(WARN, "Field Boost Config Error, str:%s, right format example: 'indexName.fieldName'",
                key.c_str());
            continue;
        }

        const string &indexName = st[0];
        const string &fieldName = st[1];
        uint32_t boostValue = it->second;

        const IndexInfo *indexInfo = indexInfos->getIndexInfo(indexName.c_str());
        if (NULL == indexInfo) {
            AUTIL_LOG(WARN, "the IndexName:%s does not exist", indexName.c_str());
            continue;
        }

        int32_t fieldPosition = indexInfo->getFieldPosition(fieldName.c_str());
        if (-1 == fieldPosition) {
            AUTIL_LOG(WARN, "the fieldName:%s does not exist", fieldName.c_str());
            continue;
        }
        _fieldBoostTable.setFieldBoostValue(indexInfo->indexId, fieldPosition,
                (fieldboost_t)boostValue);
    }
}

} // namespace rank
} // namespace isearch
