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
#include "ha3/turing/searcher/SearcherServiceSnapshot.h"

#include <assert.h>
#include <iosfwd>
#include <map>
#include <utility>
#include <vector>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/HashAlgorithm.h"
#include "autil/StringUtil.h"
#include "autil/legacy/any.h"
#include "autil/legacy/jsonizable.h"
#include "autil/legacy/exception.h"
#include "aios/network/gig/multi_call/common/ControllerParam.h"
#include "aios/network/gig/multi_call/common/TopoInfoBuilder.h"
#include "autil/EnvUtil.h"
#include "suez/sdk/KMonitorMetaInfo.h"
#include "suez/sdk/ServiceInfo.h"
#include "suez/sdk/IndexProvider.h"
#include "suez/turing/common/BizInfo.h"
#include "suez/turing/common/RunIdAllocator.h"
#include "suez/turing/common/VersionConfig.h"
#include "suez/turing/common/WorkerParam.h"
#include "suez/turing/search/Biz.h"
#include "suez/turing/search/ServiceSnapshot.h"

#include "ha3/common/VersionCalculator.h"
#include "ha3/util/TypeDefine.h"
#include "ha3/turing/searcher/DefaultAggBiz.h"
#include "ha3/turing/searcher/DefaultSqlBiz.h"
#include "ha3/turing/searcher/ParaSearchBiz.h"
#include "ha3/turing/searcher/SearcherBiz.h"
#include "ha3/turing/searcher/SearcherContext.h"
#include "ha3/turing/searcher/BasicSearcherContext.h"
#include "ha3/turing/common/ModelBiz.h"
#include "ha3/util/EnvParser.h"
#include "autil/Log.h"
#include "autil/RangeUtil.h"
#include "ha3/version.h"

using namespace suez;
using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace suez::turing;
using namespace isearch::common;
using namespace isearch::sql;
using namespace isearch::util;

namespace isearch {
namespace turing {
AUTIL_LOG_SETUP(ha3, SearcherServiceSnapshot);

SearcherServiceSnapshot::SearcherServiceSnapshot() {
    _enableSql = false;
}

SearcherServiceSnapshot::~SearcherServiceSnapshot() {
}

suez::BizMetas SearcherServiceSnapshot::addExtraBizMetas(
        const suez::BizMetas &currentBizMetas) const
{
    if (currentBizMetas.size() <= 0) {
        return currentBizMetas;
    }
    auto copyMetas = currentBizMetas;
    auto it = copyMetas.find(DEFAULT_BIZ_NAME);
    if (it != copyMetas.end() && _basicTuringBizNames.count(DEFAULT_BIZ_NAME) == 0) {
        if (DefaultAggBiz::isSupportAgg(_defaultAggStr)) {
            copyMetas[HA3_DEFAULT_AGG] = it->second;
            auto customBizInfo = it->second.getCustomBizInfo() ;
            customBizInfo[BIZ_TYPE] = autil::legacy::Any(string(AGG_BIZ_TYPE));
            copyMetas[HA3_DEFAULT_AGG].setCustomBizInfo(customBizInfo);
            AUTIL_LOG(INFO, "default agg use [%s]", _defaultAggStr.c_str());
        }
        const string &disableSqlStr = autil::EnvUtil::getEnv("disableSql", "");
        if (disableSqlStr != "true") {
            copyMetas[DEFAULT_SQL_BIZ_NAME] = it->second;
            auto customBizInfo = it->second.getCustomBizInfo() ;
            customBizInfo[BIZ_TYPE] = autil::legacy::Any(string(SQL_BIZ_TYPE));
            copyMetas[DEFAULT_SQL_BIZ_NAME].setCustomBizInfo(customBizInfo);
            AUTIL_LOG(INFO, "add default sql biz.");
            _enableSql = true;
        }
        vector<string> paraWaysVec;
        if (EnvParser::parseParaWays(_paraWaysStr, paraWaysVec)) {
            for (const auto &paraVal : paraWaysVec) {
                copyMetas[HA3_PARA_SEARCH_PREFIX + paraVal] = it->second;
                auto customBizInfo = it->second.getCustomBizInfo() ;
                customBizInfo[BIZ_TYPE] = autil::legacy::Any(string(PARA_BIZ_TYPE));
                copyMetas[HA3_PARA_SEARCH_PREFIX + paraVal].setCustomBizInfo(customBizInfo);
            }
            AUTIL_LOG(INFO, "para search use [%s]", _paraWaysStr.c_str());
        }
    }
    bool onlySql = autil::EnvUtil::getEnv<bool>("onlySql", false);
    if (onlySql) {
        BizMetas sqlMetas;
        auto it = copyMetas.find(DEFAULT_SQL_BIZ_NAME);
        if (it != copyMetas.end()) {
            sqlMetas[DEFAULT_SQL_BIZ_NAME] = it->second;
            AUTIL_LOG(INFO, "remove other bizs, only use defalut sql biz.");
        }
        copyMetas = sqlMetas;
    }
    return copyMetas;
}

void SearcherServiceSnapshot::calculateBizMetas(
        const suez::BizMetas &currentBizMetas,
        ServiceSnapshotBase *oldSnapshot, suez::BizMetas &toUpdate,
        suez::BizMetas &toKeep)
{
    const suez::BizMetas &metas = addExtraBizMetas(currentBizMetas);
    ServiceSnapshot::calculateBizMetas(metas, oldSnapshot, toUpdate, toKeep);
}

BizBasePtr SearcherServiceSnapshot::createBizBase(const string &bizName,
        const suez::BizMeta &bizMeta)
{
    BizPtr bizPtr;
    const auto &customBizInfo = bizMeta.getCustomBizInfo();
    auto iter = customBizInfo.find(BIZ_TYPE);
    if (iter != customBizInfo.end()) {
        if (autil::legacy::json::IsJsonString(iter->second)) {
            std::string bizType = *autil::legacy::AnyCast<std::string>(&iter->second);
            if (bizType == MODEL_BIZ_TYPE) {
                return BizPtr(new ModelBiz());
            } else if (bizType == SQL_BIZ_TYPE) {
                return BizPtr(new DefaultSqlBiz(this, &_ha3BizMeta));
            } else if (bizType == AGG_BIZ_TYPE) {
                return BizPtr(new DefaultAggBiz(_defaultAggStr));
            } else if (bizType == PARA_BIZ_TYPE) {
                return BizPtr(new ParaSearchBiz());
            }
        }
    }
    if (_basicTuringBizNames.count(bizName) == 1) {
        return BizPtr(new Biz());
    }
    return BizPtr(new SearcherBiz());
}

bool SearcherServiceSnapshot::doInit(const SnapshotBaseOptions &options,
                                     ServiceSnapshotBase *oldSnapshot) {
    return true;
}

std::string SearcherServiceSnapshot::getBizName(
        const std::string &bizName,
        const suez::BizMeta &bizMeta) const
{
    const auto &customBizInfo = bizMeta.getCustomBizInfo();
    auto iter = customBizInfo.find(BIZ_TYPE);
    if (iter != customBizInfo.end()) {
        if (autil::legacy::json::IsJsonString(iter->second)) {
            std::string bizType = *autil::legacy::AnyCast<std::string>(&iter->second);
            if (bizType == MODEL_BIZ_TYPE) {
                return bizName;
            }
        }
    }
    return getZoneBizName(bizName);
}

SearchContext *SearcherServiceSnapshot::doCreateContext(
        const SearchContextArgs &args,
        const GraphRequest *request,
        GraphResponse *response)
{
    string bizName = request->bizname();
    const auto &bizMetas = args.serviceSnapshot->getBizMetas();
    auto iter = bizMetas.find(bizName);
    if (iter != bizMetas.end()) {
        const auto &bizMeta = iter->second;
        const auto &customBizInfo = bizMeta.getCustomBizInfo();
        auto iter = customBizInfo.find(BIZ_TYPE);
        if (iter != customBizInfo.end()) {
            if (autil::legacy::json::IsJsonString(iter->second)) {
                std::string bizType = *autil::legacy::AnyCast<std::string>(&iter->second);
                if (bizType == MODEL_BIZ_TYPE) {
                    return new GraphSearchContext(args, request, response);
                }
            }
        }
    }
    if (_basicTuringBizNames.count(bizName) == 1) {
        return new GraphSearchContext(args, request, response);
    } else if (bizName.find(HA3_USER_SEARCH) != string::npos) {
        return new GraphSearchContext(args, request, response);
    } else if (StringUtil::endsWith(bizName, HA3_DEFAULT_AGG)) {
        return new BasicSearcherContext(args, request, response);
    } else {
        return new SearcherContext(args, request, response);
    }
}

bool SearcherServiceSnapshot::doWarmup() {
    if (_bizMap.empty()) {
        return true;
    }

    if (_enableSql) {
        for (auto biz : _bizMap) {
            auto sqlBiz = dynamic_cast<DefaultSqlBiz*>(biz.second.get());
            if (sqlBiz && !sqlBiz->updateNavi()) {
                AUTIL_LOG(ERROR, "update navi failed");
                return false;
            }
        }
        bool onlySql = autil::EnvUtil::getEnv<bool>("onlySql", false);
        if (onlySql) {
            auto searchService = getSearchService();
            if (searchService) {
                searchService->disableSnapshotLog();
                AUTIL_LOG(INFO, "only sql enabled, disable search service snapshot log for turing");
            }
        }        
    }
    return true;
}

} // namespace turing
} // namespace isearch
