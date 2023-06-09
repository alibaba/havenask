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
#include "ha3/turing/qrs/LocalServiceSnapshot.h"

#include <map>

#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "ha3/turing/common/Ha3BizMeta.h"
#include "ha3/turing/common/QrsModelBiz.h"
#include "ha3/turing/qrs/QrsBiz.h"
#include "ha3/turing/qrs/QrsSqlBiz.h"
#include "ha3/turing/searcher/BasicSearcherContext.h"
#include "ha3/turing/searcher/DefaultSqlBiz.h"
#include "ha3/turing/searcher/SearcherBiz.h"
#include "ha3/turing/searcher/SearcherContext.h"
#include "ha3/util/TypeDefine.h"
#include "suez/turing/common/BizInfo.h"
#include "suez/turing/search/Biz.h"

using namespace suez;
using namespace std;
using namespace autil;
using namespace suez::turing;
using namespace isearch::common;
namespace isearch {
namespace turing {
AUTIL_LOG_SETUP(ha3, LocalServiceSnapshot);

LocalServiceSnapshot::LocalServiceSnapshot(bool enableSql)
    : QrsServiceSnapshot(enableSql) {}

LocalServiceSnapshot::~LocalServiceSnapshot() {}

suez::BizMetas LocalServiceSnapshot::addExtraBizMetas(const suez::BizMetas &currentBizMetas) const {
    const BizMetas &bizMetas = QrsServiceSnapshot::addExtraBizMetas(currentBizMetas);
    BizMetas addMetas;
    for (auto &bizMeta : bizMetas) {
        autil::legacy::json::JsonMap customBizInfo = bizMeta.second.getCustomBizInfo();
        string bizType;
        auto iter = customBizInfo.find(BIZ_TYPE);
        if (iter != customBizInfo.end()) {
            if (autil::legacy::json::IsJsonString(iter->second)) {
                bizType = *autil::legacy::AnyCast<std::string>(&iter->second);
            }
        }
        if (MODEL_BIZ_TYPE == bizType || _basicTuringBizNames.count(bizMeta.first) == 1) {
            addMetas[bizMeta.first] = bizMeta.second;
            AUTIL_LOG(INFO, "add biz [%s], type [%s].", bizMeta.first.c_str(), bizType.c_str());
            continue;
        }
        string qrsBizName = bizMeta.first + HA3_LOCAL_QRS_BIZ_NAME_SUFFIX;
        addMetas[qrsBizName] = bizMeta.second;
        AUTIL_LOG(INFO, "add biz [%s], type [%s].", qrsBizName.c_str(), bizType.c_str());
        if (SQL_BIZ_TYPE == bizType) {
            continue;
        }
        customBizInfo[BIZ_TYPE] = autil::legacy::Any(string(SEARCHER_BIZ_TYPE));
        addMetas[bizMeta.first] = bizMeta.second;
        addMetas[bizMeta.first].setCustomBizInfo(customBizInfo);
        // TODO to support default agg/para search biz
        bizType = "";
        autil::legacy::json::ToString(customBizInfo[BIZ_TYPE], bizType);
        AUTIL_LOG(INFO, "add biz [%s], type [%s].", bizMeta.first.c_str(), bizType.c_str());
    }
    return addMetas;
}

BizBasePtr LocalServiceSnapshot::createBizBase(const std::string &bizName,
                                       const suez::BizMeta &bizMeta)
{
    const auto &customBizInfo = bizMeta.getCustomBizInfo();
    auto iter = customBizInfo.find(BIZ_TYPE);
    if (iter != customBizInfo.end()) {
        if (autil::legacy::json::IsJsonString(iter->second)) {
            std::string bizType = *autil::legacy::AnyCast<std::string>(&iter->second);
            if (bizType == MODEL_BIZ_TYPE) {
                return BizPtr(new ModelBiz());
            } else if (bizType == SQL_BIZ_TYPE) {
                return BizPtr(new QrsSqlBiz(this, &_ha3BizMeta));
            } else if (bizType == SEARCHER_SQL_BIZ_TYPE) {
                return BizPtr(new isearch::sql::DefaultSqlBiz(this, &_ha3BizMeta));
            } else if (bizType == SEARCHER_BIZ_TYPE) {
                return BizPtr(new SearcherBiz());
            }
        }
    }
    if (_basicTuringBizNames.count(bizName) == 1) {
        return BizPtr(new Biz());
    }
    return BizPtr(new QrsBiz());
}

SearchContext *LocalServiceSnapshot::doCreateContext(const SearchContextArgs &args,
                                                     const GraphRequest *request,
                                                     GraphResponse *response) {
    string bizName = request->bizname();
    const auto &bizMetas = args.serviceSnapshot->getAllBizMetas();
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
    } else if (StringUtil::endsWith(bizName, HA3_LOCAL_QRS_BIZ_NAME_SUFFIX)) {
        return new GraphSearchContext(args, request, response);
    } else {
        return new SearcherContext(args, request, response);
    }
}

} // namespace turing
} // namespace isearch
