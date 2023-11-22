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
#include "build_service_tasks/doc_reclaim/IndexReclaimConfigMaker.h"

#include <cstddef>
#include <ext/alloc_traits.h>
#include <memory>
#include <set>
#include <utility>

#include "alog/Logger.h"
#include "autil/Span.h"
#include "autil/StringUtil.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/json.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "autil/legacy/legacy_jsonizable_dec.h"
#include "beeper/beeper.h"
#include "build_service/common/BeeperCollectorDefine.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;

using namespace indexlib::merger;

namespace build_service { namespace task_base {
BS_LOG_SETUP(task_base, IndexReclaimConfigMaker);

IndexReclaimConfigMaker::IndexReclaimConfigMaker(const string& clusterName)
    : _clusterName(clusterName)
    , _msgCount(0)
    , _matchCount(0)
    , _invalidMsgCount(0)
{
}

IndexReclaimConfigMaker::~IndexReclaimConfigMaker() {}

void IndexReclaimConfigMaker::parseOneMessage(const string& msgData)
{
    _msgCount++;
    try {
        Any any = ParseJson(msgData);
        JsonMap jsonMap = AnyCast<JsonMap>(any);
        auto iter = jsonMap.find("cluster_name");
        if (iter == jsonMap.end()) {
            _invalidMsgCount++;
            BS_LOG(ERROR, "invalid reclaim msg [%s], no cluster_name", msgData.c_str());
            return;
        }
        string clusterNameStr = AnyCast<string>(iter->second);
        if (clusterNameStr != _clusterName) {
            return;
        }
        IndexReclaimerParamPtr param(new IndexReclaimerParam);
        FromJson(*param, any);
        if (param->GetReclaimOperator() != IndexReclaimerParam::AND_RECLAIM_OPERATOR) {
            const string& name = param->GetReclaimIndex();
            auto& termParamVec = _singleTermParams[name];
            termParamVec.push_back(param);
        } else {
            _andOperateParams.push_back(param);
        }
        _matchCount++;
    } catch (const autil::legacy::ExceptionBase& e) {
        BS_LOG(ERROR, "parse reclaim msg [%s] failed, exception [%s]", msgData.c_str(), e.what());
        _invalidMsgCount++;
    }
}

string IndexReclaimConfigMaker::makeReclaimParam()
{
    BS_LOG(INFO, "total msg [%ld], invalid msg [%ld], match msg [%ld]", _msgCount, _invalidMsgCount, _matchCount);

    string msg = "cluster [" + _clusterName +
                 "] parse reclaim message, "
                 "total message [" +
                 StringUtil::toString(_msgCount) +
                 "], "
                 "invalid message [" +
                 StringUtil::toString(_invalidMsgCount) +
                 "], "
                 "match message [" +
                 StringUtil::toString(_matchCount) + "]";
    BEEPER_REPORT(WORKER_STATUS_COLLECTOR_NAME, msg);

    vector<IndexReclaimerParamPtr> params;
    for (auto& iter : _singleTermParams) {
        const vector<IndexReclaimerParamPtr>& paramVec = iter.second;
        set<string> termSet;
        for (size_t i = 0; i < paramVec.size(); i++) {
            const vector<string>& terms = paramVec[i]->GetReclaimTerms();
            termSet.insert(terms.begin(), terms.end());
        }
        vector<string> finalTerms(termSet.begin(), termSet.end());
        IndexReclaimerParamPtr singleParam(new IndexReclaimerParam);
        singleParam->SetReclaimIndex(iter.first);
        singleParam->SetReclaimTerms(finalTerms);
        params.push_back(singleParam);
    }
    params.insert(params.end(), _andOperateParams.begin(), _andOperateParams.end());
    return ToJsonString(params);
}

}} // namespace build_service::task_base
