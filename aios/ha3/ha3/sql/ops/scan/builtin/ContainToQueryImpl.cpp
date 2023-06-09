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
#include "ha3/sql/ops/scan/builtin/ContainToQueryImpl.h"

#include <iosfwd>
#include <map>
#include <memory>
#include <stdint.h>
#include <string>
#include <utility>
#include <vector>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/StringUtil.h"
#include "ha3/common/MultiTermQuery.h"
#include "ha3/common/NumberQuery.h"
#include "ha3/common/TermQuery.h"
#include "ha3/common/NumberTerm.h"
#include "ha3/common/Term.h"
#include "ha3/isearch.h"
#include "suez/turing/expression/util/IndexInfos.h"
#include "ha3/sql/common/IndexInfo.h"
#include "ha3/sql/common/Log.h"
#include "ha3/sql/common/common.h"
#include "ha3/sql/ops/scan/Ha3ScanConditionVisitorParam.h"
#include "ha3/sql/ops/scan/ScanUtil.h"
#include "rapidjson/document.h"
#include "rapidjson/rapidjson.h"

using namespace std;
using namespace autil;

using namespace isearch::common;

namespace isearch {
namespace sql {
AUTIL_LOG_SETUP(sql, ContainToQueryImpl);

Query *ContainToQueryImpl::toQuery(
        const Ha3ScanConditionVisitorParam &condParam,
        const std::string &columnName,
        const vector<std::string> &termVec,
        bool extractIndexInfoFromTable) {
    string indexName;
    string indexType;
    if (condParam.forbidIndexs != nullptr && condParam.forbidIndexs->count(columnName) > 0) {
        SQL_LOG(DEBUG, "index [%s] is forbiden ", columnName.c_str());
        return nullptr;
    }
    if (extractIndexInfoFromTable) {
        const suez::turing::IndexInfo *indexInfo = condParam.indexInfos->getIndexInfo(columnName.c_str());
        if (indexInfo == nullptr) {
            SQL_LOG(DEBUG, "[%s] not index", columnName.c_str());
            return nullptr;
        }
        indexName = indexInfo->indexName;
        indexType = ScanUtil::indexTypeToString(indexInfo->indexType);
    } else {
        auto iter = condParam.indexInfo->find(columnName);
        if (iter == condParam.indexInfo->end()) {
            SQL_LOG(DEBUG, "[%s] not index", columnName.c_str());
            return nullptr;
        }
        indexName = iter->second.name;
        indexType = iter->second.type;
    }
    return genQuery(condParam, columnName, termVec, indexName, indexType);

}

Query *ContainToQueryImpl::genQuery(
    const Ha3ScanConditionVisitorParam &condParam,
    const std::string &columnName,
    const vector<std::string> &termVec,
    const std::string &indexName,
    const std::string &indexType)
{
    if (termVec.empty()) {
        SQL_LOG(DEBUG, "term size is empty");
        return nullptr;
    }
    if (indexType == "number") {
        if (termVec.size() == 1) {
            int64_t numVal = 0;
            const auto &termStr = termVec[0];
            if (StringUtil::numberFromString(termStr, numVal)) {
                NumberTerm term{numVal, indexName.c_str(), RequiredFields()};
                return new NumberQuery(term, "");
            } else {
                SQL_LOG(DEBUG, "parse term [%s] to number failed", termStr.c_str());
                return nullptr;
            }
        } else {
            std::unique_ptr<MultiTermQuery> query(new MultiTermQuery("", OP_OR));
            for (auto termStr : termVec) {
                int64_t numVal = 0;
                if (StringUtil::numberFromString(termStr, numVal)) {
                    TermPtr term(new NumberTerm(numVal, indexName.c_str(), RequiredFields()));
                    query->addTerm(term);
                } else {
                    SQL_LOG(DEBUG, "parse term [%s] to number failed", termStr.c_str());
                    return nullptr;
                }
            }
            return query.release();
        }
    } else {
        if (termVec.size() == 1) {
            Term term(termVec[0], indexName.c_str(), RequiredFields());
            return new TermQuery(term, "");
        } else {
            std::unique_ptr<MultiTermQuery> query(new MultiTermQuery("", OP_OR));
            for (auto termStr : termVec) {
                TermPtr term(new Term(termStr, indexName.c_str(), RequiredFields()));
                query->addTerm(term);
            }
            return query.release();
        }
    }
}

} // namespace sql
} // namespace isearch
