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
#include "sql/ops/scan/udf_to_query/ContainToQueryImpl.h"

#include <iosfwd>
#include <map>
#include <memory>
#include <stdint.h>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "ha3/common/MultiTermQuery.h"
#include "ha3/common/NumberQuery.h"
#include "ha3/common/NumberTerm.h"
#include "ha3/common/Term.h"
#include "ha3/common/TermQuery.h"
#include "ha3/isearch.h"
#include "indexlib/index/common/Types.h"
#include "sql/common/IndexInfo.h"
#include "sql/common/Log.h"
#include "sql/ops/scan/udf_to_query/UdfToQuery.h"
#include "suez/turing/expression/util/IndexInfos.h"

namespace isearch {
namespace common {
class Query;
} // namespace common
} // namespace isearch

using namespace std;
using namespace autil;

using namespace isearch::common;

namespace sql {
AUTIL_LOG_SETUP(sql, ContainToQueryImpl);

Query *ContainToQueryImpl::toQuery(const UdfToQueryParam &condParam,
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
        const suez::turing::IndexInfo *indexInfo
            = condParam.indexInfos->getIndexInfo(columnName.c_str());
        if (indexInfo == nullptr) {
            SQL_LOG(DEBUG, "[%s] not index", columnName.c_str());
            return nullptr;
        }
        indexName = indexInfo->indexName;
        indexType = indexTypeToString(indexInfo->indexType);
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

Query *ContainToQueryImpl::genQuery(const UdfToQueryParam &condParam,
                                    const std::string &columnName,
                                    const vector<std::string> &termVec,
                                    const std::string &indexName,
                                    const std::string &indexType) {
    if (termVec.empty()) {
        SQL_LOG(DEBUG, "term size is empty");
        return nullptr;
    }
    if (indexType == "number") {
        if (termVec.size() == 1) {
            int64_t numVal = 0;
            const auto &termStr = termVec[0];
            if (StringUtil::numberFromString(termStr, numVal)) {
                NumberTerm term {numVal, indexName.c_str(), RequiredFields()};
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

std::string ContainToQueryImpl::indexTypeToString(uint32_t indexType) {
    switch (indexType) {
    case it_text:
        return "text";
    case it_pack:
        return "pack";
    case it_expack:
        return "expack";
    case it_string:
        return "string";
    case it_enum:
        return "enum";
    case it_property:
        return "property";
    case it_number:
    case it_number_int8:
    case it_number_uint8:
    case it_number_int16:
    case it_number_uint16:
    case it_number_int32:
    case it_number_uint32:
    case it_number_int64:
    case it_number_uint64:
        return "number";
    case it_primarykey64:
    case it_primarykey128:
        return "pk";
    case it_trie:
        return "trie";
    case it_spatial:
        return "spatial";
    case it_kv:
        return "kv";
    case it_kkv:
        return "kkv";
    case it_customized:
        return "customized";
    case it_date:
        return "date";
    case it_range:
        return "range";
    default:
        return "unknown";
    }
}

} // namespace sql
