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
#include "ha3/sql/ops/tvf/builtin/RerankByQuotaTvfFunc.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <memory>
#include <queue>
#include <unordered_map>

#include "alog/Logger.h"
#include "autil/StringUtil.h"
#include "autil/legacy/exception.h"
#include "autil/mem_pool/Pool.h"
#include "autil/MultiValueCreator.h"
#include "ha3/sql/common/Log.h"
#include "ha3/sql/common/common.h"
#include "ha3/sql/ops/tvf/SqlTvfProfileInfo.h"
#include "ha3/sql/ops/tvf/TvfFunc.h"
#include "matchdoc/ValueType.h"
#include "navi/engine/CreatorRegistry.h"
#include "navi/engine/Resource.h"
#include "table/Column.h"
#include "table/ColumnSchema.h"
#include "table/ComparatorCreator.h"
#include "table/Row.h"
#include "table/ValueTypeSwitch.h"
#include "table/TableUtil.h"

namespace isearch {
namespace sql {

const SqlTvfProfileInfo rerankByQuotaTvfInfo("rerankByQuotaTvf", "rerankByQuotaTvf");
const std::string rerankByQuotaTvfDef = R"tvf_json(
{
  "function_version": 1,
  "function_name": "rerankByQuotaTvf",
  "function_type": "TVF",
  "is_deterministic": 1,
  "function_content_version": "json_default_0.1",
  "function_content": {
    "properties": {
      "enable_shuffle": false,
      "normal_scope": false
    },
    "prototypes": [
      {
        "params": {
          "scalars": [
             {
               "type":"string"
             },
             {
               "type":"string"
             },
             {
               "type":"string"
             },
             {
               "type":"string"
             },
             {
               "type":"string"
             },
             {
               "type":"string"
             },
             {
               "type":"string"
             },
             {
               "type":"string"
             }
         ],
          "tables": [
            {
              "auto_infer": true
            }
          ]
        },
        "returns": {
          "new_fields": [
          ],
          "tables": [
            {
              "auto_infer": true
            }
          ]
        }
      }
    ]
  }
}
)tvf_json";

RerankByQuotaTvfFunc::RerankByQuotaTvfFunc() :
    _perCategoryCount(0),
    _windowSize(0),
    _firstPhaseNeedMakeUp(false),
    _secondPhaseNeedMakeUp(false) {
}

RerankByQuotaTvfFunc::~RerankByQuotaTvfFunc() {}

bool RerankByQuotaTvfFunc::init(TvfFuncInitContext &context) {
    if (context.params.size() != 8u) {
        SQL_LOG(ERROR, "Param size [%ld] not equal 8", context.params.size());
        return false;
    }
    _quotaFieldStr = context.params[0];
    autil::StringUtil::split(_quotaFields, context.params[0], INPUT_VALUE_SEP);

    {
        _quotaFieldValueStr = context.params[1];
        auto&& tmps = autil::StringUtil::split(context.params[1], INPUT_VALUE_SEP);
        for (const std::string& tmp : tmps) {
            auto&& tmpInner = autil::StringUtil::split(tmp, INPUT_VALUE_INNER_SEP);
            if (tmpInner.size() != _quotaFields.size()) {
                SQL_LOG(ERROR, "Field [%s] num should be [%lu], actually is [%lu]",
                        tmp.c_str(), _quotaFields.size(), tmpInner.size());
                return false;
            }
            _quotaFieldValues.emplace_back(tmpInner);
        }
    }

    {
        _quotaNumStr = context.params[2];
        auto&& tmps = autil::StringUtil::split(context.params[2], INPUT_VALUE_SEP);
        int32_t lastFirstPhaseQuotaNum = 0, lastSecondPhaseQuotaNum = 0;
        for (const std::string& tmp : tmps) {
            auto&& tmpInners = autil::StringUtil::split(tmp, INPUT_VALUE_INNER_SEP);
            if (tmpInners.size() != 2) { // 2 is two phase
                SQL_LOG(ERROR, "Quota field value is not match 2 phase");
                return false;
            }
            try {
                lastFirstPhaseQuotaNum = std::stoi(tmpInners[0]);
                lastSecondPhaseQuotaNum = std::stoi(tmpInners[1]);
            } catch (const std::exception& e) {
                SQL_LOG(ERROR, "Parse 2 phase quota failed");
                return false;
            }
            _firstPhaseQuotaNums.push_back(lastFirstPhaseQuotaNum);
            _secondPhaseQuotaNums.push_back(lastSecondPhaseQuotaNum);
        }
        if (tmps.size() > _quotaFieldValues.size()) {
            SQL_LOG(ERROR, "Quota field value num [%lu] is not match [%lu]",
                    tmps.size(), _quotaFieldValues.size());
            return false;
        } else if (tmps.size() < _quotaFieldValues.size()) {
            // use last quota num as default value
            for (size_t i = tmps.size(); i < _quotaFieldValues.size(); i++) {
                _firstPhaseQuotaNums.push_back(lastFirstPhaseQuotaNum);
                _secondPhaseQuotaNums.push_back(lastSecondPhaseQuotaNum);
            }
        }
    }
    _rerankFieldStr = context.params[3];
    if (_rerankFieldStr.empty()) {
        SQL_LOG(ERROR, "Rerank field is empty");
        return false;
    }
    autil::StringUtil::split(_rerankFields, context.params[3], INPUT_VALUE_SEP);

    {
        _sortStr = context.params[4];
        auto&& tmps = autil::StringUtil::split(context.params[4], INPUT_VALUE_SEP);
        for (std::string& tmp : tmps) {
            if (tmp[0] != '+' && tmp[0] != '-') {
                _sortFields.push_back(tmp);
                _sortFlags.push_back(false);
            } else if (tmp[0] == '+') {
                _sortFields.push_back(tmp.substr(1));
                _sortFlags.push_back(false);
            } else if (tmp[0] == '-') {
                _sortFields.push_back(tmp.substr(1));
                _sortFlags.push_back(true);
            }

            if (_sortFields.back().empty()) {
                SQL_LOG(ERROR, "Sort filed do not allow empty");
                return false;
            }
        }
    }

    if (!autil::StringUtil::fromString<uint32_t>(context.params[5], _perCategoryCount)) {
        SQL_LOG(ERROR, "Failed to convert string[%s] to uint32_t",
                context.params[5].c_str());
        return false;
    }

    if (!autil::StringUtil::fromString<uint32_t>(context.params[6], _windowSize)) {
        SQL_LOG(ERROR, "Failed to convert string[%s] to uint32_t",
                context.params[6].c_str());
        return false;
    }

    auto&& tmps = autil::StringUtil::split(context.params[7], INPUT_VALUE_SEP);
    if (tmps.size() != 2) {
        SQL_LOG(ERROR, "Invalid value [needMakeUp]");
        return false;
    }
    bool flag = autil::StringUtil::fromString<bool>(tmps[0], _firstPhaseNeedMakeUp);
    if (!flag) {
        SQL_LOG(ERROR, "Failed to convert string [%s] to bool", context.params[7].c_str());
        return false;
    }
    flag = autil::StringUtil::fromString<bool>(tmps[1], _secondPhaseNeedMakeUp);
    if (!flag) {
        SQL_LOG(ERROR, "Failed to convert string [%s] to bool", context.params[7].c_str());
        return false;
    }

    _queryPool = context.queryPool;
    // TODO (kong.cui) add some debug info
    SQL_LOG(DEBUG, "Quota field str [%s(%lu)]",
            _quotaFieldStr.c_str(), _quotaFields.size());
    return true;
}

std::pair<int32_t, int32_t>
RerankByQuotaTvfFunc::rerankByQuota(const table::TablePtr& table,
                                    table::ComboComparatorPtr comparator,
                                    const std::vector<table::Row>& group,
                                    const int32_t quotaNum,
                                    std::vector<table::Row>* pTarget,
                                    std::vector<table::Row>* pExtra) {
    std::pair<int32_t, int32_t> result{-1, -1};
    const auto &cmp = [&comparator](const table::Row& a, const table::Row& b) -> bool {
        return comparator->compare(a, b);
    };
    std::vector<size_t> groupKeys;
    if (!table::TableUtil::calculateGroupKeyHashWithSpecificRow(table, _rerankFields,
                    group, groupKeys))
    {
        SQL_LOG(ERROR, "Failed to calculate group key [%s] hash", _rerankFieldStr.c_str());
        return result;
    }
    if (group.size() != groupKeys.size()) {
        SQL_LOG(ERROR, "Not match between group size [%lu], and group key size [%lu]",
                group.size(), groupKeys.size());
        return result;
    }
    std::unordered_map<size_t, std::vector<table::Row> > um;
    for (size_t i = 0; i < group.size(); i++) {
        std::unordered_map<size_t, std::vector<table::Row> >::iterator it;
        it = um.find(groupKeys[i]);
        if (it == um.end()) {
            std::vector<table::Row> v;
            auto tmp = um.insert(std::make_pair(groupKeys[i], v));
            if (!tmp.second) {
                SQL_LOG(ERROR, "Failed to insert empty priority_queue");
                return result;
            }
            it = tmp.first;
        }
        it->second.push_back(group[i]);
    }
    SQL_LOG(DEBUG, "Success to make a rerank group");

    std::unordered_map<int32_t, size_t> ihIndex; // indexid -> hashid
    std::unordered_map<size_t, size_t> hsIndex; // hashid -> sequence
    for (auto& pair : um) {
        // TODO (kong.cui) make sure sort success and assign um
        std::sort(pair.second.begin(), pair.second.end(), cmp);
        ihIndex[pair.second[0].getIndex()] = pair.first;
        hsIndex[pair.first] = 0;
    }

    std::priority_queue<table::Row, std::vector<table::Row>, mycomparison>
        pq(mycomparison(comparator, false));
    auto primaryQueueClear = [&pq, &ihIndex, &hsIndex, &um](int32_t left) -> bool {
        while (pq.size() > static_cast<size_t>(left)) {
            pq.pop();
        }
        while (!pq.empty()) {
            if (left <= 0) {
                return true;
            }
            size_t hashId = ihIndex[pq.top().getIndex()];
            auto& tmpSeq = hsIndex[hashId];
            if (tmpSeq + 1 < um[hashId].size()) {
                int32_t nextIndex = um[hashId][tmpSeq + 1].getIndex();
                ihIndex[nextIndex] = hashId;
            }
            left --;
            hsIndex[hashId] ++;
            pq.pop();
        }

        return false;
    };

    int32_t count = 0;
    while (count < quotaNum) {
        size_t flag = 0;
        for (auto& pair : um) {
            size_t seq = hsIndex[pair.first];
            if (seq >= _perCategoryCount) {
                SQL_LOG(WARN, "More than upper limit [%u] in hash key [%lu]",
                        _perCategoryCount, pair.first);
                flag ++;
                continue;
            }
            if (pair.second.size() <= seq) {
                flag ++;
                continue;
            }
            auto& row = (pair.second)[seq];
            if (pq.size() < _windowSize) {
                pq.push(row);
            } else {
                if (!comparator->compare(pq.top(), row)) {
                    pq.pop();
                    pq.push(row);
                }
            }
        }
        if (flag == um.size()) {
            SQL_LOG(WARN, "All group visit over, visit [%d], but real need [%d]",
                    count, quotaNum);
            break;
        }

        int32_t left = quotaNum - count;
        count += pq.size();
        if (primaryQueueClear(left)) {
            SQL_LOG(INFO, "Finish in advance");
            break;
        }
    }

    int32_t targetNum = 0, extraNum = 0;
    for (auto& pair : um) {
        size_t hashId = pair.first;
        pTarget->insert(pTarget->end(),
                um[hashId].begin(), um[hashId].begin() + hsIndex[hashId]);
        targetNum += hsIndex[hashId];
        if (hsIndex[hashId] != um[hashId].size()) {
            pExtra->insert(pExtra->end(),
                    um[hashId].begin() + hsIndex[hashId], um[hashId].end());
            extraNum += um[hashId].size() - hsIndex[hashId];
        }
    }
    result.first = targetNum;
    result.second = extraNum;

    return result;
}

void RerankByQuotaTvfFunc::makeUp(std::vector<table::Row>& target,
                                  const std::vector<table::Row>& extra) {
    size_t total = 0;
    if (getScope() == KernelScope::PARTIAL) {
        for (auto& num : _firstPhaseQuotaNums) {
            total += num;
        }
    } else {
        for (auto& num : _secondPhaseQuotaNums) {
            total += num;
        }
    }

    if (target.size() < total) {
        size_t offset = total - target.size();
        target.insert(target.end(),
                extra.begin(), extra.begin() + std::min(offset, extra.size()));
    }
}

bool RerankByQuotaTvfFunc::computeTable(const table::TablePtr& table) {
    std::vector<int32_t> quotaNums;
    bool needMakeUp = false;
    if (getScope() == KernelScope::PARTIAL) {
        quotaNums = _firstPhaseQuotaNums;
        needMakeUp = _firstPhaseNeedMakeUp;
    } else {
        quotaNums = _secondPhaseQuotaNums;
        needMakeUp = _secondPhaseNeedMakeUp;
    }

    table::ComboComparatorPtr comparator =
        table::ComparatorCreator::createComboComparator(table, _sortFields, _sortFlags, _queryPool);
    if (comparator == nullptr) {
        SQL_LOG(ERROR, "Failed to init rerankByQuota tvf combo comparator [%s]",
                _sortStr.c_str());
        return false;
    }
    // 1: make a group by using quotaFields
    std::vector<std::vector<table::Row> > groups;
    groups.resize(_quotaFieldValues.size());
    makeGroup(table, &groups);
    SQL_LOG(DEBUG, "Success to make group");
    // 2: make a rerank by using rerankFields in every group (using sort function) with a quota
    std::vector<table::Row> target, extra;
    std::vector<int32_t> targetNums, extraNums;
    for (size_t i = 0; i < groups.size(); i++) {
        std::pair<uint32_t, uint32_t> groupNum =
            rerankByQuota(table, comparator, groups[i], quotaNums[i], &target, &extra);
        if (groupNum.first == -1 || groupNum.second == -1) {
            SQL_LOG(ERROR, "Failed to rerank in group[%lu]", i);
            return false;
        }
        targetNums.push_back(groupNum.first);
        if (needMakeUp) {
            extraNums.push_back(groupNum.second);
        }
        SQL_LOG(DEBUG, "Group index [%lu], target size [%d], extra size [%d]",
                i, groupNum.first, groupNum.second);
    }
    SQL_LOG(DEBUG, "Success to make rerank");
    if (needMakeUp) {
        makeUp(target, extra);
    }

    table->setRows(target);
    return true;
}

RerankByQuotaTvfFuncCreator::RerankByQuotaTvfFuncCreator()
    : TvfFuncCreatorR(rerankByQuotaTvfDef, rerankByQuotaTvfInfo) {}

RerankByQuotaTvfFuncCreator::~RerankByQuotaTvfFuncCreator() {}

TvfFunc *RerankByQuotaTvfFuncCreator::createFunction(const SqlTvfProfileInfo &info) {
    return new RerankByQuotaTvfFunc();
}

REGISTER_RESOURCE(RerankByQuotaTvfFuncCreator);

} // namespace builtin
} // namespace isearch
