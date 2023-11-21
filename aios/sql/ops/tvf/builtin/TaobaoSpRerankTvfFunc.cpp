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
#include "sql/ops/tvf/builtin/TaobaoSpRerankTvfFunc.h"

#include <algorithm>
#include <cstdint>
#include <exception>
#include <math.h>
#include <memory>

#include "autil/StringUtil.h"
#include "navi/engine/Resource.h"
#include "sql/common/Log.h"
#include "sql/ops/tvf/SqlTvfProfileInfo.h"
#include "sql/ops/tvf/TvfFunc.h"
#include "table/ComboComparator.h"
#include "table/ComparatorCreator.h"
#include "table/Row.h"

namespace sql {

const SqlTvfProfileInfo taobaoSpRerankTvfInfo("taobaoSpRerankTvf", "taobaoSpRerankTvf");
const std::string taobaoSpRerankTvfDef = R"tvf_json(
{
  "function_version": 1,
  "function_name": "taobaoSpRerankTvf",
  "function_type": "TVF",
  "is_deterministic": 1,
  "function_content_version": "json_default_0.1",
  "function_content": {
    "properties": {
      "enable_shuffle": false,
      "normal_scope": true
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

std::string getNextTerm(const std::string &inputStr, char sep, size_t &start) {
    std::string ret;
    ret.reserve(64);
    while (start < inputStr.length()) {
        char c = inputStr[start];
        ++start;
        if (c == sep) {
            break;
        } else if (c == '\\') {
            if (start < inputStr.length()) {
                ret += inputStr[start];
                ++start;
            }
            continue;
        }
        ret += c;
    }
    autil::StringUtil::trim(ret);
    return ret;
}

TaobaoSpRerankTvfFunc::TaobaoSpRerankTvfFunc()
    : _needSort(false) {}

TaobaoSpRerankTvfFunc::~TaobaoSpRerankTvfFunc() {}

bool TaobaoSpRerankTvfFunc::init(TvfFuncInitContext &context) {
    if (context.params.size() != 4u) {
        SQL_LOG(ERROR, "Param size [%ld] not equal 4", context.params.size());
        return false;
    }
    _quotaFieldStr = context.params[0];
    autil::StringUtil::split(_quotaFields, context.params[0], INPUT_VALUE_SEP);

    {
        _quotaFieldValueStr = context.params[1];
        auto &&tmps = autil::StringUtil::split(context.params[1], INPUT_VALUE_SEP);
        for (const std::string &tmp : tmps) {
            auto &&tmpInner = autil::StringUtil::split(tmp, INPUT_VALUE_INNER_SEP);
            if (tmpInner.size() != _quotaFields.size()) {
                SQL_LOG(ERROR,
                        "Field [%s] num should be [%lu], actually is [%lu]",
                        tmp.c_str(),
                        _quotaFields.size(),
                        tmpInner.size());
                return false;
            }
            _quotaFieldValues.emplace_back(tmpInner);
        }
    }

    {
        auto &&tmps = autil::StringUtil::split(context.params[2], INPUT_VALUE_SEP);
        for (const auto &tmp : tmps) {
            try {
                _quotaNums.push_back(std::stoi(tmp));
            } catch (const std::exception &e) {
                SQL_LOG(ERROR, "Parse quota num [%s] failed", tmp.c_str());
                return false;
            }
        }
        if (_quotaNums.size() == 0) {
            SQL_LOG(ERROR, "Must input quota num [%s]", context.params[2].c_str());
            return false;
        } else if (_quotaNums.size() > _quotaFieldValues.size()) {
            SQL_LOG(ERROR,
                    "quota num [%s] must less or equal to all values size [%lu]",
                    context.params[2].c_str(),
                    _quotaFieldValues.size());
            return false;
        } else if (_quotaNums.size() < _quotaFieldValues.size()) {
            // use the last value as default quota num
            int32_t val = _quotaNums[_quotaNums.size() - 1];
            for (size_t i = _quotaNums.size(); i < _quotaFieldValues.size(); i++) {
                _quotaNums.push_back(val);
            }
        }
    }

    {
        _sortStr = context.params[3];
        if (!_sortStr.empty()) {
            _needSort = true;
            auto &&tmps = autil::StringUtil::split(context.params[3], INPUT_VALUE_SEP);
            for (std::string &tmp : tmps) {
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
    }

    _queryPool = context.queryPool;
    return true;
}

bool TaobaoSpRerankTvfFunc::computeTable(const table::TablePtr &table) {
    // 1: make a group by using quotaFields
    std::vector<std::vector<table::Row>> groups;
    groups.resize(_quotaFieldValues.size());
    makeGroup(table, &groups);
    SQL_LOG(TRACE3, "Success to make group");
    // 2: make a rerank by using rerankFields in every group (using sort function) with a quota
    std::vector<table::Row> target;
    for (size_t i = 0; i < groups.size(); i++) {
        if (_needSort) {
            table::ComboComparatorPtr comparator = table::ComparatorCreator::createComboComparator(
                table, _sortFields, _sortFlags, _queryPool);
            if (comparator == nullptr) {
                SQL_LOG(ERROR,
                        "Failed to init rerankByQuota tvf combo comparator [%s]",
                        _sortStr.c_str());
                return false;
            }
            const auto &cmp = [&comparator](const table::Row &a, const table::Row &b) -> bool {
                return comparator->compare(a, b);
            };
            std::sort(groups[i].begin(), groups[i].end(), cmp);
        }
    }

    std::vector<size_t> actualQuota(groups.size(), 0);
    computeActualQuota(groups, actualQuota);

    for (size_t i = 0; i < groups.size(); ++i) {
        if (groups[i].size() < actualQuota[i]) {
            SQL_LOG(ERROR,
                    "compute actual quota error. group size[%lu], actual[%lu]",
                    groups[i].size(),
                    actualQuota[i]);
            return false;
        }
        target.insert(target.end(), groups[i].begin(), groups[i].begin() + actualQuota[i]);
        SQL_LOG(DEBUG, "compute actual quota[%lu], total[%lu]", actualQuota[i], target.size());
    }

    table->setRows(target);
    return true;
}

void TaobaoSpRerankTvfFunc::computeActualQuota(const std::vector<std::vector<table::Row>> &groups,
                                               std::vector<size_t> &actualQuota) {
    size_t leftCount = 0;
    for (const auto &quotaNum : _quotaNums) {
        leftCount += quotaNum;
    }

    if (leftCount <= 0) {
        SQL_LOG(INFO, "compute actual quota error, total num is 0.");
        return;
    }

    std::vector<size_t> groupsSize;
    for (const auto &group : groups) {
        groupsSize.emplace_back(group.size());
    }
    while (leftCount > 0) {
        auto curLeftCount = computeLeftCount(groups, leftCount, groupsSize, actualQuota);
        if (curLeftCount == leftCount) {
            break;
        }
        leftCount = curLeftCount;
    }
}

size_t TaobaoSpRerankTvfFunc::computeLeftCount(const std::vector<std::vector<table::Row>> &groups,
                                               const size_t leftCount,
                                               std::vector<size_t> &groupsSize,
                                               std::vector<size_t> &actualQuota) {
    size_t remainCount = leftCount;
    if (leftCount <= 0) {
        return 0;
    }

    size_t totalWeights = 0;
    for (size_t i = 0; i < groupsSize.size(); ++i) {
        if (groupsSize[i] > 0) {
            totalWeights += _quotaNums[i];
        }
    }

    // actual quota fill empty
    if (totalWeights <= 0) {
        return 0;
    }

    size_t lastRow = -1;
    for (size_t i = 0; i < groupsSize.size(); ++i) {
        size_t limit = groupsSize[i];
        if (limit <= 0) {
            continue;
        }
        lastRow = i;
        // compute quota by weight
        size_t mergeCount = (size_t)floor(leftCount * _quotaNums[i] * 1.0 / totalWeights + 0.5);
        mergeCount = std::min(mergeCount, groupsSize[i]);
        actualQuota[i] += mergeCount;
        groupsSize[i] -= mergeCount;
        remainCount -= mergeCount;
    }

    // fill quota by last row
    if (remainCount > 0 && lastRow >= 0) {
        size_t mergeCount = std::min(remainCount, groupsSize[lastRow]);
        actualQuota[lastRow] += mergeCount;
        groupsSize[lastRow] -= mergeCount;
        remainCount -= mergeCount;
    }

    return remainCount;
}

TaobaoSpRerankTvfFuncCreator::TaobaoSpRerankTvfFuncCreator()
    : TvfFuncCreatorR(taobaoSpRerankTvfDef, taobaoSpRerankTvfInfo) {}

TaobaoSpRerankTvfFuncCreator::~TaobaoSpRerankTvfFuncCreator() {}

TvfFunc *TaobaoSpRerankTvfFuncCreator::createFunction(const SqlTvfProfileInfo &info) {
    return new TaobaoSpRerankTvfFunc();
}

REGISTER_RESOURCE(TaobaoSpRerankTvfFuncCreator);

} // namespace sql
