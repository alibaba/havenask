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
#include "sql/ops/tvf/builtin/SimpleRerankTvfFunc.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <exception>
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

const SqlTvfProfileInfo simpleRerankTvfInfo("simpleRerankTvf", "simpleRerankTvf");
const std::string simpleRerankTvfDef = R"tvf_json(
{
  "function_version": 1,
  "function_name": "simpleRerankTvf",
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

SimpleRerankTvfFunc::SimpleRerankTvfFunc()
    : _needMakeUp(false)
    , _needSort(false) {}

SimpleRerankTvfFunc::~SimpleRerankTvfFunc() {}

bool SimpleRerankTvfFunc::init(TvfFuncInitContext &context) {
    if (context.params.size() != 5u) {
        SQL_LOG(ERROR, "Param size [%ld] not equal 5", context.params.size());
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
        _quotaNumStr = context.params[2];
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

    bool flag = autil::StringUtil::fromString<bool>(context.params[4], _needMakeUp);
    if (!flag) {
        SQL_LOG(ERROR, "Failed to convert string [%s] to bool", context.params[4].c_str());
        return false;
    }

    _queryPool = context.queryPool;
    return true;
}

void SimpleRerankTvfFunc::makeUp(std::vector<table::Row> &target,
                                 const std::vector<table::Row> &extra) {
    size_t total = 0;
    for (auto &num : _quotaNums) {
        total += num;
    }

    if (target.size() < total) {
        size_t offset = total - target.size();
        target.insert(target.end(), extra.begin(), extra.begin() + std::min(offset, extra.size()));
    }
}

bool SimpleRerankTvfFunc::computeTable(const table::TablePtr &table) {
    // 1: make a group by using quotaFields
    std::vector<std::vector<table::Row>> groups;
    groups.resize(_quotaFieldValues.size());
    makeGroup(table, &groups);
    SQL_LOG(DEBUG, "Success to make group");
    // 2: make a rerank by using rerankFields in every group (using sort function) with a quota
    std::vector<table::Row> target, extra;
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
        if (groups[i].size() >= _quotaNums[i]) {
            target.insert(target.end(), groups[i].begin(), groups[i].begin() + _quotaNums[i]);
            extra.insert(extra.end(), groups[i].begin() + _quotaNums[i], groups[i].end());
        } else {
            target.insert(target.end(), groups[i].begin(), groups[i].begin() + groups[i].size());
        }
    }
    if (_needMakeUp) {
        makeUp(target, extra);
    }

    table->setRows(target);
    return true;
}

SimpleRerankTvfFuncCreator::SimpleRerankTvfFuncCreator()
    : TvfFuncCreatorR(simpleRerankTvfDef, simpleRerankTvfInfo) {}

SimpleRerankTvfFuncCreator::~SimpleRerankTvfFuncCreator() {}

TvfFunc *SimpleRerankTvfFuncCreator::createFunction(const SqlTvfProfileInfo &info) {
    return new SimpleRerankTvfFunc();
}

REGISTER_RESOURCE(SimpleRerankTvfFuncCreator);

} // namespace sql
