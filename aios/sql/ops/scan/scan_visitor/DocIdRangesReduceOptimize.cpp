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
#include "sql/ops/scan/DocIdRangesReduceOptimize.h"

#include <algorithm>
#include <cstddef>
#include <ext/alloc_traits.h>
#include <memory>
#include <rapidjson/document.h>
#include <utility>
#include <vector>

#include "autil/TimeUtility.h"
#include "autil/legacy/RapidJsonCommon.h"
#include "autil/legacy/RapidJsonHelper.h"
#include "autil/mem_pool/PoolVector.h"
#include "indexlib/base/Types.h"
#include "indexlib/index_base/index_meta/partition_meta.h"
#include "indexlib/indexlib.h"
#include "matchdoc/ValueType.h"
#include "sql/common/FieldInfo.h"
#include "sql/common/Log.h"
#include "sql/common/common.h"
#include "sql/ops/condition/Condition.h"
#include "sql/ops/condition/ExprUtil.h"
#include "sql/ops/condition/SqlJsonUtil.h"
#include "sql/ops/scan/DocIdRangesReduceOptimize.hpp"
#include "suez/sdk/TableDefConfig.h"

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil

using namespace std;
using namespace autil;
using namespace matchdoc;
using namespace indexlib::index_base;

namespace sql {
AUTIL_LOG_SETUP(sql, DocIdRangesReduceOptimize);

DocIdRangesReduceOptimize::DocIdRangesReduceOptimize(
    const std::vector<suez::SortDescription> &sortDescs,
    const std::map<std::string, FieldInfo> &fieldInfos)
    : _fieldInfos(fieldInfos) {
    for (const auto &sortDesc : sortDescs) {
        _keyVec.push_back(sortDesc.field);
    }
}

DocIdRangesReduceOptimize::~DocIdRangesReduceOptimize() {}

void DocIdRangesReduceOptimize::visitAndCondition(AndCondition *condition) {
    std::unordered_map<std::string, KeyRangeBasePtr> key2keyRange;
    const vector<ConditionPtr> &children = condition->getChildCondition();
    for (size_t i = 0; i < children.size(); ++i) {
        children[i]->accept(this);
        std::unordered_map<std::string, KeyRangeBasePtr> curKey2Range;
        swapKey2KeyRange(curKey2Range);
        for (const auto &k2r : curKey2Range) {
            auto iter = key2keyRange.find(k2r.first);
            if (iter == key2keyRange.end()) {
                key2keyRange[k2r.first] = k2r.second;
            } else {
                iter->second->intersectionRanges(k2r.second.get());
            }
        }
    }
    swapKey2KeyRange(key2keyRange);
}

void DocIdRangesReduceOptimize::visitOrCondition(OrCondition *condition) {
    std::unordered_map<std::string, KeyRangeBasePtr> key2keyRange;
    const vector<ConditionPtr> &children = condition->getChildCondition();
    for (size_t i = 0; i < children.size(); ++i) {
        children[i]->accept(this);
        std::unordered_map<std::string, KeyRangeBasePtr> curKey2Range;
        swapKey2KeyRange(curKey2Range);
        if (curKey2Range.size() != 1) {
            return;
        }
        if (key2keyRange.empty()) {
            key2keyRange = curKey2Range;
        } else {
            if (key2keyRange.begin()->first != curKey2Range.begin()->first) {
                return;
            } else {
                auto &keyRange = key2keyRange.begin()->second;
                keyRange->insertAndTrim(curKey2Range.begin()->second.get());
            }
        }
    }
    swapKey2KeyRange(key2keyRange);
}

void DocIdRangesReduceOptimize::visitNotCondition(NotCondition *condition) {
    return;
}

void DocIdRangesReduceOptimize::visitLeafCondition(LeafCondition *condition) {
    const SimpleValue &leafCondition = condition->getCondition();
    if (!leafCondition.IsObject() || !leafCondition.HasMember(SQL_CONDITION_OPERATOR)
        || !leafCondition.HasMember(SQL_CONDITION_PARAMETER)) {
        return;
    }
    string op(leafCondition[SQL_CONDITION_OPERATOR].GetString());
    const SimpleValue &param = leafCondition[SQL_CONDITION_PARAMETER];
    if (ExprUtil::isUdf(leafCondition)) {
        if (ExprUtil::isSameUdf(leafCondition, SQL_UDF_CONTAIN_OP)) {
            if ((!param.IsArray()) || param.Size() > 3 || param.Size() < 2) {
                return;
            }
            tryAddKeyRange(param[0], param, SQL_UDF_CONTAIN_OP);
        }
        return;
    } else if (op == SQL_IN_OP) {
        if ((!param.IsArray()) || param.Size() < 2) {
            return;
        }
        tryAddKeyRange(param[0], param, SQL_IN_OP);
    } else {
        parseArithmeticOp(param, op);
    }
}

void DocIdRangesReduceOptimize::parseArithmeticOp(const SimpleValue &param, const std::string &op) {
    if ((!param.IsArray()) || param.Size() != 2) {
        SQL_LOG(TRACE2,
                "param [%s] is not 2 args array, skip",
                RapidJsonHelper::SimpleValue2Str(param).c_str());
        return;
    }
    const SimpleValue &left = param[0];
    const SimpleValue &right = param[1];
    bool leftHasColumn = SqlJsonUtil::isColumn(left);
    bool rightHasColumn = SqlJsonUtil::isColumn(right);
    if (leftHasColumn && rightHasColumn) {
        SQL_LOG(TRACE2,
                "param [%s] args are both column, skip",
                RapidJsonHelper::SimpleValue2Str(param).c_str());
        return;
    } else if (leftHasColumn && right.IsNumber()) {
        tryAddKeyRange(left, right, op);
    } else if (rightHasColumn && left.IsNumber()) {
        tryAddKeyRange(right, left, op);
    } else {
        SQL_LOG(TRACE2,
                "param [%s] args are not column, skip",
                RapidJsonHelper::SimpleValue2Str(param).c_str());
        return;
    }
}

void DocIdRangesReduceOptimize::tryAddKeyRange(const SimpleValue &attr,
                                               const SimpleValue &value,
                                               const std::string &op) {
    if (!SqlJsonUtil::isColumn(attr)) {
        SQL_LOG(TRACE3, "attr [%s] not column", RapidJsonHelper::SimpleValue2Str(attr).c_str());
        return;
    }
    const string &attrName = SqlJsonUtil::getColumnName(attr);
    if (std::count(_keyVec.begin(), _keyVec.end(), attrName) == 0) {
        SQL_LOG(TRACE3, "attr [%s] not found in sort desc", attrName.c_str());
        return;
    }
    auto iter = _fieldInfos.find(attrName);
    if (iter == _fieldInfos.end()) {
        SQL_LOG(TRACE3, "attr [%s] not found in field infos", attrName.c_str());
        return;
    }
    static const std::map<std::string, matchdoc::BuiltinType> strToBuiltinTypeMap
        = {{"int8", bt_int8},
           {"int16", bt_int16},
           {"int32", bt_int32},
           {"int64", bt_int64},
           {"uint8", bt_uint8},
           {"uint16", bt_uint16},
           {"uint32", bt_uint32},
           {"uint64", bt_uint64}};
    const string &fieldType = iter->second.type;
    auto fieldIter = strToBuiltinTypeMap.find(fieldType);
    if (fieldIter != strToBuiltinTypeMap.end()) {
        const BuiltinType &bt = fieldIter->second;
        switch (bt) {
#define KEY_RANGE_HELPER(ft, R, func)                                                              \
    case ft: {                                                                                     \
        typedef MatchDocBuiltinType2CppType<ft, false>::CppType T;                                 \
        _key2keyRange[attrName] = KeyRangeBasePtr(genKeyRange<T>(attrName, op, value));            \
        break;                                                                                     \
    }
            KEY_RANGE_HELPER(bt_int8, int64_t, GetInt64);
            KEY_RANGE_HELPER(bt_int16, int64_t, GetInt64);
            KEY_RANGE_HELPER(bt_int32, int64_t, GetInt64);
            KEY_RANGE_HELPER(bt_int64, int64_t, GetInt64);
            KEY_RANGE_HELPER(bt_uint8, uint64_t, GetUInt64);
            KEY_RANGE_HELPER(bt_uint16, uint64_t, GetUInt64);
            KEY_RANGE_HELPER(bt_uint32, uint64_t, GetUInt64);
            KEY_RANGE_HELPER(bt_uint64, uint64_t, GetUInt64);
#undef KEY_RANGE_HELPER
        default: {
            break;
        }
        }
    } else {
        SQL_LOG(TRACE3, "type not supported, will skip");
    }
}

indexlib::table::DimensionDescriptionVector DocIdRangesReduceOptimize::convertDimens() {
    indexlib::table::DimensionDescriptionVector dimens;
    for (const auto &key : _keyVec) {
        auto iter = _key2keyRange.find(key);
        if (iter == _key2keyRange.end()) {
            break;
        }
        KeyRangeBasePtr keyRange = iter->second;
        dimens.emplace_back(keyRange->convertDimenDescription());
        if (!dimens.back()->ranges.empty()) {
            break;
        }
    }
    return dimens;
}

std::string DocIdRangesReduceOptimize::toDebugString(
    const indexlib::table::DimensionDescriptionVector &dimens) {
    std::string dimensString("[");
    for (size_t i = 0; i < dimens.size(); ++i) {
        dimensString += (i == 0 ? "" : ", ") + dimens[i]->toString();
    }
    dimensString += "]";
    return dimensString;
}

isearch::search::LayerMetaPtr DocIdRangesReduceOptimize::reduceDocIdRange(
    const isearch::search::LayerMetaPtr &lastRange,
    autil::mem_pool::Pool *pool,
    isearch::search::IndexPartitionReaderWrapperPtr &readerPtr) {
    const indexlib::table::DimensionDescriptionVector &dimens = convertDimens();
    SQL_LOG(DEBUG, "after convert to dimentions, dimens : %s", toDebugString(dimens).c_str());
    isearch::search::LayerMetaPtr layerMeta(new isearch::search::LayerMeta(pool));
    layerMeta->quota = lastRange->quota;
    layerMeta->maxQuota = lastRange->maxQuota;
    layerMeta->quotaMode = lastRange->quotaMode;
    layerMeta->needAggregate = lastRange->needAggregate;
    layerMeta->quotaType = lastRange->quotaType;

    for (size_t i = 0; i < lastRange->size(); ++i) {
        if ((*lastRange)[i].ordered != isearch::search::DocIdRangeMeta::OT_ORDERED) {
            layerMeta->push_back((*lastRange)[i]);
            continue;
        }
        indexlib::DocIdRange rangeLimit((*lastRange)[i].begin, (*lastRange)[i].end + 1);
        if (rangeLimit.second < rangeLimit.first) {
            continue;
        }
        indexlib::DocIdRangeVector resultRanges;
        if (!readerPtr->getSortedDocIdRanges(dimens, rangeLimit, resultRanges)) {
            return lastRange;
        }
        for (size_t j = 0; j < resultRanges.size(); ++j) {
            isearch::search::DocIdRangeMeta rangeMeta(
                resultRanges[j].first, resultRanges[j].second - 1, (*lastRange)[i].ordered);
            if (rangeMeta.begin <= rangeMeta.end) {
                layerMeta->push_back(rangeMeta);
            }
        }
    }
    return layerMeta;
}

} // namespace sql
