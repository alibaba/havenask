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
#include "indexlib/document/query/range_query_evaluator.h"

#include "autil/StringUtil.h"

using namespace std;
using namespace autil;

namespace indexlib { namespace document {
IE_LOG_SETUP(document, RangeQueryEvaluator);

EvaluatorState RangeQueryEvaluator::DoEvaluate(EvaluatorStateHandler& handler, const RawDocumentPtr& rawDoc,
                                               EvaluateParam param)
{
    if (!rawDoc) {
        return ES_PENDING;
    }

    if (mFunc) {
        mFunc->Execute(rawDoc);
    }

    if (!rawDoc->exist(mFieldName)) {
        return param.pendingForFieldNotExist ? ES_PENDING : ES_FALSE;
    }

    std::string fieldValue = rawDoc->getField(mFieldName);
    RangeQuery::RangeValueType type = RangeQuery::GetValueType(fieldValue);
    if (type == mRangeInfo.valueType) {
        switch (type) {
        case RangeQuery::RVT_UINT: {
            uint64_t value;
            if (!StringUtil::fromString(fieldValue, value)) {
                return ES_FALSE;
            }
            bool leftResult = mRangeInfo.leftInclude ? mRangeInfo.leftValue.uintValue <= value
                                                     : mRangeInfo.leftValue.uintValue < value;
            bool rightResult = mRangeInfo.rightInclude ? mRangeInfo.rightValue.uintValue >= value
                                                       : mRangeInfo.rightValue.uintValue > value;
            return (leftResult && rightResult) ? ES_TRUE : ES_FALSE;
        }
        case RangeQuery::RVT_INT: {
            int64_t value;
            if (!StringUtil::fromString(fieldValue, value)) {
                return ES_FALSE;
            }
            bool leftResult =
                mRangeInfo.leftInclude ? mRangeInfo.leftValue.intValue <= value : mRangeInfo.leftValue.intValue < value;
            bool rightResult = mRangeInfo.rightInclude ? mRangeInfo.rightValue.intValue >= value
                                                       : mRangeInfo.rightValue.intValue > value;
            return (leftResult && rightResult) ? ES_TRUE : ES_FALSE;
        }
        case RangeQuery::RVT_DOUBLE: {
            double value;
            if (!StringUtil::fromString(fieldValue, value)) {
                return ES_FALSE;
            }
            bool leftResult = mRangeInfo.leftInclude ? mRangeInfo.leftValue.doubleValue <= value
                                                     : mRangeInfo.leftValue.doubleValue < value;
            bool rightResult = mRangeInfo.rightInclude ? mRangeInfo.rightValue.doubleValue >= value
                                                       : mRangeInfo.rightValue.doubleValue > value;
            return (leftResult && rightResult) ? ES_TRUE : ES_FALSE;
        }
        default:
            return ES_FALSE;
        }
    }

    type = (RangeQuery::RangeValueType)max((int)type, (int)mRangeInfo.valueType);
    switch (type) {
    case RangeQuery::RVT_INT: {
        int64_t value, leftValue, rightValue;
        if (!StringUtil::fromString(fieldValue, value) || !StringUtil::fromString(mRangeInfo.leftStr, leftValue) ||
            !StringUtil::fromString(mRangeInfo.rightStr, rightValue)) {
            return ES_FALSE;
        }
        bool leftResult = mRangeInfo.leftInclude ? leftValue <= value : leftValue < value;
        bool rightResult = mRangeInfo.rightInclude ? rightValue >= value : rightValue > value;
        return (leftResult && rightResult) ? ES_TRUE : ES_FALSE;
    }
    case RangeQuery::RVT_DOUBLE: {
        double value, leftValue, rightValue;
        if (!StringUtil::fromString(fieldValue, value) || !StringUtil::fromString(mRangeInfo.leftStr, leftValue) ||
            !StringUtil::fromString(mRangeInfo.rightStr, rightValue)) {
            return ES_FALSE;
        }
        bool leftResult = mRangeInfo.leftInclude ? leftValue <= value : leftValue < value;
        bool rightResult = mRangeInfo.rightInclude ? rightValue >= value : rightValue > value;
        return (leftResult && rightResult) ? ES_TRUE : ES_FALSE;
    }
    default:
        return ES_FALSE;
    }
    return ES_FALSE;
}
}} // namespace indexlib::document
