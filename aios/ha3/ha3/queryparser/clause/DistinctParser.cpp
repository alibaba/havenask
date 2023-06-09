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
#include "ha3/queryparser/DistinctParser.h"

#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include <algorithm>
#include <sstream>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "ha3/common/DistinctDescription.h"
#include "ha3/common/ErrorDefine.h"
#include "ha3/common/ErrorResult.h"
#include "ha3/common/FilterClause.h"
#include "ha3/common/RequestSymbolDefine.h"
#include "suez/turing/expression/syntax/SyntaxExpr.h"

using namespace std;
using namespace autil;
using namespace suez::turing;

using namespace isearch::common;

namespace isearch {
namespace queryparser {

AUTIL_LOG_SETUP(ha3, DistinctParser);

DistinctParser::DistinctParser(ErrorResult *errorResult) 
    : _errorResult(errorResult)
{
}

DistinctParser::~DistinctParser() { 
}

DistinctDescription* DistinctParser::createDistinctDescription() {
    return new DistinctDescription;
}

vector<string *>* DistinctParser::createThresholds() {
    return new std::vector<std::string *>;
}

FilterClause* DistinctParser::createFilterClause(SyntaxExpr *expr){
    FilterClause* filterClause = new FilterClause(expr);
    if (expr) {
        filterClause->setOriginalString(expr->getExprString());
    }
    return filterClause;
}

void DistinctParser::setDistinctCount(DistinctDescription *distDes,
        const std::string &distinctCountStr) {
    assert(distDes);
    int32_t intValue = 0;
    if (!StringUtil::strToInt32(distinctCountStr.c_str(), intValue)) {
        string errMsg = string("this value cannot be converted to int32: ") 
                        + distinctCountStr;
        _errorResult->resetError(ERROR_DISTINCT_COUNT, errMsg);
        AUTIL_LOG(ERROR, "%s", errMsg.c_str());
        return;
    }
    distDes->setDistinctCount(intValue);

}

void DistinctParser::setDistinctTimes(DistinctDescription *distDes,
        const std::string &distinctTimesStr) {
    assert(distDes);
    int32_t intValue;
    if (!StringUtil::strToInt32(distinctTimesStr.c_str(), intValue)) {
        string errMsg = string("this value cannot be converted to int32: ") 
                        + distinctTimesStr;
        _errorResult->resetError(ERROR_DISTINCT_TIMES, errMsg);
        AUTIL_LOG(ERROR, "%s", errMsg.c_str());
        return ;
    }
    distDes->setDistinctTimes(intValue);
}

void DistinctParser::setMaxItemCount(DistinctDescription *distDes,
                                     const string &maxItemCountStr)
{
    assert(distDes);
    int32_t intValue;
    if (!StringUtil::strToInt32(maxItemCountStr.c_str(), intValue)) {
        string errMsg = string("this value cannot be converted to int32: ") 
                        + maxItemCountStr;
        _errorResult->resetError(ERROR_DISTINCT_MAX_ITEM_COUNT, errMsg);
        AUTIL_LOG(ERROR, "%s", errMsg.c_str());
        return ;
    }
    distDes->setMaxItemCount(intValue);
}

void DistinctParser::setReservedFlag(DistinctDescription *distDes,
        const std::string &value)
{
    assert(distDes);
    bool reservedFlag = true;
    if (value == DISTINCT_RESERVED_FLAG_TRUE) {
        reservedFlag = true;
    } else if (value == DISTINCT_RESERVED_FLAG_FALSE) {
        reservedFlag = false;
    } else {
        string errMsg = string("distinct reserved flag: ") + value;
        _errorResult->resetError(ERROR_DISTINCT_RESERVED_FLAG, errMsg);
        AUTIL_LOG(ERROR, "%s", errMsg.c_str());
        return;
    }
    distDes->setReservedFlag(reservedFlag);
}

void DistinctParser::setUpdateTotalHitFlag(DistinctDescription *distDes,
        const std::string &updateTotalHitFlagStr)
{
    assert(distDes);
    bool updateTotalHitFlag = true;
    if (updateTotalHitFlagStr == DISTINCT_UPDATE_TOTAL_HIT_FLAG_TRUE) {
        updateTotalHitFlag = true;
    } else if (updateTotalHitFlagStr == DISTINCT_UPDATE_TOTAL_HIT_FLAG_FALSE) {
        updateTotalHitFlag = false;
    } else {
        string errMsg = string("distinct update total hit flag: ") 
                        + updateTotalHitFlagStr;
        _errorResult->resetError(ERROR_DISTINCT_UPDATE_TOTAL_HIT_FLAG, errMsg);
        AUTIL_LOG(WARN, "%s", errMsg.c_str());
        return;
    }
    distDes->setUpdateTotalHitFlag(updateTotalHitFlag);
}

void DistinctParser::setGradeThresholds(DistinctDescription *distDes,
        vector<string *> *gradeThresholdsStr)
{
    assert(distDes);
    
    if (gradeThresholdsStr->size() == (size_t)0) {
        _errorResult->resetError(ERROR_DISTINCT_GRADE,
                                string("error distinct grade: empty"));
        AUTIL_LOG(WARN, "error distinct grade: empty");
        delete gradeThresholdsStr;
        return;
    }

    vector<double> gradeThresholds;
    double gradeThreshold = 0.0;
    bool successed = true;
    for (size_t i = 0; i < gradeThresholdsStr->size(); ++i)
    {
        if ((*gradeThresholdsStr)[i]->empty()) {
            _errorResult->resetError(ERROR_DISTINCT_GRADE,
                    string("error distinct grade thresholds have empty element"));
            AUTIL_LOG(WARN, "error distinct grade thresholds have empty element");
            successed = false;
            break;
        }
        if (!StringUtil::strToDouble((*gradeThresholdsStr)[i]->c_str(), 
                        gradeThreshold))
        {
            string errMsg = string("this value cannot be converted to double:")
                            + *(*gradeThresholdsStr)[i];
            _errorResult->resetError(ERROR_DISTINCT_GRADE, errMsg);
            AUTIL_LOG(WARN, "%s", errMsg.c_str());
            successed = false;
            break;
        }
        gradeThresholds.push_back(gradeThreshold);
    }
    if (successed) {
        sort(gradeThresholds.begin(), gradeThresholds.end());
        distDes->setGradeThresholds(gradeThresholds);
    }

    for (size_t i = 0; i < gradeThresholdsStr->size(); ++i)
    {
        delete (*gradeThresholdsStr)[i];
    }
    delete gradeThresholdsStr;
}

} // namespace queryparser
} // namespace isearch

