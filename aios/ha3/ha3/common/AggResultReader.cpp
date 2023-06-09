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
#include "ha3/common/AggResultReader.h"

#include <cstddef>
#include <memory>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "ha3/common/AggregateResult.h"
#include "ha3/queryparser/AggregateParser.h"
#include "ha3/queryparser/ClauseParserContext.h"

using namespace std;

using namespace isearch::queryparser;
namespace isearch {
namespace common {
AUTIL_LOG_SETUP(ha3, AggResultReader);

AggResultReader::AggResultReader(const AggregateResultsPtr &aggResults)
    : _aggResults(aggResults)
{
    _constructedMap.resize(aggResults ? aggResults->size() : 0, false);
}

AggResultReader::~AggResultReader() { 
}

AggResultReader::BasePair AggResultReader::getAggResult(
        const string &key, const string &funDes)
{
    BasePair ret(NULL, NULL);

    if (!_aggResults) {
        return ret;
    }

    ClauseParserContext ctx;
    string keyExprStr = ctx.getNormalizedExprStr(key);
    string funDesExprStr = ctx.getNormalizedExprStr(funDes);

    if (keyExprStr.empty()) { 
        AUTIL_LOG(WARN, "invalid key [%s]", key.c_str());
        return ret;
    }

    if (funDesExprStr.empty()) {
        AUTIL_LOG(WARN, "invalid funDes [%s]", funDes.c_str());
        return ret;
    }
    
    for (size_t i = 0; i < _aggResults->size(); ++i) {
        const AggregateResultPtr &resultPtr = (*_aggResults)[i];
        if (!resultPtr) {
            continue;
        }
        if (resultPtr->_groupExprStr != keyExprStr) {
            continue;
        }
        if (!_constructedMap[i]) {
            resultPtr->constructGroupValueMap();
            _constructedMap[i] = true;
        }
        
        uint32_t funCount = resultPtr->getAggFunCount();
        for (uint32_t i = 0; i < funCount; ++i) {
            if (funDesExprStr == resultPtr->getAggFunDesStr(i)) {
                ret.first = &(resultPtr->_exprValue2AggResult);
                ret.second = resultPtr->getAggFunResultReferBase(i);
                return ret;
            }
        }
    }
    
    return ret;
}

} // namespace common
} // namespace isearch

