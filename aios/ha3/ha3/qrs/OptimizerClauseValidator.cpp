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
#include "ha3/qrs/OptimizerClauseValidator.h"

#include <cstddef>
#include <string>
#include <vector>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "autil/StringTokenizer.h"
#include "autil/TimeUtility.h"
#include "ha3/common/ErrorDefine.h"
#include "ha3/common/OptimizerClause.h"


using namespace std;
using namespace autil;

using namespace isearch::common;
namespace isearch {
namespace qrs {
AUTIL_LOG_SETUP(ha3, OptimizerClauseValidator);

OptimizerClauseValidator::OptimizerClauseValidator()
    : _errorCode(ERROR_NONE) 
{
}

OptimizerClauseValidator::~OptimizerClauseValidator() { 
}

bool OptimizerClauseValidator::validate(const OptimizerClause* optimizerClause)
{
    if (NULL == optimizerClause){
        return true;
    }
    const string &originalStr = optimizerClause->getOriginalString();
    bool flag = true;
    // split optimizer
    StringTokenizer entities(originalStr, ";",
            StringTokenizer::TOKEN_IGNORE_EMPTY |
            StringTokenizer::TOKEN_TRIM);
    for (auto& each : entities) {
        // split name & option 
        StringTokenizer entity(each, ",",
                    StringTokenizer::TOKEN_IGNORE_EMPTY |
                    StringTokenizer::TOKEN_TRIM);
        if (entity.getNumTokens() < 2) {
            //AUTIL_LOG(WARN, "entity.getNumTokens = %d", entity.getNumTokens());
            flag = false;
            break;
        }
    }
    if (!flag) {
        AUTIL_LOG(WARN, "Missing option of optimizer in optimizerClause[%s]", originalStr.c_str());
        _errorCode = ERROR_OPTIMIZER_CLAUSE_MISSING_OPTION;
        return false;
    }
    
    return true;
}




ErrorCode OptimizerClauseValidator::getErrorCode()
{
    return _errorCode;
}

} // namespace qrs
} // namespace isearch

