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
#include "ha3/qrs/ConfigClauseValidator.h"

#include <cstddef>
#include <string>

#include "alog/Logger.h"

#include "ha3/common/ConfigClause.h"
#include "ha3/common/ErrorDefine.h"
#include "ha3/isearch.h"
#include "autil/Log.h"

using namespace std;
using namespace isearch::common;
namespace isearch {
namespace qrs {
AUTIL_LOG_SETUP(ha3, ConfigClauseValidator);

ConfigClauseValidator::ConfigClauseValidator(uint32_t retHitsLimit)
    : _errorCode(ERROR_NONE)
{
    _retHitsLimit = retHitsLimit;
}

ConfigClauseValidator::~ConfigClauseValidator() {
}

bool ConfigClauseValidator::validate(const ConfigClause* configClause)
{
    if (NULL == configClause)
    {
        _errorCode = ERROR_NO_CONFIG_CLAUSE;
        return false;
    }

    return (validateRetHitsLimit(configClause) &&
            validateResultFormat(configClause));
}

bool ConfigClauseValidator::validateRetHitsLimit(
        const ConfigClause* configClause)
{
    uint32_t hits = configClause->getStartOffset() +
                    configClause->getHitCount();
    AUTIL_LOG(DEBUG, "hits:[%d], hitsLimit:[%d]", hits, _retHitsLimit);
    if (hits < configClause->getStartOffset()
        || hits > _retHitsLimit)
    {
        _errorCode = ERROR_OVER_RETURN_HITS_LIMIT;
        return false;
    }

    return true;
}

bool ConfigClauseValidator::validateResultFormat(
        const ConfigClause* configClause)
{
    const string &format = configClause->getResultFormatSetting();
    if (format != RESULT_FORMAT_XML &&
        format != RESULT_FORMAT_PROTOBUF &&
        format != RESULT_FORMAT_JSON &&
        (format != RESULT_FORMAT_FB_SUMMARY))
    {
        _errorCode = ERROR_UNSUPPORT_RESULT_FORMAT;
        return false;
    }

    return true;
}

ErrorCode ConfigClauseValidator::getErrorCode()
{
    return _errorCode;
}

} // namespace qrs
} // namespace isearch
