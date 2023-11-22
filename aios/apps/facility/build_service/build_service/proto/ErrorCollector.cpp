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
#include "build_service/proto/ErrorCollector.h"

#include <iosfwd>
#include <stdarg.h>
#include <stdio.h>

#include "autil/TimeUtility.h"
#include "beeper/beeper.h"
#include "build_service/common_define.h"
#include "build_service/proto/ProtoUtil.h"

using namespace std;
using namespace autil;
using namespace beeper;

namespace build_service { namespace proto {
BS_LOG_SETUP(proto, ErrorCollector);

ErrorCollector::ErrorCollector()
{
    _maxErrorCount = MAX_ERROR_COUNT;
    _globalTags.reset(new beeper::EventTags);
}

ErrorCollector::~ErrorCollector() {}

void ErrorCollector::reportError(ServiceErrorCode errorCode, const char* fmt, ...)
{
    char buffer[1024];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(buffer, 1024, fmt, ap);
    va_end(ap);
    std::string msg(buffer);
    addErrorInfo(errorCode, msg, BS_RETRY);
}

void ErrorCollector::addErrorInfo(ServiceErrorCode errorCode, const string& errorMsg, ErrorAdvice errorAdvice) const
{
    ErrorInfo errorInfo = makeErrorInfo(errorCode, errorMsg, errorAdvice);
    autil::ScopedLock lock(_mutex);
    _errorInfos.push_front(errorInfo);
    rmUselessErrorInfo();
}

void ErrorCollector::addErrorInfos(const std::vector<ErrorInfo>& errorInfos) const
{
    autil::ScopedLock lock(_mutex);
    _errorInfos.insert(_errorInfos.end(), errorInfos.begin(), errorInfos.end());
    rmUselessErrorInfo();
}

string ErrorCollector::errorInfosToString(std::vector<ErrorInfo>& errorInfos)
{
    string errorMsg;
    for (auto errorInfo : errorInfos) {
        errorMsg += "[" + errorInfo.errormsg() + "]";
    }
    return errorMsg;
}

void ErrorCollector::rmUselessErrorInfo() const
{
    while (_errorInfos.size() > _maxErrorCount) {
        _errorInfos.pop_back();
    }
}

void ErrorCollector::setMaxErrorCount(uint32_t maxErrorCount)
{
    autil::ScopedLock lock(_mutex);
    _maxErrorCount = maxErrorCount;
}

void ErrorCollector::fillErrorInfos(vector<ErrorInfo>& errorInfos) const
{
    autil::ScopedLock lock(_mutex);
    errorInfos.insert(errorInfos.end(), _errorInfos.begin(), _errorInfos.end());
}

void ErrorCollector::fillErrorInfos(deque<ErrorInfo>& errorInfos) const
{
    autil::ScopedLock lock(_mutex);
    errorInfos.insert(errorInfos.end(), _errorInfos.begin(), _errorInfos.end());
}

ErrorInfo ErrorCollector::makeErrorInfo(ServiceErrorCode errorCode, const string& errorMsg,
                                        ErrorAdvice errorAdvice) const
{
    ErrorInfo errorInfo;
    int64_t ts = TimeUtility::currentTimeInSeconds();
    errorInfo.set_errorcode(errorCode);
    errorInfo.set_errortime(ts);
    errorInfo.set_errormsg(errorMsg.substr(0, DEFAULT_MAX_ERROR_LEN));
    errorInfo.set_advice(errorAdvice);

    EventTags tags;
    _globalTags->MergeTags(&tags);
    tags.AddTag("errorCode", ProtoUtil::getServiceErrorCodeName(errorCode));
    tags.AddTag("errorAdvice", ProtoUtil::getErrorAdviceName(errorAdvice));

    if (errorAdvice == BS_RETRY) {
        BEEPER_INTERVAL_REPORT(10, _beeperCollector, errorMsg, tags);
    } else {
        BEEPER_REPORT(_beeperCollector, errorMsg, tags);
    }
    return errorInfo;
}

ServiceErrorCode ErrorCollector::getServiceErrorCode() const
{
    if (_errorInfos.empty()) {
        return ServiceErrorCode::SERVICE_ERROR_NONE;
    }
    return _errorInfos[0].errorcode();
}

void ErrorCollector::initBeeperTag(const proto::PartitionId& partitionId)
{
    ProtoUtil::partitionIdToBeeperTags(partitionId, *_globalTags);
}

void ErrorCollector::initBeeperTag(const proto::BuildId& buildId)
{
    ProtoUtil::buildIdToBeeperTags(buildId, *_globalTags);
}

void ErrorCollector::addBeeperTag(const string& tagKey, const string& tagValue)
{
    _globalTags->AddTag(tagKey, tagValue);
}

void ErrorCollector::setBeeperCollector(const string& id) { _beeperCollector = id; }

}} // namespace build_service::proto
