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
#pragma once

#include <deque>
#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Lock.h"
#include "beeper/common/common_type.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/proto/Heartbeat.pb.h"
#include "build_service/util/Log.h"

namespace beeper {
// BS_TYPEDEF_PTR(EventTags);
typedef std::shared_ptr<EventTags> EventTagsPtr;
} // namespace beeper

namespace build_service { namespace proto {
#define REPORT_ERROR(errorCode, errorMsg)                                                                              \
    do {                                                                                                               \
        addErrorInfo(errorCode, errorMsg, BS_RETRY);                                                                   \
        BS_LOG(ERROR, "%s", errorMsg.c_str());                                                                         \
    } while (0)

#define REPORT(errorCode, format, args...)                                                                             \
    do {                                                                                                               \
        reportError(errorCode, format, ##args);                                                                        \
        BS_LOG(ERROR, format, ##args);                                                                                 \
    } while (0)

#define REPORT_ERROR_WITH_ADVICE(errorCode, errorMsg, advice)                                                          \
    do {                                                                                                               \
        addErrorInfo(errorCode, errorMsg, advice);                                                                     \
        BS_LOG(ERROR, "%s", errorMsg.c_str());                                                                         \
    } while (0)

// TODO: ErrorCollector should become globally visible to worker/admin tasks in a different way.
// It's easy to forget to call `fillErrorInfos` explicitly for thiry-party developers.
class ErrorCollector
{
public:
    ErrorCollector();
    virtual ~ErrorCollector();

public:
    static std::string errorInfosToString(std::vector<ErrorInfo>& errorInfos);
    virtual void fillErrorInfos(std::vector<ErrorInfo>& errorInfos) const;
    virtual void fillErrorInfos(std::deque<ErrorInfo>& errorInfos) const;

    virtual void transferErrorInfos(std::vector<proto::ErrorInfo>& errorInfos)
    {
        fillErrorInfos(errorInfos);
        clearErrorInfos();
    }

    proto::ServiceErrorCode getServiceErrorCode() const;

protected:
    ErrorInfo makeErrorInfo(ServiceErrorCode errorCode, const std::string& errorMsg, ErrorAdvice errorAdvice) const;

    void reportError(ServiceErrorCode errorCode, const char* fmt, ...);
    void addErrorInfo(ServiceErrorCode errorCode, const std::string& errorMsg, ErrorAdvice errorAdvice) const;
    void addErrorInfos(const std::vector<ErrorInfo>& errorInfos) const;
    void setMaxErrorCount(uint32_t maxErrorCount);
    void clearErrorInfos() { _errorInfos.clear(); }

    void initBeeperTag(const proto::BuildId& buildId);
    void initBeeperTag(const proto::PartitionId& partitionId);
    void addBeeperTag(const std::string& tagKey, const std::string& tagValue);
    void setBeeperCollector(const std::string& id);

private:
    void rmUselessErrorInfo() const;

protected:
    mutable std::deque<ErrorInfo> _errorInfos;
    mutable autil::ThreadMutex _mutex;
    uint32_t _maxErrorCount;
    beeper::EventTagsPtr _globalTags;
    std::string _beeperCollector;

private:
    static const size_t DEFAULT_MAX_ERROR_LEN = 2048;

private:
    BS_LOG_DECLARE();
};

}} // namespace build_service::proto
