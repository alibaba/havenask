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
#include <map>
#include <stddef.h>
#include <stdint.h>

#include "autil/Log.h"
#include "autil/TimeUtility.h"
#include "swift/protocol/AdminRequestResponse.pb.h"
#include "swift/protocol/ErrCode.pb.h"

namespace swift {
namespace admin {

const uint32_t PARTITION_ERROR_QUEUE_SIZE_LIMIT = 1000;
const uint32_t DEFAULT_PARTITION_ERROR_COUNT = 100;

template <typename ErrorKey, typename ErrorValue, typename ErrorResponse>
class ErrorHandlerTemplate {
private:
    typedef std::map<ErrorKey, protocol::ErrorCode> ErrorMap;
    typedef std::deque<ErrorValue> ErrorQueue;

public:
    ErrorHandlerTemplate();
    ~ErrorHandlerTemplate();

private:
    ErrorHandlerTemplate(const ErrorHandlerTemplate &);
    ErrorHandlerTemplate &operator=(const ErrorHandlerTemplate &);

public:
    bool addError(const ErrorKey &key, const ErrorValue &);

    void getError(const protocol::ErrorRequest &request, ErrorResponse &response);

    void setErrorLimit(uint32_t limit);

    void clear();

public:
    // for test
    size_t getErrorQueueSize() const { return _errQueue.size(); }

private:
    protocol::ErrorCode getErrorLevel(const protocol::ErrorRequest &request);

    void choiceByTime(protocol::ErrorCode errorLevel, int64_t time, ErrorResponse &response);
    void choiceByCount(protocol::ErrorCode errorLevel, uint32_t count, ErrorResponse &response);

private:
    ErrorMap _errMap;
    ErrorQueue _errQueue;
    uint32_t _errorLimit;

private:
    friend class PartitionErrorHandlerTest;

private:
    AUTIL_LOG_DECLARE();
};

template <typename ErrorKey, typename ErrorValue, typename ErrorResponse>
ErrorHandlerTemplate<ErrorKey, ErrorValue, ErrorResponse>::ErrorHandlerTemplate() {
    _errorLimit = PARTITION_ERROR_QUEUE_SIZE_LIMIT;
}

template <typename ErrorKey, typename ErrorValue, typename ErrorResponse>
ErrorHandlerTemplate<ErrorKey, ErrorValue, ErrorResponse>::~ErrorHandlerTemplate() {}

template <typename ErrorKey, typename ErrorValue, typename ErrorResponse>
bool ErrorHandlerTemplate<ErrorKey, ErrorValue, ErrorResponse>::addError(const ErrorKey &key, const ErrorValue &value) {
    typename ErrorMap::iterator it = _errMap.find(key);
    if (it != _errMap.end() && it->second == value.errcode()) {
        return false;
    } else if (it == _errMap.end()) {
        _errMap[key] = value.errcode();
    }

    _errQueue.push_back(value);

    if (_errQueue.size() > _errorLimit) {
        _errQueue.pop_front();
    }

    return true;
}

template <typename ErrorKey, typename ErrorValue, typename ErrorResponse>
void ErrorHandlerTemplate<ErrorKey, ErrorValue, ErrorResponse>::getError(const protocol::ErrorRequest &request,
                                                                         ErrorResponse &response) {
    protocol::ErrorCode errorLevel = getErrorLevel(request);

    if (request.has_time()) {
        int64_t timeLimit = autil::TimeUtility::currentTime() - request.time() * 1000 * 1000;
        return choiceByTime(errorLevel, timeLimit, response);
    }
    uint32_t count = DEFAULT_PARTITION_ERROR_COUNT;
    if (request.has_count()) {
        count = request.count();
    }
    return choiceByCount(errorLevel, count, response);
}

template <typename ErrorKey, typename ErrorValue, typename ErrorResponse>
protocol::ErrorCode
ErrorHandlerTemplate<ErrorKey, ErrorValue, ErrorResponse>::getErrorLevel(const protocol::ErrorRequest &request) {
    protocol::ErrorLevel errorLevel = request.level();
    switch (errorLevel) {
    case protocol::ERROR_LEVEL_WARN:
        return protocol::ERROR_WARN_START;
    case protocol::ERROR_LEVEL_FATAL:
        return protocol::ERROR_FATAL_START;
    default:
        return protocol::ERROR_INFO_START;
    }
}

template <typename ErrorKey, typename ErrorValue, typename ErrorResponse>
void ErrorHandlerTemplate<ErrorKey, ErrorValue, ErrorResponse>::choiceByTime(protocol::ErrorCode errorLevel,
                                                                             int64_t timeLimit,
                                                                             ErrorResponse &response) {
    typedef typename ErrorQueue::reverse_iterator ReverseIterator;
    for (ReverseIterator it = _errQueue.rbegin(); it != _errQueue.rend(); ++it) {
        if (it->errtime() < timeLimit) {
            return;
        }
        if (it->errcode() < errorLevel) {
            continue;
        }
        *(response.add_errors()) = *it;
    }
}

template <typename ErrorKey, typename ErrorValue, typename ErrorResponse>
void ErrorHandlerTemplate<ErrorKey, ErrorValue, ErrorResponse>::choiceByCount(protocol::ErrorCode errorLevel,
                                                                              uint32_t count,
                                                                              ErrorResponse &response) {
    uint32_t number = 0;
    typedef typename ErrorQueue::reverse_iterator ReverseIterator;

    for (ReverseIterator it = _errQueue.rbegin(); it != _errQueue.rend(); ++it) {
        if (number >= count) {
            return;
        }
        if (it->errcode() < errorLevel) {
            continue;
        }
        *(response.add_errors()) = *it;
        number++;
    }
}

template <typename ErrorKey, typename ErrorValue, typename ErrorResponse>
void ErrorHandlerTemplate<ErrorKey, ErrorValue, ErrorResponse>::setErrorLimit(uint32_t limit) {
    _errorLimit = limit;
}

template <typename ErrorKey, typename ErrorValue, typename ErrorResponse>
void ErrorHandlerTemplate<ErrorKey, ErrorValue, ErrorResponse>::clear() {
    _errMap.clear();
    _errQueue.clear();
}

} // namespace admin
} // namespace swift
