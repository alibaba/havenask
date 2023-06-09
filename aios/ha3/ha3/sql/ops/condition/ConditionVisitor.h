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
#include <assert.h>
#include <stdarg.h>
#include <stdio.h>
#include <string>

#include "autil/CommonMacros.h"
#include "autil/Log.h"
#include "ha3/sql/common/Log.h" // IWYU pragma: keep

namespace isearch {
namespace sql {
class AndCondition;
class LeafCondition;
class NotCondition;
class OrCondition;
} // namespace sql
} // namespace isearch

namespace isearch {
namespace sql {

class ConditionVisitor {
public:
    ConditionVisitor();
    virtual ~ConditionVisitor();

public:
    virtual void visitAndCondition(AndCondition *condition) = 0;
    virtual void visitOrCondition(OrCondition *condition) = 0;
    virtual void visitNotCondition(NotCondition *condition) = 0;
    virtual void visitLeafCondition(LeafCondition *condition) = 0;

public:
    bool isError() const {
        return _hasError;
    }
    const std::string &errorInfo() const {
        return _errorInfo;
    }
    void setErrorInfo(const char *fmt, ...) {
        const size_t maxMessageLength = 1024;
        char buffer[maxMessageLength];
        va_list ap;
        va_start(ap, fmt);
        vsnprintf(buffer, maxMessageLength, fmt, ap);
        va_end(ap);

        setErrorInfo(std::string(buffer));
    }
    void setErrorInfo(const std::string &errorInfo) {
        assert(!_hasError);
        _hasError = true;
        _errorInfo = errorInfo;
    }

protected:
    bool _hasError;
    std::string _errorInfo;

    AUTIL_LOG_DECLARE();
};

#define CHECK_VISITOR_ERROR(visitor)                                                               \
    do {                                                                                           \
        if (unlikely(visitor.isError())) {                                                         \
            SQL_LOG(ERROR, "visitor has error: %s", visitor.errorInfo().c_str());                  \
            return false;                                                                          \
        }                                                                                          \
    } while (false)

#define CHECK_ERROR_AND_RETRUN()                                                                   \
    do {                                                                                           \
        if (isError()) {                                                                           \
            return;                                                                                \
        }                                                                                          \
    } while (false)

} // namespace sql
} // namespace isearch
