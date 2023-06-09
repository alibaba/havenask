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

#include <stddef.h>
#include <memory>
#include <string>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/common/ErrorDefine.h"
#include "ha3/common/ErrorResult.h"

namespace autil {
class DataBuffer;
}  // namespace autil

namespace isearch {
namespace common {

class MultiErrorResult;

typedef std::shared_ptr<MultiErrorResult> MultiErrorResultPtr;
typedef std::vector<ErrorResult> ErrorResults;

class MultiErrorResult
{
public:
    MultiErrorResult() {}
    ~MultiErrorResult() {}
public:
    bool hasError() const { 
        return _mErrorResult.size() != 0;
    }
    bool hasErrorCode(ErrorCode ec) const {
        for (size_t i = 0; i < _mErrorResult.size(); ++i) {
            const ErrorResult& error = _mErrorResult[i];
            ErrorCode errorCode = error.getErrorCode();
            if (ec == errorCode) {
                return true;
            }
        }
        return false;
    }

    size_t getErrorCount() const {
        return _mErrorResult.size();        
    }
    const ErrorResults& getErrorResults() const {
        return _mErrorResult;
    }

    void resetAllHostInfo(const std::string& partitionID, 
                          const std::string& hostName = "");
    void setHostInfo(const std::string& partitionID, 
                     const std::string& hostName = "");
    void addError(const ErrorCode& errorCode, const std::string& errorMsg = "");
    void addErrorResult(const MultiErrorResultPtr &multiError);
    void addErrorResult(const ErrorResult &error);

    std::string toXMLString();
    std::string toDebugString() const;
    void serialize(autil::DataBuffer &dataBuffer) const;
    void deserialize(autil::DataBuffer &dataBuffer);

    void resetErrors() {
        _mErrorResult.clear();
    }

private:
    std::string collectErrorString();
private:
    ErrorResults _mErrorResult;
private:
    AUTIL_LOG_DECLARE();
};

} // namespace common
} // namespace isearch

