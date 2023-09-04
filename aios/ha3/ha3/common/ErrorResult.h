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

#include <memory>
#include <string>
#include <unistd.h>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/common/ErrorDefine.h"

namespace autil {
class DataBuffer;
} // namespace autil

namespace isearch {
namespace common {
#define CDATA_BEGIN_STRING "<![CDATA["
#define CDATA_END_STRING "]]>"

class ErrorResult {
public:
    ErrorResult(ErrorCode errorCode = ERROR_NONE,
                const std::string &errMsg = "",
                const std::string &partitionID = "",
                const std::string &hostName = "");

    ~ErrorResult();

    ErrorResult &operator=(const ErrorResult &other);

public:
    bool hasError() const {
        return _errorCode != ERROR_NONE;
    }

    void setHostInfo(const std::string &partitionID, const std::string &hostName) {
        _partitionID = partitionID;
        _hostName = hostName;
    }

    void setHostInfo(const std::string &partitionID) {
        _partitionID = partitionID;
        char tmpHostname[256];
        // don't care whether error happen
        gethostname(tmpHostname, sizeof(tmpHostname));
        _hostName = tmpHostname;
    }

    const std::string &getPartitionID() const {
        return _partitionID;
    }

    void setHostName(std::string hostName) {
        _hostName = hostName;
    }

    const std::string &getHostName() const {
        return _hostName;
    }

    void setErrorCode(ErrorCode errorCode) {
        _errorCode = errorCode;
    }
    ErrorCode getErrorCode() const {
        return _errorCode;
    }

    void setErrorMsg(const std::string &errorMsg) {
        _errorMsg = errorMsg;
    }
    const std::string &getErrorMsg() const {
        return _errorMsg;
    }

    void resetError(ErrorCode errorCode, const std::string &errMsg = "");
    std::string getErrorDescription() const;

    std::string toXMLString();
    std::string toJsonString();

    void serialize(autil::DataBuffer &dataBuffer) const;
    void deserialize(autil::DataBuffer &dataBuffer);

protected:
    ErrorCode _errorCode;
    std::string _errorMsg;
    std::string _partitionID;
    std::string _hostName;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<ErrorResult> ErrorResultPtr;

} // namespace common
} // namespace isearch
