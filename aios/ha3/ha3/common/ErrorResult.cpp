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
#include "ha3/common/ErrorResult.h"

#include <ostream>

#include "autil/DataBuffer.h"

namespace isearch {
namespace common {
AUTIL_LOG_SETUP(ha3, ErrorResult);
using namespace std;

ErrorResult::ErrorResult(ErrorCode errorCode,
                         const string &errMsg,
                         const string &partitionID,
                         const string &hostName)
    : _errorCode(errorCode)
    , _errorMsg(errMsg)
    , _partitionID(partitionID)
    , _hostName(hostName) {}

ErrorResult &ErrorResult::operator=(const ErrorResult &other) {
    if (this != &other) {
        _errorCode = other._errorCode;
        _errorMsg = other._errorMsg;
        _partitionID = other._partitionID;
        _hostName = other._hostName;
    }
    return *this;
}

ErrorResult::~ErrorResult() {}

void ErrorResult::resetError(ErrorCode errorCode, const string &errMsg) {
    _errorCode = errorCode;
    _errorMsg = errMsg;
}

string ErrorResult::getErrorDescription() const {
    const string &msg = haErrorCodeToString(getErrorCode());
    if (_errorMsg.find(msg) == 0) {
        return _errorMsg;
    }

    if (_errorMsg.empty()) {
        return msg;
    }

    return msg + " " + _errorMsg;
}

string ErrorResult::toXMLString() {
    stringstream ss;
    ss << "<Error>" << endl
       << "\t<PartitionID>" << _partitionID << "</PartitionID>" << endl
       << "\t<HostName>" << _hostName << "</HostName>" << endl
       << "\t<ErrorCode>" << getErrorCode() << "</ErrorCode>" << endl
       << "\t<ErrorDescription>" << CDATA_BEGIN_STRING << getErrorDescription() << CDATA_END_STRING
       << "</ErrorDescription>" << endl
       << "</Error>" << endl;
    return ss.str();
}

string ErrorResult::toJsonString() {
    stringstream ss;
    ss << "{" << endl;
    if (!_partitionID.empty()) {
        ss << "\"partId\": " + _partitionID << "," << endl;
    }
    if (!_hostName.empty()) {
        ss << "\"hostname\": " + _hostName << "," << endl;
    }
    ss << "\"errorCode\": " + std::to_string(getErrorCode()) << "," << endl
       << "\"errorMsg\": " + getErrorDescription() << endl
       << "}" << endl;
    return ss.str();
}

void ErrorResult::serialize(autil::DataBuffer &dataBuffer) const {
    dataBuffer.write(_errorCode);
    dataBuffer.write(_errorMsg);
    dataBuffer.write(_partitionID);
    dataBuffer.write(_hostName);
}

void ErrorResult::deserialize(autil::DataBuffer &dataBuffer) {
    dataBuffer.read(_errorCode);
    dataBuffer.read(_errorMsg);
    dataBuffer.read(_partitionID);
    dataBuffer.read(_hostName);
}

} // namespace common
} // namespace isearch
