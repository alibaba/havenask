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
#include "ha3/common/MultiErrorResult.h"

#include <memory>
#include <ostream>
#include <string>

#include "autil/DataBuffer.h"
#include "autil/Log.h"
#include "ha3/common/ErrorResult.h"

using namespace std;

namespace isearch {
namespace common {
AUTIL_LOG_SETUP(ha3, MultiErrorResult);


void MultiErrorResult::setHostInfo(const string& partitionID, 
                                   const string& hostName) 
{
    for (size_t i = 0; i < _mErrorResult.size(); ++i) {
        ErrorResult& error = _mErrorResult[i];
        if (error.getPartitionID().empty() && error.getHostName().empty()) {
            error.setHostInfo(partitionID, hostName);
        }
    }
}

void MultiErrorResult::resetAllHostInfo(const string& partitionID, 
                                     const string& hostName) 
{
    for (size_t i = 0; i < _mErrorResult.size(); ++i) {
        ErrorResult& error = _mErrorResult[i];
        error.setHostInfo(partitionID, hostName);
    }
}


void MultiErrorResult::addErrorResult(const MultiErrorResultPtr &multiError){
    ErrorResults errors = multiError->getErrorResults();
    for (size_t i = 0; i < errors.size(); ++i) {
        _mErrorResult.push_back(errors[i]);
    }
}

void MultiErrorResult::addErrorResult(const ErrorResult &error)
{
    if (error.hasError()) {
        _mErrorResult.push_back(error);
    }
}

void MultiErrorResult::addError(const ErrorCode& errorCode, 
                                const string& errorMsg) 
{
    if (errorCode != ERROR_NONE) {
        ErrorResult error(errorCode, errorMsg);
        _mErrorResult.push_back(error);
    }
}

void MultiErrorResult::serialize(autil::DataBuffer &dataBuffer) const
{
    dataBuffer.write(_mErrorResult);
}
 
void MultiErrorResult::deserialize(autil::DataBuffer &dataBuffer)
{
    dataBuffer.read(_mErrorResult);
}

string MultiErrorResult::toXMLString(){
    string errorString;

    if (_mErrorResult.size() == 0 ){
        errorString = string("<Error>"
                             "\n\t<ErrorCode>0</ErrorCode>"
                             "\n\t<ErrorDescription></ErrorDescription>"
                             "\n</Error>\n");
    }
    else{
        errorString = collectErrorString();
        stringstream ss;
        ss << "<Error>"<<endl
           << "\t<ErrorCode>"<< ERROR_GENERAL<<"</ErrorCode>"<<endl
           << "\t<ErrorDescription>" 
           << CDATA_BEGIN_STRING << haErrorCodeToString(ERROR_GENERAL) << CDATA_END_STRING
           <<"</ErrorDescription>"<<endl
           << errorString
           << "</Error>"<<endl;
        errorString = ss.str();
    }

    return errorString;
}

string MultiErrorResult::toDebugString() const{
    string errorString;

    if (_mErrorResult.size() == 0 ){
        errorString = string("No Error");
    }
    else{
        stringstream ss;
        for (size_t i = 0; i < _mErrorResult.size(); ++i) {
            ss << "\nError[" << _mErrorResult[i].getErrorCode() << "] "
               << "Msg[" << _mErrorResult[i].getErrorMsg() << "]";
        }
        errorString = ss.str();
    }

    return errorString;
}

string MultiErrorResult::collectErrorString(){
    stringstream ss;
    for (size_t i = 0; i < _mErrorResult.size(); ++i){
        ss << _mErrorResult[i].toXMLString();
    }

    return ss.str();
}

} // namespace common
} // namespace isearch

