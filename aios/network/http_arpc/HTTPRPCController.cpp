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
#include "HTTPRPCController.h"
#include "Log.h"

using namespace std;

namespace http_arpc {

HTTPRPCController::HTTPRPCController() {
    Reset();
}

HTTPRPCController::~HTTPRPCController() {
}

void HTTPRPCController::Reset()  {
    _reason.clear();
    _cancelList.clear();
    _errorCode = 0;
    _failed = false;
    _canceled = false;
    _queueTime = 0;
}

bool HTTPRPCController::Failed() const {
    return _failed;
}

string HTTPRPCController::ErrorText() const {
    return _reason;
}

void HTTPRPCController::StartCancel() {
    _canceled = true;
    list<RPCClosure*>::const_iterator itEnd = _cancelList.end();
    for (list<RPCClosure*>::const_iterator it = _cancelList.begin ();
         it != itEnd; ++it)
    {
        (*it)->Run();
    }
}

void HTTPRPCController::SetFailed(const string& reason){
    _failed = true;
    _reason = reason;
}

bool HTTPRPCController::IsCanceled() const {
    return _canceled;
}

void HTTPRPCController::NotifyOnCancel(RPCClosure* callback) {
    _cancelList.push_back (callback);
}

void HTTPRPCController::SetErrorCode(int32_t errorCode) {
    _errorCode = errorCode;
}

int32_t HTTPRPCController::GetErrorCode() {
    return _errorCode;
}

void HTTPRPCController::SetQueueTime(int64_t beginTime) {
    _queueTime = beginTime;
}

int64_t HTTPRPCController::GetQueueTime() const {
    return _queueTime;
}

void HTTPRPCController::SetAddr(const string &addr) {
    _addr = addr;
}

const string &HTTPRPCController::GetAddr() const {
    return _addr;
}

}
