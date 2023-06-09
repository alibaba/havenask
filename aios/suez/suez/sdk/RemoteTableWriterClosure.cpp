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
#include "suez/sdk/RemoteTableWriterClosure.h"

#include "autil/result/Errors.h"
#include "multi_call/interface/Reply.h"
#include "suez/sdk/RemoteTableWriterRequestGenerator.h"
#include "suez/service/Service.pb.h"

using namespace multi_call;
using namespace std;
using namespace autil::result;

namespace suez {

RemoteTableWriterClosure::RemoteTableWriterClosure(std::function<void(autil::result::Result<WriteCallbackParam>)> cb)
    : _cb(std::move(cb)) {}

RemoteTableWriterClosure::~RemoteTableWriterClosure() {}

void RemoteTableWriterClosure::Run() {
    doRun();
    delete this;
}

void RemoteTableWriterClosure::doRun() {
    if (_reply == nullptr) {
        _cb(RuntimeError::make("gig reply is null"));
        return;
    }
    size_t lackCount = 0;
    vector<multi_call::LackResponseInfo> lackInfos;
    const vector<multi_call::ResponsePtr> &responseVec = _reply->getResponses(lackCount, lackInfos);
    if (lackCount != 0) {
        string lackInfoStr;
        for (const auto &lackInfo : lackInfos) {
            lackInfoStr += lackInfo.bizName + " " + autil::StringUtil::toString(lackInfo.version) + " " +
                           autil::StringUtil::toString(lackInfo.partId) + "";
        }
        _cb(RuntimeError::make(
            "lack provider, count [%lu], maybe no leader, lack info [%s]", lackCount, lackInfoStr.c_str()));
        return;
    }
    for (const auto &response : responseVec) {
        if (!getDataFromResponse(response)) {
            return;
        }
    }
    _cb(std::move(_cbParam));
}

bool RemoteTableWriterClosure::getDataFromResponse(const multi_call::ResponsePtr &response) {
    if (response->isFailed()) {
        auto ec = response->getErrorCode();
        _cb(RuntimeError::make("gig has error, error code[%s], errorMsg[%s], response[%s]",
                               multi_call::translateErrorCode(ec),
                               response->errorString().c_str(),
                               response->toString().c_str()));
        return false;
    }
    auto remoteResponse = dynamic_cast<RemoteTableWriterResponse *>(response.get());
    assert(remoteResponse);
    auto writeResponse = dynamic_cast<WriteResponse *>(remoteResponse->getMessage());
    if (writeResponse == nullptr) {
        _cb(RuntimeError::make("get write response message failed, response[%s]", response->toString().c_str()));
        return false;
    }
    assert(!writeResponse->has_errorinfo());
    if (writeResponse->has_checkpoint() && writeResponse->checkpoint() > _cbParam.maxCp) {
        _cbParam.maxCp = writeResponse->checkpoint();
    }
    if (writeResponse->has_buildgap() && writeResponse->buildgap() > _cbParam.maxBuildGap) {
        _cbParam.maxBuildGap = writeResponse->buildgap();
    }
    if (_cbParam.firstResponseInfo.empty()) {
        _cbParam.firstResponseInfo = response->toString();
    }
    return true;
}

std::shared_ptr<multi_call::Reply> &RemoteTableWriterClosure::getReplyPtr() { return _reply; }

} // namespace suez
