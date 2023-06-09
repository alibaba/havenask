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
#include "aios/network/gig/multi_call/interface/Reply.h"
#include "aios/network/gig/multi_call/service/ChildNodeReply.h"
#include "aios/network/gig/multi_call/service/ConnectionManager.h"
#include "autil/StringUtil.h"

using namespace std;
using namespace autil;
namespace multi_call {

AUTIL_LOG_SETUP(multi_call, Reply);

Reply::Reply() {}

Reply::~Reply() {}

vector<ResponsePtr> Reply::getResponses(size_t &lackCount) {
    vector<LackResponseInfo> lackInfos;
    return getResponses(lackCount, lackInfos);
}

vector<ResponsePtr> Reply::getResponses(size_t &lackCount, std::vector<LackResponseInfo> &lackInfos) {
    lackCount = 0;
    vector<ResponsePtr> ret;
    if (_childNodeReply) {
        lackCount = _childNodeReply->fillResponses(ret, lackInfos);
        // report metrics
        _childNodeReply->reportMetrics();
    }
    return ret;
}

map<string, vector<ResponsePtr> > Reply::getResponseMap(size_t &lackCount) {
    auto responseVec = getResponses(lackCount);
    map<string, vector<ResponsePtr> > responseMap;
    for (const auto &response : responseVec) {
        responseMap[response->getBizName()].push_back(response);
    }
    return responseMap;
}

void Reply::setChildNodeReply(const ChildNodeReplyPtr &reply) {
    _childNodeReply = reply;
}

std::string Reply::getErrorMessage(MultiCallErrorCode ec) {
    return translateErrorCode(ec);
}

} // namespace multi_call
