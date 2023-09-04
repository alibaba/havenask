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
#include "sql/ops/externalTable/GigQuerySessionClosure.h"

#include <cstddef>
#include <string>
#include <utility>
#include <vector>

#include "autil/StringUtil.h"
#include "autil/result/Errors.h"
#include "autil/result/ForwardList.h"
#include "multi_call/interface/Reply.h"
#include "multi_call/interface/Response.h"
#include "sql/ops/externalTable/GigQuerySessionCallbackCtxR.h"

using namespace std;
using namespace autil::result;

namespace sql {

GigQuerySessionClosure::GigQuerySessionClosure(std::shared_ptr<GigQuerySessionCallbackCtxR> ctx)
    : _ctx(std::move(ctx)) {}

GigQuerySessionClosure::~GigQuerySessionClosure() {}

void GigQuerySessionClosure::Run() {
    doRun();
    delete this;
}

void GigQuerySessionClosure::doRun() {
    if (_reply == nullptr) {
        _ctx->onWaitResponse(RuntimeError::make("gig reply is null"));
        return;
    }
    size_t lackCount = 0;
    vector<multi_call::LackResponseInfo> lackInfos;
    auto responseVec = _reply->getResponses(lackCount, lackInfos);
    if (lackCount != 0) {
        string lackInfoStr;
        for (const auto &lackInfo : lackInfos) {
            lackInfoStr += lackInfo.bizName + " " + autil::StringUtil::toString(lackInfo.version)
                           + " " + autil::StringUtil::toString(lackInfo.partId) + "";
        }
        _ctx->onWaitResponse(RuntimeError::make(
            "lack provider, count [%lu], lack info [%s]", lackCount, lackInfoStr.c_str()));
        return;
    }
    _ctx->onWaitResponse(std::move(responseVec));
}

std::shared_ptr<multi_call::Reply> &GigQuerySessionClosure::getReplyPtr() {
    return _reply;
}

} // namespace sql
