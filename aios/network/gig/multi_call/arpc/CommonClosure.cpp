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
#include "aios/network/gig/multi_call/arpc/CommonClosure.h"
#include "aios/network/gig/multi_call/agent/QueryInfo.h"
#include "aios/network/http_arpc/HTTPRPCServerClosure.h"
#include "autil/legacy/base64.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;

namespace multi_call {

void CommonClosure::Run() {
    if (_closure) {
        auto httpClosure =
            dynamic_cast<http_arpc::HTTPRPCServerClosure *>(_closure);
        if (httpClosure) {
            const QueryInfoPtr &queryInfo = getQueryInfo();
            if (queryInfo) {
                string responseInfo = queryInfo->finish(
                    (TimeUtility::currentTime() - getStartTime()) /
                        FACTOR_US_TO_MS,
                    getErrorCode(), getTargetWeight());
                httpClosure->addResponseHeader(GIG_DATA,
                                               Base64EncodeFast(responseInfo));
            }
        }

        _closure->Run();
    }
    delete this;
}

} // namespace multi_call
