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
#include "aios/network/gig/multi_call/java/arpc/GigArpcCallBack.h"

#include "autil/StringUtil.h"

using namespace std;
using namespace autil;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, GigArpcCallBack);

GigArpcCallBack::GigArpcCallBack(const std::string &requestStr, const CallBackPtr &callBack,
                                 autil::LockFreeThreadPool *callBackThreadPool)
    : ArpcCallBack(NULL, NULL, callBack, callBackThreadPool)
    , _requestStr(requestStr) {
}

GigArpcCallBack::~GigArpcCallBack() {
}

void GigArpcCallBack::callBack() {
    if (!_callBack->isCopyRequest()) {
        auto ec = MULTI_CALL_ERROR_NONE;
        auto arpcErrorCode = _controller.GetErrorCode();
        if (_controller.Failed()) {
            if (arpc::ARPC_ERROR_TIMEOUT == arpcErrorCode) {
                ec = MULTI_CALL_REPLY_ERROR_TIMEOUT;
            } else {
                ec = MULTI_CALL_REPLY_ERROR_RESPONSE;
            }
            AUTIL_LOG(WARN, "%s", _controller.ErrorText().c_str());
        }
        assert(_callBack);
        const auto &span = _callBack->getSpan();
        if (span) {
            span->setAttribute("gig.arpc.error_code", StringUtil::toString(arpcErrorCode));
        }
        std::string responseInfo;
        ec = getCompatibleInfo(ec, responseInfo);
        _callBack->run(&_responseStr, ec, _controller.ErrorText(), responseInfo);
    }
}

} // namespace multi_call
