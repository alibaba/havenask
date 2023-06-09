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
#include "aios/network/gig/multi_call/java/GigHttpGenerator.h"

using namespace std;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, GigHttpGenerator);

RequestPtr GigHttpGenerator::generateRequest(const std::string &bodyStr,
        const GigRequestPlan &requestPlan) {
    GigHttpRequestPtr request(new GigHttpRequest(getProtobufArena()));
    if (requestPlan.has_timeout()) {
        request->setTimeout(requestPlan.timeout());
    }
    string uri;
    if (requestPlan.has_uri()) {
        uri = requestPlan.uri();
    }
    if (uri.empty()) {
        uri = "/";
    }
    request->setRequestUri(uri);
    const auto &method = requestPlan.method();
    if (method == "POST") {
        request->setHttpMethod(HM_POST);
    } else {
        request->setHttpMethod(HM_GET);
    }

    request->setBody(bodyStr);
    for (auto i = 0; i < requestPlan.headers_size(); i++) {
        request->setRequestHeader(requestPlan.headers(i).key(),
                                  requestPlan.headers(i).value());
    }
    request->setKeepAlive(requestPlan.keep_alive());
    return request;
}

} // namespace multi_call
