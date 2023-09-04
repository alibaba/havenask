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
#include "aios/network/gig/multi_call/java/GigTcpGenerator.h"

#include "autil/StringUtil.h"

using namespace std;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, GigTcpGenerator);

RequestPtr GigTcpGenerator::generateRequest(const std::string &bodyStr,
                                            const GigRequestPlan &requestPlan) {
    GigTcpRequestPtr request(new GigTcpRequest(getProtobufArena()));
    if (requestPlan.has_timeout()) {
        request->setTimeout(requestPlan.timeout());
    }
    if (requestPlan.has_meta_bt()) {
        request->setMetaByTcp(requestPlan.meta_bt());
    }
    request->setBody(bodyStr);
    return request;
}

} // namespace multi_call
