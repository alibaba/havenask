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
#include "aios/network/gig/multi_call/java/GigRequestGenerator.h"

using namespace std;

namespace multi_call {

void GigRequestGenerator::generate(PartIdTy partCnt, PartRequestMap &requestMap) {
    if (!_partRequests.empty()) {
        for (auto iter = _partRequests.begin(); iter != _partRequests.end(); ++iter) {
            PartIdTy partId = iter->first;
            if (partId != INVALID_PART_ID && partId < partCnt) {
                requestMap[partId] = iter->second;
            }
        }
    } else {
        for (PartIdTy partId = 0; partId < partCnt; partId++) {
            requestMap[partId] = _request;
        }
    }
}

bool GigRequestGenerator::generatePartRequests(const char *body, int len,
        const GigRequestPlan &requestPlan) {

    std::string bodyStr(body, len);
    _request = generateRequest(bodyStr, requestPlan);
    if (!_request) {
        return false;
    }
    if (requestPlan.has_part_id()) {
        _partRequests[requestPlan.part_id()] = _request;
    }
    for (auto i = 0; i < requestPlan.part_requests_size(); i++) {
        const auto &partRequest = requestPlan.part_requests(i);
        if (!partRequest.has_part_id()) {
            continue;
        }
        PartIdTy partId = partRequest.part_id();
        if (partId == INVALID_PART_ID) {
            continue;
        }
        if (partRequest.has_body()) {
            _partRequests[partId] =
                generateRequest(partRequest.body(), requestPlan);
        } else {
            _partRequests[partId] = _request;
        }
    }
    return true;
}

}
