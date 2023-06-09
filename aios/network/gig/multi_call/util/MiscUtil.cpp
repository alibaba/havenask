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
#include "aios/network/gig/multi_call/util/MiscUtil.h"
#include "aios/network/arpc/arpc/common/Exception.h"

using namespace std;
using namespace autil;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, MiscUtil);

MiscUtil::MiscUtil() {}

MiscUtil::~MiscUtil() {}

std::string MiscUtil::createBizName(const std::string &cluster,
                                    const std::string &biz) {
    if (cluster.empty()) {
        return biz;
    } else if (biz.empty()) {
        return cluster;
    } else {
        return biz + GIG_CLUSTER_BIZ_SPLITTER + cluster;
    }
}

float MiscUtil::scaleLinear(uint32_t begin, uint32_t end, float pos) {
    if (pos <= begin || begin > end) {
        return MIN_PERCENT;
    }
    uint32_t range = end - begin;
    if (0 == range) {
        return MAX_PERCENT;
    } else {
        return min(MAX_PERCENT, (pos - begin) / range);
    }
}

std::string MiscUtil::getBackTrace() {
    arpc::common::Exception e;
    e.Init("", "", 0);
    return e.ToString();
}

} // namespace multi_call
