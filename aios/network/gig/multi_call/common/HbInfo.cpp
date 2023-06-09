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
#include "aios/network/gig/multi_call/common/HbInfo.h"
#include "autil/TimeUtility.h"

using namespace std;

namespace multi_call {

void HbInfo::updateHbMeta(const HbMetaInfoPtr &hbMetaPtr) {
    _lock.lock();
    hbMeta = hbMetaPtr;
    _lock.unlock();

    updateTime();
}

HbMetaInfoPtr HbInfo::getHbMeta() {
    _lock.lock();
    HbMetaInfoPtr tmp = hbMeta; // return a copy
    _lock.unlock();
    return tmp;
}

void HbInfo::updateTime() {
    lastUpdateTime = autil::TimeUtility::currentTime();
}

void HbInfo::updateTime(int64_t time) { lastUpdateTime = time; }

} // namespace multi_call
