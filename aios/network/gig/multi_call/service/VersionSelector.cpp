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
#include "aios/network/gig/multi_call/service/VersionSelector.h"

#include "autil/StringUtil.h"

using namespace std;
using namespace autil;

namespace multi_call {

bool VersionSelector::needCopy(size_t copyProviderCount,
                               size_t normalProviderCount) {
    if (0 == normalProviderCount) {
        return true;
    }
    auto rangeRandom = _randomGenerator.get() % MAX_WEIGHT;
    auto prob = ((double)copyProviderCount) / normalProviderCount;
    return rangeRandom < prob * MAX_WEIGHT;
}

} // namespace multi_call
