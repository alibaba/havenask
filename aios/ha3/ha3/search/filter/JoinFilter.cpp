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
#include "ha3/search/JoinFilter.h"

#include <iosfwd>
#include <map>
#include <utility>

#include "autil/Log.h"
#include "indexlib/indexlib.h"
#include "suez/turing/expression/framework/JoinDocIdConverterBase.h"
#include "suez/turing/expression/framework/JoinDocIdConverterCreator.h"

using namespace std;
using namespace suez::turing;

namespace isearch {
namespace search {
AUTIL_LOG_SETUP(ha3, JoinFilter);

JoinFilter::JoinFilter(JoinDocIdConverterCreator *convertFactory, bool forceStrongJoin)
    : _convertFactory(convertFactory)
    , _filteredCount(0)
    , _forceStrongJoin(forceStrongJoin) {}

JoinFilter::~JoinFilter() {}

bool JoinFilter::doPass(matchdoc::MatchDoc doc, bool isSub) {
    const auto &convertMap = _convertFactory->getConverterMap();
    auto iter = convertMap.begin();
    for (; iter != convertMap.end(); iter++) {
        JoinDocIdConverterBase *converter = iter->second;
        if (converter->isSubJoin() != isSub) {
            continue;
        }
        if (!_forceStrongJoin && !converter->isStrongJoin()) {
            continue;
        }
        if (converter->convert(doc) == INVALID_DOCID) {
            _filteredCount++;
            return false;
        }
    }
    return true;
}

} // namespace search
} // namespace isearch
