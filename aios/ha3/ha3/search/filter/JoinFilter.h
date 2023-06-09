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
#pragma once

#include <stdint.h>
#include <memory>

#include "autil/Log.h" // IWYU pragma: keep
#include "matchdoc/MatchDoc.h"

namespace suez {
namespace turing {
class JoinDocIdConverterCreator;
}  // namespace turing
}  // namespace suez

namespace isearch {
namespace search {

class JoinFilter
{
public:
    JoinFilter(suez::turing::JoinDocIdConverterCreator *convertFactory, bool forceStrongJoin);
    ~JoinFilter();
private:
    JoinFilter(const JoinFilter &);
    JoinFilter& operator=(const JoinFilter &);
public:
    bool pass(matchdoc::MatchDoc doc) {
        return doPass(doc, false);
    }
    bool passSubDoc(matchdoc::MatchDoc doc) {
        return doPass(doc, true);
    }
    uint32_t getFilteredCount() const {
        return _filteredCount;
    }
private:
    bool doPass(matchdoc::MatchDoc doc, bool isSub);
private:
    suez::turing::JoinDocIdConverterCreator *_convertFactory;
    uint32_t _filteredCount;
    bool _forceStrongJoin;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<JoinFilter> JoinFilterPtr;

} // namespace search
} // namespace isearch

