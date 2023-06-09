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
#include <string>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/search/MatchDataFetcher.h"
#include "ha3/search/QueryExecutor.h"
#include "ha3/search/SimpleMatchData.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/misc/common.h"
#include "matchdoc/Reference.h"

namespace matchdoc {
class MatchDoc;
class MatchDocAllocator;
}  // namespace matchdoc

namespace isearch {
namespace search {

class SimpleMatchDataFetcher : public MatchDataFetcher
{
public:
    SimpleMatchDataFetcher();
    ~SimpleMatchDataFetcher();
private:
    SimpleMatchDataFetcher(const SimpleMatchDataFetcher &);
    SimpleMatchDataFetcher& operator=(const SimpleMatchDataFetcher &);
public:
    matchdoc::ReferenceBase *require(matchdoc::MatchDocAllocator *allocator,
            const std::string &refName, uint32_t termCount) override;
    indexlib::index::ErrorCode fillMatchData(const SingleLayerExecutors &singleLayerExecutors,
                                                  matchdoc::MatchDoc matchDoc,
                                                  matchdoc::MatchDoc subDoc) const override;
private:
    matchdoc::Reference<rank::SimpleMatchData> *_ref;
private:
    friend class SimpleMatchDataFetcherTest;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SimpleMatchDataFetcher> SimpleMatchDataFetcherPtr;

} // namespace search
} // namespace isearch
