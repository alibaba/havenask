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

#include <memory>
#include <stdint.h>
#include <string>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/search/MatchData.h"
#include "ha3/search/MatchDataFetcher.h"
#include "ha3/search/QueryExecutor.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/misc/common.h"
#include "matchdoc/Reference.h"

namespace matchdoc {
class MatchDoc;
class MatchDocAllocator;
} // namespace matchdoc

namespace isearch {
namespace search {

class FullMatchDataFetcher : public MatchDataFetcher {
public:
    FullMatchDataFetcher();
    ~FullMatchDataFetcher();

private:
    FullMatchDataFetcher(const FullMatchDataFetcher &);
    FullMatchDataFetcher &operator=(const FullMatchDataFetcher &);

public:
    matchdoc::ReferenceBase *require(matchdoc::MatchDocAllocator *allocator,
                                     const std::string &refName,
                                     uint32_t termCount) override;
    indexlib::index::ErrorCode fillMatchData(const SingleLayerExecutors &singleLayerExecutors,
                                             matchdoc::MatchDoc matchDoc,
                                             matchdoc::MatchDoc subDoc) const override;

private:
    matchdoc::Reference<rank::MatchData> *_ref;
    friend class FullMatchDataFetcherTest;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<FullMatchDataFetcher> FullMatchDataFetcherPtr;

} // namespace search
} // namespace isearch
