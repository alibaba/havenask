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
#include "ha3/common/CommonDef.h"
#include "ha3/search/QueryExecutor.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/misc/common.h"
#include "matchdoc/CommonDefine.h"
#include "matchdoc/MatchDocAllocator.h" // IWYU pragma: keep

namespace matchdoc {
class MatchDoc;
class ReferenceBase;
template <typename T>
class Reference;
} // namespace matchdoc

namespace isearch {
namespace search {

class MatchDataFetcher {
public:
    MatchDataFetcher();
    virtual ~MatchDataFetcher();

private:
    MatchDataFetcher(const MatchDataFetcher &);
    MatchDataFetcher &operator=(const MatchDataFetcher &);

public:
    virtual matchdoc::ReferenceBase *
    require(matchdoc::MatchDocAllocator *allocator, const std::string &refName, uint32_t termCount)
        = 0;

    virtual indexlib::index::ErrorCode
    fillMatchData(const SingleLayerExecutors &singleLayerExecutors,
                  matchdoc::MatchDoc matchDoc,
                  matchdoc::MatchDoc subDoc) const = 0;

public:
    void setAccTermCount(uint32_t accTermCount) {
        _accTermCount = accTermCount;
    }

protected:
    template <typename MatchDataType>
    matchdoc::Reference<MatchDataType> *createReference(matchdoc::MatchDocAllocator *allocator,
                                                        const std::string &refName,
                                                        uint32_t termCount) {
        return allocator->declare<MatchDataType>(
            refName, common::HA3_MATCHDATA_GROUP, SL_NONE, MatchDataType::sizeOf(termCount));
    }
    template <typename MatchDataType>
    matchdoc::Reference<MatchDataType> *createSubReference(matchdoc::MatchDocAllocator *allocator,
                                                           const std::string &refName,
                                                           uint32_t termCount) {
        return allocator->declareSub<MatchDataType>(
            refName, common::HA3_SUBMATCHDATA_GROUP, SL_NONE, MatchDataType::sizeOf(termCount));
    }

protected:
    uint32_t _termCount;
    uint32_t _accTermCount;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<MatchDataFetcher> MatchDataFetcherPtr;

} // namespace search
} // namespace isearch
