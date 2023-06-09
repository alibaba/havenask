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

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/config/IndexInfoHelper.h"

namespace autil {
namespace mem_pool {
template <typename T> class PoolVector;
}  // namespace mem_pool
}  // namespace autil
namespace isearch {
namespace common {
class Request;
}  // namespace common
namespace search {
class SearchCommonResource;
class SearchPartitionResource;
struct InnerSearchResult;
struct SearchProcessorResource;
}  // namespace search
}  // namespace isearch
namespace matchdoc {
class MatchDoc;
}  // namespace matchdoc

namespace isearch {
namespace rank {
class ComboComparator;

} // namespace rank
} // namespace isearch

namespace isearch {
namespace search {

class MatchDocScorers;

class RerankProcessor
{
public:
    RerankProcessor(SearchCommonResource &resource,
                    SearchPartitionResource &partitionResource,
                    SearchProcessorResource &processorResource);
    ~RerankProcessor();

private:
    RerankProcessor(const RerankProcessor &);
    RerankProcessor& operator=(const RerankProcessor &);

public:
    bool init(const common::Request* request);
    void process(const common::Request *request,
                 bool ranked, const rank::ComboComparator *rankComp,
                 InnerSearchResult& result);

private:
    bool createMatchDocScorers(const common::Request* reques);
    void sortMatchDocs(autil::mem_pool::PoolVector<matchdoc::MatchDoc> &matchDocVect,
                       const rank::ComboComparator *rankComp);

private:
    SearchCommonResource &_resource;
    SearchPartitionResource &_partitionResource;
    SearchProcessorResource &_processorResource;
    MatchDocScorers *_matchDocScorers;
    const config::IndexInfoHelper *_indexInfoHelper;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<RerankProcessor> RerankProcessorPtr;

} // namespace search
} // namespace isearch
