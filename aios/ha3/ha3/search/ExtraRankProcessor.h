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

#include "ha3/sorter/SorterResource.h"
#include "autil/Log.h" // IWYU pragma: keep

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
namespace sorter {
class SorterProvider;
}  // namespace sorter
}  // namespace isearch
namespace suez {
namespace turing {
class SorterManager;
class SorterWrapper;
}  // namespace turing
}  // namespace suez

namespace isearch {
namespace request {
} // namespace request
} // namespace isearch

namespace isearch {
namespace search {

class ExtraRankProcessor
{
public:
    ExtraRankProcessor(SearchCommonResource &resource,
                       SearchPartitionResource &partitionResouce,
                       SearchProcessorResource &processorResource);
    ~ExtraRankProcessor();

private:
    ExtraRankProcessor(const ExtraRankProcessor &);
    ExtraRankProcessor& operator=(const ExtraRankProcessor &);

public:
    bool init(const common::Request *request);
    void process(const common::Request *request,
                 InnerSearchResult& result,
                 uint32_t extraRankCount) ;
private:
    sorter::SorterResource createSorterResource(const common::Request *request);
    suez::turing::SorterWrapper *createFinalSorterWrapper(
            const common::Request *request,
            const suez::turing::SorterManager *sorterManager) const;
private:
    SearchCommonResource &_resource;
    SearchPartitionResource &_partitionResource;
    SearchProcessorResource &_processorResource;
    sorter::SorterProvider *_sorterProvider;
    suez::turing::SorterWrapper *_finalSorterWrapper;
private:
    AUTIL_LOG_DECLARE();
};

} // namespace search
} // namespace isearch

