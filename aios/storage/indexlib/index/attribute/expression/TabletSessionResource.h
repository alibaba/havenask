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

#include "expression/common/SessionResource.h"
namespace indexlibv2::framework {
class Segment;
}

namespace indexlibv2::index {
class TabletSessionResource : public expression::SessionResource
{
public:
    TabletSessionResource(autil::mem_pool::Pool* pool, const std::vector<std::shared_ptr<framework::Segment>>& segments)
        : SessionResource()
        , segments(segments)
    {
        assert(pool);
        sessionPool = pool;
        allocator.reset(new matchdoc::MatchDocAllocator(pool));
        location = expression::FL_SEARCHER;
    }
    TabletSessionResource(const TabletSessionResource& other) : SessionResource(other), segments(other.segments) {}
    ~TabletSessionResource() {}

public:
    std::vector<std::shared_ptr<framework::Segment>> segments;
};

}; // namespace indexlibv2::index
