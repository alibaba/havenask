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
#ifndef __INDEXLIB_MERGER_RESOURCE_H
#define __INDEXLIB_MERGER_RESOURCE_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/util/reclaim_map.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

struct MergerResource {
    MergerResource() : targetSegmentCount(0), isEntireDataSet(false) {}

    ReclaimMapPtr reclaimMap;
    index_base::Version targetVersion;
    size_t targetSegmentCount;
    bool isEntireDataSet;
    std::vector<docid_t> mainBaseDocIds;

    segmentindex_t GetOutputSegmentIndex(docid_t newDocId) const
    {
        assert(reclaimMap.get());
        assert(newDocId < static_cast<int64_t>(reclaimMap->GetNewDocCount()));
        return reclaimMap->GetTargetSegmentIndex(newDocId);
    }
};

DEFINE_SHARED_PTR(MergerResource);
}} // namespace indexlib::index

#endif //__INDEXLIB_MERGER_RESOURCE_H
