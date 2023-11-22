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

#include "indexlib/common_define.h"
#include "indexlib/index/merger_util/truncate/bucket_map.h"
#include "indexlib/index/util/reclaim_map.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace merger {

struct MergePlanResource {
    MergePlanResource(const index::ReclaimMapPtr& reclaimMap = index::ReclaimMapPtr(),
                      const index::ReclaimMapPtr& subReclaimMap = index::ReclaimMapPtr(),
                      const index::legacy::BucketMaps& bucketMaps = index::legacy::BucketMaps())
    {
        this->reclaimMap = reclaimMap;
        this->subReclaimMap = subReclaimMap;
        this->bucketMaps = bucketMaps;
    }
    ~MergePlanResource() {}
    index::ReclaimMapPtr reclaimMap;
    index::ReclaimMapPtr subReclaimMap;
    index::legacy::BucketMaps bucketMaps;
};
}} // namespace indexlib::merger
