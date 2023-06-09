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
#include "indexlib/partition/segment/in_memory_segment_cleaner.h"

#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/partition/segment/in_memory_segment_container.h"

using namespace std;

using namespace indexlib::index_base;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, InMemorySegmentCleaner);

InMemorySegmentCleaner::InMemorySegmentCleaner(const InMemorySegmentContainerPtr& container)
    : mInMemSegContainer(container)
{
}

InMemorySegmentCleaner::~InMemorySegmentCleaner() {}

void InMemorySegmentCleaner::Execute()
{
    if (!mInMemSegContainer) {
        return;
    }
    InMemorySegmentPtr inMemSeg = mInMemSegContainer->GetOldestInMemSegment();
    while (inMemSeg && inMemSeg.use_count() == 2) {
        IE_LOG(INFO, "evict old InMemorySegment[%d]", inMemSeg->GetSegmentId());
        mInMemSegContainer->EvictOldestInMemSegment();
        inMemSeg = mInMemSegContainer->GetOldestInMemSegment();
    }
}
}} // namespace indexlib::partition
