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
#include "indexlib/index/normal/summary/in_mem_summary_segment_reader_container.h"

using namespace std;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, InMemSummarySegmentReaderContainer);

InMemSummarySegmentReaderContainer::InMemSummarySegmentReaderContainer() {}

InMemSummarySegmentReaderContainer::~InMemSummarySegmentReaderContainer() {}

const InMemSummarySegmentReaderPtr&
InMemSummarySegmentReaderContainer::GetInMemSummarySegmentReader(summarygroupid_t summaryGroupId) const
{
    assert(summaryGroupId >= 0 && summaryGroupId < (summarygroupid_t)mInMemSummarySegmentReaderVec.size());
    return mInMemSummarySegmentReaderVec[summaryGroupId];
}

void InMemSummarySegmentReaderContainer::AddReader(const SummarySegmentReaderPtr& summarySegmentReader)
{
    InMemSummarySegmentReaderPtr inMemSummarySegmentReader =
        DYNAMIC_POINTER_CAST(InMemSummarySegmentReader, summarySegmentReader);
    mInMemSummarySegmentReaderVec.push_back(inMemSummarySegmentReader);
}
}} // namespace indexlib::index
