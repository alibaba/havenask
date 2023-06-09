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
#include "indexlib/index/normal/summary/in_mem_summary_segment_reader.h"

#include "indexlib/util/Exception.h"

using namespace std;
using namespace indexlib::config;
using namespace indexlib::document;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, InMemSummarySegmentReader);

InMemSummarySegmentReader::InMemSummarySegmentReader(const SummaryGroupConfigPtr& summaryGroupConfig,
                                                     SummaryDataAccessor* accessor)
    : mSummaryGroupConfig(summaryGroupConfig)
    , mAccessor(accessor)
{
}

InMemSummarySegmentReader::~InMemSummarySegmentReader() {}

bool InMemSummarySegmentReader::GetDocument(docid_t localDocId, SearchSummaryDocument* summaryDoc) const
{
    if ((uint64_t)localDocId >= mAccessor->GetDocCount()) {
        return false;
    }

    uint8_t* value = NULL;
    uint32_t size = 0;
    mAccessor->ReadData(localDocId, value, size);

    // TODO: ADD UT
    if (size == 0) {
        return false;
    }

    SummaryGroupFormatter formatter(mSummaryGroupConfig);
    if (formatter.DeserializeSummary(summaryDoc, (char*)value, (size_t)size)) {
        return true;
    }
    INDEXLIB_FATAL_ERROR(IndexCollapsed, "Deserialize in mem summary[docid = %d] FAILED.", localDocId);
    return false;
}
}} // namespace indexlib::index
