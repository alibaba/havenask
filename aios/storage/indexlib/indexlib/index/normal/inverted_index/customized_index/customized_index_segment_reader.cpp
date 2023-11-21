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
#include "indexlib/index/normal/inverted_index/customized_index/customized_index_segment_reader.h"

#include "indexlib/index_base/segment/in_memory_segment_reader.h"

using namespace std;
using namespace autil::mem_pool;

using namespace indexlib::config;
using namespace indexlib::index_base;
using namespace indexlib::plugin;
using namespace indexlib::file_system;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, CustomizedIndexSegmentReader);

CustomizedIndexSegmentReader::CustomizedIndexSegmentReader() {}

CustomizedIndexSegmentReader::~CustomizedIndexSegmentReader() {}

bool CustomizedIndexSegmentReader::Init(const IndexConfigPtr& indexConfig, const IndexSegmentRetrieverPtr& retriever)
{
    if (!indexConfig) {
        IE_LOG(ERROR, "indexConfig is NULL");
        return false;
    }
    mRetriever = retriever;
    return true;
}

bool CustomizedIndexSegmentReader::GetSegmentPosting(const index::DictKeyInfo& key, docid64_t baseDocId,
                                                     SegmentPosting& segPosting, Pool* sessionPool,
                                                     indexlib::index::InvertedIndexSearchTracer* tracer) const
{
    INDEXLIB_FATAL_ERROR(UnSupported, "unsupported interface!");
    return false;
}
}} // namespace indexlib::index
