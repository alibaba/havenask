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
#include "indexlib/index/normal/inverted_index/accessor/in_mem_normal_index_segment_reader.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/Exception.h"

namespace indexlib { namespace index {

class InMemMultiShardingIndexSegmentReader : public IndexSegmentReader
{
public:
    InMemMultiShardingIndexSegmentReader(
        const std::vector<std::pair<std::string, std::shared_ptr<IndexSegmentReader>>>& segReaders,
        const std::shared_ptr<index::AttributeSegmentReader>& sectionSegmentReader)
        : mShardingIndexSegReaders(segReaders)
        , mSectionSegmentReader(sectionSegmentReader)
    {
    }

    ~InMemMultiShardingIndexSegmentReader() {}

public:
    std::shared_ptr<index::AttributeSegmentReader> GetSectionAttributeSegmentReader() const override
    {
        return mSectionSegmentReader;
    }

    std::shared_ptr<index::InMemBitmapIndexSegmentReader> GetBitmapSegmentReader() const override
    {
        assert(false);
        return std::shared_ptr<index::InMemBitmapIndexSegmentReader>();
    }

    bool GetSegmentPosting(const index::DictKeyInfo& key, docid64_t baseDocId, SegmentPosting& segPosting,
                           autil::mem_pool::Pool* sessionPool,
                           InvertedIndexSearchTracer* tracer = nullptr) const override
    {
        assert(false);
        return false;
    }

    void Update(docid_t docId, const document::ModifiedTokens& tokens) override
    {
        assert(false);
        INDEXLIB_FATAL_ERROR(InconsistentState, "InMemMultiShardingIndexSegmentReader update called");
    }
    void Update(docid_t docId, const index::DictKeyInfo& key, bool isDelete) override
    {
        assert(false);
        INDEXLIB_FATAL_ERROR(InconsistentState, "InMemMultiShardingIndexSegmentReader update called");
    }

public:
    const std::vector<std::pair<std::string, std::shared_ptr<IndexSegmentReader>>>& GetShardingIndexSegReaders() const
    {
        return mShardingIndexSegReaders;
    }

private:
    std::vector<std::pair<std::string, std::shared_ptr<IndexSegmentReader>>> mShardingIndexSegReaders;
    std::shared_ptr<index::AttributeSegmentReader> mSectionSegmentReader;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(InMemMultiShardingIndexSegmentReader);
}} // namespace indexlib::index
