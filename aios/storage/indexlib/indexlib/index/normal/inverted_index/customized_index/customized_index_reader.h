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
#ifndef __INDEXLIB_CUSTOMIZED_INDEX_READER_H
#define __INDEXLIB_CUSTOMIZED_INDEX_READER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/normal/framework/legacy_index_reader.h"
#include "indexlib/index/normal/inverted_index/customized_index/customized_index_segment_reader.h"
#include "indexlib/index/normal/inverted_index/customized_index/indexer_define.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(index_base, SegmentData);
DECLARE_REFERENCE_CLASS(index_base, InMemorySegment);
DECLARE_REFERENCE_CLASS(index, IndexRetriever);
namespace indexlib { namespace index {

class CustomizedIndexReader : public index::LegacyIndexReader
{
public:
    CustomizedIndexReader();
    ~CustomizedIndexReader();

public:
    virtual void Open(const std::shared_ptr<config::IndexConfig>& indexConfig,
                      const std::shared_ptr<index_base::PartitionData>& partitionData,
                      const InvertedIndexReader* hintReader = nullptr) override;

    virtual index::Result<PostingIterator*> Lookup(const index::Term& term, uint32_t statePoolSize = 1000,
                                                   PostingType type = pt_default,
                                                   autil::mem_pool::Pool* pool = nullptr) override;

    // pool in LookupAsync and BatchLookup should be thread-safe
    virtual future_lite::coro::Lazy<index::Result<PostingIterator*>>
    LookupAsync(const index::Term* term, uint32_t statePoolSize, PostingType type, autil::mem_pool::Pool* pool,
                file_system::ReadOption option) noexcept override;

    virtual const SectionAttributeReader* GetSectionReader(const std::string& indexName) const override
    {
        assert(false);
        return nullptr;
    }

    virtual std::shared_ptr<KeyIterator> CreateKeyIterator(const std::string& indexName) override
    {
        assert(false);
        return std::shared_ptr<KeyIterator>();
    }

    virtual size_t EstimateLoadSize(const std::shared_ptr<index_base::PartitionData>& partitionData,
                                    const std::shared_ptr<config::IndexConfig>& indexConfig,
                                    const index_base::Version& lastLoadVersion) override
    {
        // TODO: support
        assert(false);
        return 0;
    }

    IndexRetrieverPtr getIndexRetriever() const { return mIndexRetriever; }

private:
    PostingIterator* CreatePostingIterator(const std::vector<SegmentMatchInfo>& segMatchInfos,
                                           autil::mem_pool::Pool* sessionPool);

    IndexSegmentRetrieverPtr LoadBuiltSegmentRetriever(const index_base::SegmentData& segmentData,
                                                       const config::IndexConfigPtr& indexConfig,
                                                       const plugin::PluginManagerPtr& pluginManager);

    IndexSegmentRetrieverPtr LoadBuildingSegmentRetriever(const index_base::SegmentData& segmentData,
                                                          const index_base::InMemorySegmentPtr& inMemSegment);

private:
    IndexRetrieverPtr mIndexRetriever;
    std::vector<segmentid_t> mBuiltSegIds;
    std::vector<segmentid_t> mBuildingSegIds;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(CustomizedIndexReader);
}} // namespace indexlib::index

#endif //__INDEXLIB_CUSTOMIZED_INDEX_READER_H
