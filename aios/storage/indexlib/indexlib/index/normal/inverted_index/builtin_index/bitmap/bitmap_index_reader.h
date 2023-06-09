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
#ifndef __INDEXLIB_BITMAP_INDEX_READER_H
#define __INDEXLIB_BITMAP_INDEX_READER_H

#include <memory>

#include "autil/HashAlgorithm.h"
#include "fslib/fslib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_config.h"
#include "indexlib/index/common/DictHasher.h"
#include "indexlib/index/common/DictKeyInfo.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/index/common/Term.h"
#include "indexlib/index/inverted_index/SegmentPosting.h"
#include "indexlib/index/normal/inverted_index/format/index_format_option.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/index_meta/temperature_doc_info.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(index_base, SegmentData);
DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(index_base, InMemorySegment);
DECLARE_REFERENCE_CLASS(index, InMemBitmapIndexSegmentReader);
DECLARE_REFERENCE_CLASS(index, PostingIterator);
DECLARE_REFERENCE_CLASS(file_system, FileReader);
DECLARE_REFERENCE_CLASS(file_system, ResourceFile);
DECLARE_REFERENCE_CLASS(util, BuildResourceMetrics);
DECLARE_REFERENCE_CLASS(util, BuildResourceMetricsNode);

namespace indexlib { namespace index {
struct BitmapPostingExpandData;
class DictionaryReader;
class BitmapPostingIterator;
class BuildingIndexReader;
}} // namespace indexlib::index

namespace indexlib { namespace index { namespace legacy {
class BitmapIndexReader
{
public:
    class TermUpdater
    {
    public:
        TermUpdater() = default;
        ~TermUpdater();
        TermUpdater(const TermUpdater&) = delete;
        TermUpdater(TermUpdater&& other);
        TermUpdater& operator=(TermUpdater&& other);
        TermUpdater(uint8_t* segmentPostingFileBaseAddr, BitmapPostingExpandData* expandData,
                    BitmapIndexReader* reader);
        void Update(docid_t localDocId, bool isDelete);
        bool Empty() const { return _expandData == nullptr; }

    private:
        uint8_t* _segmentPostingFileBaseAddr = 0;
        BitmapPostingExpandData* _expandData = 0;
        BitmapIndexReader* _reader = nullptr;
    };

public:
    BitmapIndexReader();
    BitmapIndexReader(const BitmapIndexReader& other);
    virtual ~BitmapIndexReader();

public:
    // TODO: return bool -> void
    bool Open(const config::IndexConfigPtr& indexConfig, const index_base::PartitionDataPtr& partitionData);
    void InitBuildResourceMetricsNode(util::BuildResourceMetrics* buildResourceMetrics);
    index::Result<PostingIterator*> Lookup(const index::Term& term, uint32_t statePoolSize,
                                           autil::mem_pool::Pool* sessionPool,
                                           file_system::ReadOption option) const noexcept;

    index::Result<PostingIterator*> PartialLookup(const index::Term& term, const DocIdRangeVector& ranges,
                                                  uint32_t statePoolSize, autil::mem_pool::Pool* sessionPool,
                                                  file_system::ReadOption) const noexcept;

    void Update(docid_t docId, const index::DictKeyInfo& key, bool isDelete);

    BitmapIndexReader* Clone() const;

    virtual index::Result<bool> GetSegmentPosting(const index::DictKeyInfo& key, uint32_t segmentIdx,
                                                  SegmentPosting& segPosting,
                                                  file_system::ReadOption option) const noexcept;

    TermUpdater GetTermUpdater(segmentid_t targetSegmentId, index::DictKeyInfo termKey);
    void UpdateBuildResourceMetrics();

public: // for test
    void SetIndexConfig(const config::IndexConfigPtr& indexConfig) { mIndexConfig = indexConfig; }

    virtual index::BitmapPostingIterator* CreateBitmapPostingIterator(autil::mem_pool::Pool* sessionPool) const;

private:
    bool LoadSegments(const index_base::PartitionDataPtr& partitionData);
    void LoadSegment(const index_base::SegmentData& segmentData, std::shared_ptr<DictionaryReader>* dictReader,
                     file_system::FileReaderPtr* postingReader, file_system::ResourceFilePtr* expandFile);
    index::Result<bool> GetSegmentPostingFromIndex(const index::DictKeyInfo& key, uint32_t segmentIdx,
                                                   SegmentPosting& segPosting,
                                                   file_system::ReadOption option) const noexcept;

    index::Result<PostingIterator*> Lookup(const index::DictKeyInfo& key, const DocIdRangeVector& ranges,
                                           uint32_t statePoolSize, autil::mem_pool::Pool* sessionPool,
                                           file_system::ReadOption option) const noexcept;
    void AddInMemSegment(docid_t baseDocId, const index_base::InMemorySegmentPtr& inMemSegment);
    void AddInMemSegmentReader(docid_t baseDocId, const InMemBitmapIndexSegmentReaderPtr& bitmapSegReader);
    index::ErrorCode FillSegmentPostingVector(const index::DictKeyInfo& key, const DocIdRangeVector& ranges,
                                              autil::mem_pool::Pool* sessionPool,
                                              PostingFormatOption bitmapPostingFormatOption,
                                              std::shared_ptr<SegmentPostingVector>& segPostings,
                                              file_system::ReadOption option) const noexcept;

private:
    IndexDictHasher mDictHasher;
    config::IndexConfigPtr mIndexConfig;
    LegacyIndexFormatOption mIndexFormatOption;
    std::vector<std::shared_ptr<DictionaryReader>> mDictReaders;
    std::vector<file_system::FileReaderPtr> mPostingReaders;
    std::vector<file_system::ResourceFilePtr> mExpandResources;
    std::vector<uint64_t> mSegmentDocCount;
    std::vector<docid_t> mBaseDocIds;
    std::vector<segmentid_t> mSegmentIds;
    std::shared_ptr<BuildingIndexReader> mBuildingIndexReader;
    util::BuildResourceMetricsNode* mBuildResourceMetricsNode;
    index_base::TemperatureDocInfoPtr mTemperatureDocInfo;

private:
    friend class BitmapIndexReaderTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(BitmapIndexReader);

inline index::Result<PostingIterator*> BitmapIndexReader::Lookup(const index::Term& term, uint32_t statePoolSize,
                                                                 autil::mem_pool::Pool* sessionPool,
                                                                 file_system::ReadOption option) const noexcept
{
    index::DictKeyInfo key;
    if (!mDictHasher.GetHashKey(term, key)) {
        IE_LOG(WARN, "invalid term [%s], index name [%s]", term.GetWord().c_str(),
               mIndexConfig->GetIndexName().c_str());
        return nullptr;
    }
    return Lookup(key, {}, statePoolSize, sessionPool, option);
}

inline index::Result<PostingIterator*>
BitmapIndexReader::PartialLookup(const index::Term& term, const DocIdRangeVector& ranges, uint32_t statePoolSize,
                                 autil::mem_pool::Pool* sessionPool, file_system::ReadOption option) const noexcept
{
    index::DictKeyInfo key;
    if (!mDictHasher.GetHashKey(term, key)) {
        IE_LOG(WARN, "invalid term [%s], index name [%s]", term.GetWord().c_str(),
               mIndexConfig->GetIndexName().c_str());
        return nullptr;
    }
    return Lookup(key, ranges, statePoolSize, sessionPool, option);
}
}}} // namespace indexlib::index::legacy

#endif //__INDEXLIB_BITMAP_INDEX_READER_H
