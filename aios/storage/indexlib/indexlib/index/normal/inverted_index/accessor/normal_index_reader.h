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
#ifndef __INDEXLIB_NORMAL_INDEX_READER_H
#define __INDEXLIB_NORMAL_INDEX_READER_H

#include <memory>

#include "future_lite/Executor.h"
#include "future_lite/coro/Lazy.h"
#include "indexlib/common_define.h"
#include "indexlib/index/common/DictHasher.h"
#include "indexlib/index/inverted_index/KeyIteratorTyped.h"
#include "indexlib/index/inverted_index/SegmentPosting.h"
#include "indexlib/index/normal/framework/legacy_index_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_index_segment_reader.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/PooledUniquePtr.h"

DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(index_base, InMemorySegment);
DECLARE_REFERENCE_CLASS(index_base, TemperatureDocInfo);
DECLARE_REFERENCE_CLASS(index, IndexMetrics);

namespace indexlib { namespace index { namespace legacy {
class BitmapIndexReader;
class MultiFieldIndexReader;
class DynamicIndexReader;
}}} // namespace indexlib::index::legacy

namespace indexlib { namespace index {
class DictionaryReader;
class SegmentPosting;
class BufferedPostingIterator;
class TermMeta;
class BuildingIndexReader;
class SectionAttributeReader;

class NormalIndexReader : public LegacyIndexReader
{
public:
    NormalIndexReader();
    NormalIndexReader(IndexMetrics* indexMetrics);
    NormalIndexReader(const NormalIndexReader& other) = delete;
    virtual ~NormalIndexReader();

public:
    void Open(const config::IndexConfigPtr& indexConfig, const index_base::PartitionDataPtr& partitionData,
              const InvertedIndexReader* hintReader = nullptr) override;

    void InitBuildResourceMetricsNode(util::BuildResourceMetrics* buildResourceMetrics) override;
    void SetLegacyAccessoryReader(const std::shared_ptr<LegacyIndexAccessoryReader>& accessoryReader) override;

    index::Result<PostingIterator*> Lookup(const index::Term& term, uint32_t statePoolSize = 1000,
                                           PostingType type = pt_default,
                                           autil::mem_pool::Pool* sessionPool = NULL) override;

    future_lite::coro::Lazy<index::Result<PostingIterator*>>
    LookupAsync(const index::Term* term, uint32_t statePoolSize, PostingType type, autil::mem_pool::Pool* pool,
                file_system::ReadOption option) noexcept override;

    index::Result<PostingIterator*> PartialLookup(const index::Term& term, const DocIdRangeVector& ranges,
                                                  uint32_t statePoolSize, PostingType type,
                                                  autil::mem_pool::Pool* sessionPool) override;
    const SectionAttributeReader* GetSectionReader(const std::string& indexName) const override;

    std::shared_ptr<KeyIterator> CreateKeyIterator(const std::string& indexName) override
    {
        return std::shared_ptr<KeyIterator>(new KeyIteratorTyped<NormalIndexReader>(*this));
    }

    void SetMultiFieldIndexReader(InvertedIndexReader* multiFieldIndexReader) override;

    bool GetSegmentPosting(const index::DictKeyInfo& key, uint32_t segmentIdx, SegmentPosting& segPosting,
                           InvertedIndexSearchTracer* tracer = nullptr) override;
    future_lite::coro::Lazy<index::Result<bool>>
    GetSegmentPostingAsync(const index::DictKeyInfo& key, uint32_t segmentIdx, SegmentPosting& segPosting,
                           file_system::ReadOption option,
                           InvertedIndexSearchTracer* tracer = nullptr) noexcept override;

    void Update(docid_t docId, const document::ModifiedTokens& modifiedTokens) override;
    void Update(docid_t docId, index::DictKeyInfo termKey, bool isDelete) override;
    void UpdateIndex(IndexUpdateTermIterator* iterator) override;

    // TODO(xiaohao.yxh) this code will be removed in HA3 3.11 is no perfmance problem
    [[deprecated("use CreateMainPostingIteratorAsync")]] virtual PostingIterator*
    CreateMainPostingIterator(index::DictKeyInfo key, uint32_t statePoolSize, autil::mem_pool::Pool* sessionPool,
                              bool needBuildingSegment);

    // only main chain and no section reader
    future_lite::coro::Lazy<index::Result<PostingIterator*>>
    CreateMainPostingIteratorAsync(index::DictKeyInfo key, uint32_t statePoolSize, autil::mem_pool::Pool* sessionPool,
                                   bool needBuildingSegment, file_system::ReadOption option,
                                   InvertedIndexSearchTracer* tracer = nullptr) noexcept;

    size_t GetTotalPostingFileLength() const;

protected:
    future_lite::coro::Lazy<index::Result<PostingIterator*>>
    CreatePostingIteratorAsync(const index::Term* term, const DocIdRangeVector& ranges, uint32_t statePoolSize,
                               autil::mem_pool::Pool* sessionPool, file_system::ReadOption option) noexcept;

    future_lite::coro::Lazy<index::Result<PostingIterator*>>
    CreatePostingIteratorByHashKey(const index::Term* term, index::DictKeyInfo termHashKey,
                                   const DocIdRangeVector& ranges, uint32_t statePoolSize,
                                   autil::mem_pool::Pool* sessionPool, file_system::ReadOption option) noexcept;

    virtual bool LoadSegments(const index_base::PartitionDataPtr& partitionData,
                              std::vector<NormalIndexSegmentReaderPtr>& segmentReaders,
                              const InvertedIndexReader* hintReader);

    void AddBuildingSegmentReader(docid_t baseDocId, const index_base::InMemorySegmentPtr& inMemSegment);

    bool NeedTruncatePosting(const index::Term& term) const;

    virtual std::shared_ptr<BuildingIndexReader> CreateBuildingIndexReader();

public:
    // public for test
    void SetIndexConfig(const config::IndexConfigPtr& indexConfig)
    {
        _indexConfig = indexConfig;
        _indexFormatOption->Init(indexConfig);
    }

    std::shared_ptr<LegacyIndexAccessoryReader> GetLegacyAccessoryReader() const override { return mAccessoryReader; }

    // only public for key iterator
    // construct vector when calling
    std::vector<std::shared_ptr<DictionaryReader>> GetDictReaders() const;

    void SetVocabulary(const std::shared_ptr<config::HighFrequencyVocabulary>& vol) { this->mHighFreqVol = vol; }

    void SetBitmapIndexReader(legacy::BitmapIndexReader* bitmapIndexReader) { mBitmapIndexReader = bitmapIndexReader; }

protected:
    // virtual bool FillSegmentPosting(const index::Term& term, const index::DictKeyInfo& key, uint32_t segmentIdx,
    //                                 SegmentPosting& segPosting);

    future_lite::coro::Lazy<index::Result<bool>>
    FillSegmentPostingAsync(const index::Term* term, const index::DictKeyInfo& key, uint32_t segmentIdx,
                            SegmentPosting& segPosting, file_system::ReadOption option) noexcept;

    future_lite::coro::Lazy<index::Result<bool>>
    FillTruncSegmentPosting(const index::Term& term, const index::DictKeyInfo& key, uint32_t segmentIdx,
                            SegmentPosting& segPosting, file_system::ReadOption option) noexcept;

    future_lite::coro::Lazy<index::Result<bool>>
    GetSegmentPostingFromTruncIndex(const index::Term& term, const index::DictKeyInfo& key, uint32_t segmentIdx,
                                    file_system::ReadOption option, SegmentPosting& segPosting) noexcept;

    index::Result<bool> GetMainChainTermMeta(const SegmentPosting& mainChainSegPosting, const index::DictKeyInfo& key,
                                             uint32_t segmentIdx, TermMeta& termMeta,
                                             file_system::ReadOption option) noexcept;

    virtual NormalIndexSegmentReaderPtr CreateSegmentReader();

protected:
    virtual BufferedPostingIterator*
    CreateBufferedPostingIterator(autil::mem_pool::Pool* sessionPool,
                                  util::PooledUniquePtr<InvertedIndexSearchTracer> tracer) const;
    bool ValidatePartitonRange(const DocIdRangeVector& ranges);
    DocIdRangeVector MergeDocIdRanges(int32_t hintValues, const DocIdRangeVector& ranges) const;

private:
    future_lite::coro::Lazy<index::Result<PostingIterator*>>
    DoLookupAsync(const index::Term* term, const DocIdRangeVector& ranges, uint32_t statePoolSize, PostingType type,
                  autil::mem_pool::Pool* pool, file_system::ReadOption option) noexcept;

    void FillRangeByBuiltSegments(const index::Term* term, const index::DictKeyInfo& termHashKey,
                                  const DocIdRangeVector& ranges,
                                  std::vector<future_lite::coro::Lazy<index::Result<bool>>>& tasks,
                                  SegmentPostingVector& segPostings, bool& needBuildingSegment,
                                  file_system::ReadOption option) noexcept;
    DocIdRangeVector IntersectDocIdRange(const DocIdRangeVector& currentRange, const DocIdRangeVector& range) const;
    bool IsHighFreqTerm(const index::DictKeyInfo& termKey) const;

protected:
    IndexDictHasher mDictHasher;
    std::vector<NormalIndexSegmentReaderPtr> mSegmentReaders;
    std::shared_ptr<config::HighFrequencyVocabulary> mHighFreqVol;
    legacy::BitmapIndexReader* mBitmapIndexReader;
    legacy::DynamicIndexReader* mDynamicIndexReader;
    std::shared_ptr<LegacyIndexAccessoryReader> mAccessoryReader;
    legacy::MultiFieldIndexReader* mMultiFieldIndexReader;

    std::shared_ptr<BuildingIndexReader> mBuildingIndexReader;

    future_lite::Executor* mExecutor;
    bool mIndexSupportNull;
    IndexMetrics* mIndexMetrics;
    index_base::TemperatureDocInfoPtr mTemperatureDocInfo;

private:
    friend class NormalIndexReaderTest;

private:
    IE_LOG_DECLARE();
};
DEFINE_SHARED_PTR(NormalIndexReader);

//////////////////////////////////////////////////////////////////////////////////
inline bool NormalIndexReader::GetSegmentPosting(const index::DictKeyInfo& key, uint32_t segmentIdx,
                                                 SegmentPosting& segPosting, InvertedIndexSearchTracer* tracer)
{
    // TODO: use session pool when use thread bind pool object
    return mSegmentReaders[segmentIdx]->GetSegmentPosting(key, 0, segPosting, NULL);
}

inline future_lite::coro::Lazy<index::Result<bool>>
NormalIndexReader::GetSegmentPostingAsync(const index::DictKeyInfo& key, uint32_t segmentIdx,
                                          SegmentPosting& segPosting, file_system::ReadOption option,
                                          InvertedIndexSearchTracer* tracer) noexcept
{
    // TODO: use session pool when use thread bind pool object
    if (mExecutor) {
        co_return co_await mSegmentReaders[segmentIdx]->GetSegmentPostingAsync(key, 0, segPosting, NULL, option);
    }
    try {
        bool success = GetSegmentPosting(key, segmentIdx, segPosting);
        co_return success;
    } catch (...) {
        IE_LOG(ERROR, "get segment posting failed");
        co_return ErrorCode::FileIO;
    }
    co_return false;
}
}} // namespace indexlib::index

/////////////////////legacy for test///////////////////////////
namespace indexlib { namespace index {

#define INDEX_MACRO_FOR_TEST(indexName)                                                                                \
    typedef NormalIndexReader indexName##IndexReader;                                                                  \
    DEFINE_SHARED_PTR(indexName##IndexReader);                                                                         \
    typedef KeyIteratorTyped<indexName##IndexReader> indexName##KeyIterator;                                           \
    DEFINE_SHARED_PTR(indexName##KeyIterator);

INDEX_MACRO_FOR_TEST(Text);
INDEX_MACRO_FOR_TEST(String);
INDEX_MACRO_FOR_TEST(Pack);
INDEX_MACRO_FOR_TEST(Expack);

typedef NormalIndexReader NumberInt8IndexReader;
typedef NormalIndexReader NumberUInt8IndexReader;
typedef NormalIndexReader NumberInt16IndexReader;
typedef NormalIndexReader NumberUInt16IndexReader;
typedef NormalIndexReader NumberInt32IndexReader;
typedef NormalIndexReader NumberUInt32IndexReader;
typedef NormalIndexReader NumberInt64IndexReader;
typedef NormalIndexReader NumberUInt64IndexReader;

DEFINE_SHARED_PTR(NumberInt8IndexReader);
DEFINE_SHARED_PTR(NumberUInt8IndexReader);
DEFINE_SHARED_PTR(NumberInt16IndexReader);
DEFINE_SHARED_PTR(NumberUInt16IndexReader);
DEFINE_SHARED_PTR(NumberInt32IndexReader);
DEFINE_SHARED_PTR(NumberUInt32IndexReader);
DEFINE_SHARED_PTR(NumberInt64IndexReader);
DEFINE_SHARED_PTR(NumberUInt64IndexReader);
}} // namespace indexlib::index

#endif //__INDEXLIB_NORMAL_INDEX_READER_H
