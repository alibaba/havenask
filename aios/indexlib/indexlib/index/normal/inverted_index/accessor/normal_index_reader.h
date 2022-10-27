#ifndef __INDEXLIB_NORMAL_INDEX_READER_H
#define __INDEXLIB_NORMAL_INDEX_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/accessor/index_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/key_iterator_typed.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_index_segment_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/segment_posting.h"
#include <future_lite/Future.h>
#include <future_lite/Executor.h>

DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(index_base, InMemorySegment);
DECLARE_REFERENCE_CLASS(index, BufferedPostingIterator);
DECLARE_REFERENCE_CLASS(index, DictionaryReader);
DECLARE_REFERENCE_CLASS(index, SegmentPosting);
DECLARE_REFERENCE_CLASS(index, BitmapIndexReader);
DECLARE_REFERENCE_CLASS(index, SectionAttributeReader);
DECLARE_REFERENCE_CLASS(index, MultiFieldIndexReader);
DECLARE_REFERENCE_CLASS(index, BuildingIndexReader);
DECLARE_REFERENCE_CLASS(index, TermMeta);
DECLARE_REFERENCE_CLASS(util, KeyHasher);

IE_NAMESPACE_BEGIN(index);

class NormalIndexReader : public index::IndexReader
{
public:
    NormalIndexReader();
    NormalIndexReader(const NormalIndexReader& other);
    virtual ~NormalIndexReader();

public:
    
    virtual void Open(const config::IndexConfigPtr& indexConfig,
                      const index_base::PartitionDataPtr& partitionData) override;

    
    const index::SectionAttributeReader* GetSectionReader(
            const std::string& indexName) const override;

    
    void SetAccessoryReader(
            const index::IndexAccessoryReaderPtr &accessoryReader) override;


    index::PostingIterator *Lookup(const common::Term& term,
                            uint32_t statePoolSize = 1000, 
                            PostingType type = pt_default,
                            autil::mem_pool::Pool *sessionPool = NULL) override;

    future_lite::Future<index::PostingIterator*> LookupAsync(const common::Term* term, uint32_t statePoolSize,
        PostingType type, autil::mem_pool::Pool* pool) override;

    index::PostingIterator* PartialLookup(const common::Term& term, const DocIdRangeVector& ranges,
        uint32_t statePoolSize, PostingType type,
        autil::mem_pool::Pool* sessionPool) override;

    index::IndexReader* Clone() const override;

    
    index::KeyIteratorPtr CreateKeyIterator(const std::string& indexName) override
    {
        return index::KeyIteratorPtr(
                new index::KeyIteratorTyped<NormalIndexReader>(*this));
    }

    
    void SetMultiFieldIndexReader(index::IndexReader *multiFieldIndexReader) override;
    
    index::BitmapIndexReader* GetBitmapIndexReader() const
    {
        return mBitmapIndexReader;
    }

    
    bool GetSegmentPosting(dictkey_t key, uint32_t segmentIdx, 
                           index::SegmentPosting &segPosting) override;

    //only main chain and no section reader
    virtual index::PostingIterator *CreateMainPostingIterator(
            dictkey_t key, uint32_t statePoolSize,
            autil::mem_pool::Pool *sessionPool);

protected:
    virtual index::PostingIterator* CreatePostingIterator(const common::Term& term,
        const DocIdRangeVector& ranges, uint32_t statePoolSize,
        autil::mem_pool::Pool* sessionPool);
    future_lite::Future<index::PostingIterator*> CreatePostingIteratorAsync(
        const common::Term* term, const DocIdRangeVector& ranges, uint32_t statePoolSize,
        autil::mem_pool::Pool* sessionPool);

    future_lite::Future<index::PostingIterator*> CreatePostingIteratorByHashKey(
        const common::Term* term, dictkey_t termHashKey, const DocIdRangeVector& ranges,
        uint32_t statePoolSize, autil::mem_pool::Pool* sessionPool);

    virtual bool LoadSegments(const index_base::PartitionDataPtr& partitionData,
                              std::vector<NormalIndexSegmentReaderPtr>& segmentReaders);

    void AddBuildingSegmentReader(docid_t baseDocId,
                                  const index_base::InMemorySegmentPtr& inMemSegment);

    bool NeedTruncatePosting(const common::Term& term) const;
    
    virtual BuildingIndexReaderPtr CreateBuildingIndexReader();

    future_lite::Future<bool>
    GetSegmentPostingAsync(dictkey_t key, uint32_t segmentIdx, index::SegmentPosting& segPosting);

public:
    const config::IndexConfigPtr& GetIndexConfig() const { return mIndexConfig; }

    // public for test
    void SetIndexConfig(const config::IndexConfigPtr& indexConfig) 
    {
        mIndexConfig = indexConfig;
        mIndexFormatOption.Init(indexConfig);
    }

    index::IndexAccessoryReaderPtr GetAccessoryReader() const override
    { return mAccessoryReader; }

    // only public for key iterator
    // construct vector when calling
    std::vector<index::DictionaryReaderPtr> GetDictReaders() const;

    void SetVocabulary(const config::HighFrequencyVocabularyPtr& vol)
    { this->mHighFreqVol = vol;}

    void SetBitmapIndexReader(index::BitmapIndexReader *bitmapIndexReader)
    {
        mBitmapIndexReader = bitmapIndexReader;
    }

protected:
    virtual bool FillSegmentPosting(const common::Term &term, dictkey_t key,
                                    uint32_t segmentIdx, index::SegmentPosting &segPosting);

    future_lite::Future<bool> FillSegmentPostingAsync(const common::Term* term, dictkey_t key,
        uint32_t segmentIdx, index::SegmentPosting& segPosting);

    bool FillTruncSegmentPosting(const common::Term &term, dictkey_t key,
                                 uint32_t segmentIdx, index::SegmentPosting &segPosting);

    bool GetSegmentPostingFromTruncIndex(const common::Term &term, dictkey_t key,
            uint32_t segmentIdx, index::SegmentPosting &segPosting);

    bool GetMainChainTermMeta(const index::SegmentPosting& mainChainSegPosting,
                              dictkey_t key, uint32_t segmentIdx, index::TermMeta &termMeta);
    
    virtual NormalIndexSegmentReaderPtr CreateSegmentReader();
    
protected:
    virtual index::BufferedPostingIterator* CreateBufferedPostingIterator(
            autil::mem_pool::Pool *sessionPool) const;

private:
    future_lite::Future<index::PostingIterator*> DoLookupAsync(const common::Term* term,
        const DocIdRangeVector& ranges, uint32_t statePoolSize, PostingType type,
        autil::mem_pool::Pool* pool);

    void FillRangeByBuiltSegments(const common::Term* term, dictkey_t termHashKey,
        const DocIdRangeVector& ranges, std::vector<future_lite::Future<bool>>& futures,
        index::SegmentPostingVector& segPostings, bool& needBuildingSegment);

protected:
    util::KeyHasherPtr mHasher;
    std::vector<NormalIndexSegmentReaderPtr> mSegmentReaders;

    config::HighFrequencyVocabularyPtr mHighFreqVol;
    index::BitmapIndexReader* mBitmapIndexReader;
    index::IndexAccessoryReaderPtr mAccessoryReader;
    index::MultiFieldIndexReader *mMultiFieldIndexReader;

    BuildingIndexReaderPtr mBuildingIndexReader;

    future_lite::Executor* mExecutor;

private:
    friend class NormalIndexReaderTest;
private:
    IE_LOG_DECLARE();
};
DEFINE_SHARED_PTR(NormalIndexReader);

//////////////////////////////////////////////////////////////////////////////////
inline bool NormalIndexReader::GetSegmentPosting(dictkey_t key, 
        uint32_t segmentIdx, index::SegmentPosting &segPosting)
{
    return mSegmentReaders[segmentIdx]->GetSegmentPosting(key, 0, segPosting, NULL);
}


inline future_lite::Future<bool>
NormalIndexReader::GetSegmentPostingAsync(
        dictkey_t key, uint32_t segmentIdx, index::SegmentPosting& segPosting)
{
    return mSegmentReaders[segmentIdx]->GetSegmentPostingAsync(key, 0, segPosting, NULL);
}

IE_NAMESPACE_END(index);

/////////////////////legacy for test///////////////////////////
IE_NAMESPACE_BEGIN(index);

#define INDEX_MACRO_FOR_TEST(indexName)                                 \
    typedef index::NormalIndexReader indexName##IndexReader;   \
    DEFINE_SHARED_PTR(indexName##IndexReader);                          \
    typedef index::KeyIteratorTyped<indexName##IndexReader> indexName##KeyIterator; \
    DEFINE_SHARED_PTR(indexName##KeyIterator);                          \

INDEX_MACRO_FOR_TEST(Text);
INDEX_MACRO_FOR_TEST(String);
INDEX_MACRO_FOR_TEST(Pack);
INDEX_MACRO_FOR_TEST(Expack);

typedef index::NormalIndexReader NumberInt8IndexReader;
typedef index::NormalIndexReader NumberUInt8IndexReader;
typedef index::NormalIndexReader NumberInt16IndexReader;
typedef index::NormalIndexReader NumberUInt16IndexReader;
typedef index::NormalIndexReader NumberInt32IndexReader;
typedef index::NormalIndexReader NumberUInt32IndexReader;
typedef index::NormalIndexReader NumberInt64IndexReader;
typedef index::NormalIndexReader NumberUInt64IndexReader;

DEFINE_SHARED_PTR(NumberInt8IndexReader);
DEFINE_SHARED_PTR(NumberUInt8IndexReader);
DEFINE_SHARED_PTR(NumberInt16IndexReader);
DEFINE_SHARED_PTR(NumberUInt16IndexReader);
DEFINE_SHARED_PTR(NumberInt32IndexReader);
DEFINE_SHARED_PTR(NumberUInt32IndexReader);
DEFINE_SHARED_PTR(NumberInt64IndexReader);
DEFINE_SHARED_PTR(NumberUInt64IndexReader);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_NORMAL_INDEX_READER_H
