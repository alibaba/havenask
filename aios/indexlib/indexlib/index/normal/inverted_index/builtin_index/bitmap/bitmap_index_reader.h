#ifndef __INDEXLIB_BITMAP_INDEX_READER_H
#define __INDEXLIB_BITMAP_INDEX_READER_H

#include <tr1/memory>
#include <autil/HashAlgorithm.h>
#include "fslib/fslib.h"
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/common/term.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/util/key_hasher_factory.h"
#include "indexlib/config/index_config.h"

DECLARE_REFERENCE_CLASS(index_base, SegmentData);
DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(index_base, InMemorySegment);
DECLARE_REFERENCE_CLASS(index, DictionaryReader);
DECLARE_REFERENCE_CLASS(index, InMemBitmapIndexSegmentReader);
DECLARE_REFERENCE_CLASS(index, BitmapPostingIterator);
DECLARE_REFERENCE_CLASS(index, PostingIterator);
DECLARE_REFERENCE_CLASS(index, SegmentPosting);
DECLARE_REFERENCE_CLASS(index, BuildingIndexReader);
DECLARE_REFERENCE_CLASS(file_system, FileReader);

IE_NAMESPACE_BEGIN(index);

class BitmapIndexReader
{
public:
    BitmapIndexReader();
    BitmapIndexReader(const BitmapIndexReader& other);
    virtual ~BitmapIndexReader();

public:
    // TODO: return bool -> void
    bool Open(const config::IndexConfigPtr& indexConfig,
              const index_base::PartitionDataPtr& partitionData);

    PostingIterator *Lookup(const common::Term& term,
                            uint32_t statePoolSize = 1000,
                            autil::mem_pool::Pool *sessionPool = NULL) const;

    BitmapIndexReader* Clone() const;

    virtual bool GetSegmentPosting(dictkey_t key, uint32_t segmentIdx, 
                                   SegmentPosting &segPosting) const;

// for test
public:
    void SetIndexConfig(const config::IndexConfigPtr& indexConfig)
    { mIndexConfig = indexConfig; }

    void AddInMemSegmentReader(docid_t baseDocId,
                               const InMemBitmapIndexSegmentReaderPtr& bitmapSegReader);
private:
    bool LoadSegments(const index_base::PartitionDataPtr& partitionData);
    void LoadSegment(const index_base::SegmentData& segmentData,
                     DictionaryReaderPtr& dictReader,
                     file_system::FileReaderPtr& postingReader);

    PostingIterator *Lookup(uint64_t key, uint32_t statePoolSize = 1000,
                            autil::mem_pool::Pool *sessionPool = NULL) const;

    virtual BitmapPostingIterator* CreateBitmapPostingIterator(
            autil::mem_pool::Pool *sessionPool) const;

    void AddInMemSegment(docid_t baseDocId,
                         const index_base::InMemorySegmentPtr& inMemSegment);
private:
    config::IndexConfigPtr mIndexConfig;
    std::vector<DictionaryReaderPtr> mDictReaders;
    std::vector<file_system::FileReaderPtr> mPostingReaders;
    index_base::SegmentInfos mSegmentInfos;
    std::vector<docid_t> mBaseDocIds;
    util::KeyHasherPtr mHasher;
    index::BuildingIndexReaderPtr mBuildingIndexReader;

private:
    friend class BitmapIndexReaderTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(BitmapIndexReader);

inline PostingIterator *BitmapIndexReader::Lookup(
        const common::Term& term, uint32_t statePoolSize,
        autil::mem_pool::Pool *sessionPool) const
{
    dictkey_t termHashKey;
    if (!mHasher->GetHashKey(term, termHashKey))
    {
        IE_LOG(WARN, "invalid term [%s], index name [%s]",
               term.GetWord().c_str(), mIndexConfig->GetIndexName().c_str());
        return NULL;
    }
    return Lookup(termHashKey, statePoolSize, sessionPool);
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_BITMAP_INDEX_READER_H
