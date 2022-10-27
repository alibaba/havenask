#ifndef __INDEXLIB_INDEX_READER_H
#define __INDEXLIB_INDEX_READER_H

#include <tr1/memory>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/common/term.h"
#include "indexlib/config/index_config.h"
#include "indexlib/index/normal/inverted_index/format/index_format_option.h"
#include <future_lite/Future.h>

DECLARE_REFERENCE_CLASS(index, IndexSegmentReader);
DECLARE_REFERENCE_CLASS(index, BitmapIndexReader);
DECLARE_REFERENCE_CLASS(index, PostingIterator);
DECLARE_REFERENCE_CLASS(index, SegmentPosting);
DECLARE_REFERENCE_CLASS(index, IndexAccessoryReader);
DECLARE_REFERENCE_CLASS(index, KeyIterator);
DECLARE_REFERENCE_CLASS(index, SectionAttributeReader);
DECLARE_REFERENCE_CLASS(index_base, Version);
DECLARE_REFERENCE_CLASS(index_base, PartitionData);

IE_NAMESPACE_BEGIN(index);

class IndexReader
{
public:
    struct LookupContext
    {
        common::Term *term = nullptr;
        uint32_t statePoolSize = 1000;
        PostingType type = pt_default;
        LookupContext() = default;
    };

    public : IndexReader();
    virtual ~IndexReader();

public:
    virtual void Open(const config::IndexConfigPtr& indexConfig,
                      const index_base::PartitionDataPtr& partitionData)
    { assert(false); }

    /**
     * Lookup a term in inverted index
     * @param term term to lookup
     * @param statePoolSize pool size of InDocPositionState
     * @param type iterator type[pt_normal, pt_default, pt_bitmap]
     * @param pool session memory pool
     */
    virtual PostingIterator *Lookup(const common::Term& term,
                                    uint32_t statePoolSize = 1000, 
                                    PostingType type = pt_default,
                                    autil::mem_pool::Pool *pool = NULL) = 0;

    // pool in LookupAsync and BatchLookup should be thread-safe
    virtual future_lite::Future<PostingIterator*> LookupAsync(
        const common::Term* term, uint32_t statePoolSize, PostingType type, autil::mem_pool::Pool* pool)
    {
        if (!term)
        {
            return future_lite::makeReadyFuture<PostingIterator*>(nullptr);
        }
        return future_lite::makeReadyFuture<PostingIterator*>(Lookup(*term, statePoolSize, type, pool));
    }

    // ranges must be sorted and do not intersect
    virtual PostingIterator *PartialLookup(const common::Term& term,
                                    const DocIdRangeVector& ranges,
                                    uint32_t statePoolSize,
                                    PostingType type,
                                    autil::mem_pool::Pool *pool)
    {
        return Lookup(term, statePoolSize, type, pool);
    }

    /**
     * Return section reader of specified index.
     */
    virtual const SectionAttributeReader* GetSectionReader(
            const std::string& indexName) const = 0;



    virtual KeyIteratorPtr CreateKeyIterator(const std::string& indexName) = 0;


    // TODO: useless, remove it
    virtual IndexReader* Clone() const = 0;

    virtual size_t EstimateLoadSize(const index_base::PartitionDataPtr& partitionData,
                                    const config::IndexConfigPtr& indexConfig,
                                    const index_base::Version& lastLoadVersion)
    {
        //TODO: support
        assert(false);
        return 0;
    }

public:
    // for test
    virtual PostingIterator *Lookup(dictkey_t key, 
                                    uint32_t statePoolSize = 1000, 
                                    PostingType type = pt_default,
                                    autil::mem_pool::Pool *pool = NULL)
    {
        assert(false);
        return NULL;
    }

public:
    /**
     * Set index accessory reader of specified index.
     */
    virtual void SetAccessoryReader(
            const IndexAccessoryReaderPtr& accessorReader) {}

    /**
     * for test : Get index accessory reader of specified index.
     */
    virtual IndexAccessoryReaderPtr GetAccessoryReader() const
    { return IndexAccessoryReaderPtr(); }

    virtual bool GetSegmentPosting(
            dictkey_t key, uint32_t segmentIdx, SegmentPosting &segPosting)
    {
        assert(false);
        return false;
    }

    virtual void SetMultiFieldIndexReader(IndexReader *truncateReader) {}

    std::string GetIndexName() const
    {
        return mIndexConfig->GetIndexName();
    }
    
    IndexType GetIndexType() const {
        return mIndexConfig->GetIndexType();
    }
    
    /**
     * to convert field names to fieldmap_t
     */
    virtual bool GenFieldMapMask(
            const std::string &indexName,
            const std::vector<std::string>& termFieldNames,
            fieldmap_t &targetFieldMap);
    
    bool HasTermPayload() const { return mIndexFormatOption.HasTermPayload(); }
    bool HasDocPayload() const  { return mIndexFormatOption.HasDocPayload(); }
    bool HasPositionPayload() const  { return mIndexFormatOption.HasPositionPayload(); }

public:
    std::vector<PostingIterator*> BatchLookup(
        const std::vector<LookupContext>& contexts, autil::mem_pool::Pool* pool);

protected:
    index::IndexFormatOption mIndexFormatOption;
    config::IndexConfigPtr mIndexConfig;

private:
    IE_LOG_DECLARE();
};

typedef std::tr1::shared_ptr<IndexReader> IndexReaderPtr;

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_INDEX_READER_H
