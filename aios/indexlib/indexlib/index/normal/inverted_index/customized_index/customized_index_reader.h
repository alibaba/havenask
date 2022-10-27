#ifndef __INDEXLIB_CUSTOMIZED_INDEX_READER_H
#define __INDEXLIB_CUSTOMIZED_INDEX_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/accessor/index_reader.h"
#include "indexlib/index/normal/inverted_index/customized_index/indexer_define.h"
#include "indexlib/index/normal/inverted_index/customized_index/customized_index_segment_reader.h"

DECLARE_REFERENCE_CLASS(index_base, SegmentData);
DECLARE_REFERENCE_CLASS(index_base, InMemorySegment);
DECLARE_REFERENCE_CLASS(index, IndexRetriever);
IE_NAMESPACE_BEGIN(index);

class CustomizedIndexReader : public index::IndexReader
{
public:
    CustomizedIndexReader();
    ~CustomizedIndexReader();
public:
    virtual void Open(const config::IndexConfigPtr& indexConfig,
                      const index_base::PartitionDataPtr& partitionData) override;

    virtual PostingIterator *Lookup(const common::Term& term,
                                    uint32_t statePoolSize = 1000, 
                                    PostingType type = pt_default,
                                    autil::mem_pool::Pool *pool = NULL) override;

    // pool in LookupAsync and BatchLookup should be thread-safe
    virtual future_lite::Future<PostingIterator*> LookupAsync(
                                    const common::Term* term,
                                    uint32_t statePoolSize,
                                    PostingType type,
                                    autil::mem_pool::Pool* pool) override;

    virtual const SectionAttributeReader* GetSectionReader(
            const std::string& indexName) const override
    {
        assert(false);
        return NULL;
    }


    virtual KeyIteratorPtr CreateKeyIterator(const std::string& indexName) override
    {
        assert(false);
        return KeyIteratorPtr();
    }

    // TODO: useless, remove it
    virtual IndexReader* Clone() const override
    {
        assert(false);
        return NULL;
    }

    virtual size_t EstimateLoadSize(const index_base::PartitionDataPtr& partitionData,
                                    const config::IndexConfigPtr& indexConfig,
                                    const index_base::Version& lastLoadVersion) override
    {
        //TODO: support
        assert(false);
        return 0;
    }

    IndexRetrieverPtr getIndexRetriever() const {
        return mIndexRetriever;
    }

private:
    PostingIterator* CreatePostingIterator(
            const std::vector<SegmentMatchInfo>& segMatchInfos,
            autil::mem_pool::Pool* sessionPool);

    IndexSegmentRetrieverPtr LoadBuiltSegmentRetriever(
            const index_base::SegmentData& segmentData,
            const config::IndexConfigPtr& indexConfig,
            const plugin::PluginManagerPtr& pluginManager);

    IndexSegmentRetrieverPtr LoadBuildingSegmentRetriever(
            const index_base::InMemorySegmentPtr& inMemSegment,
            const config::IndexConfigPtr& indexConfig);

private:
    IndexRetrieverPtr mIndexRetriever;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(CustomizedIndexReader);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_CUSTOMIZED_INDEX_READER_H
