#ifndef __INDEXLIB_BUILDING_SEGMENT_DOCUMENT_DEDUPER_H
#define __INDEXLIB_BUILDING_SEGMENT_DOCUMENT_DEDUPER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/util/expandable_bitmap.h"
#include "indexlib/index/normal/primarykey/primary_key_index_reader_typed.h"
#include "indexlib/index/normal/primarykey/in_mem_primary_key_segment_reader_typed.h"
#include "indexlib/partition/document_deduper/document_deduper.h"
#include "indexlib/partition/modifier/partition_modifier.h"
#include "indexlib/config/index_partition_schema.h"

IE_NAMESPACE_BEGIN(partition);

class BuildingSegmentDocumentDeduper : public DocumentDeduper
{
public:
    BuildingSegmentDocumentDeduper(const config::IndexPartitionSchemaPtr& schema)
        : DocumentDeduper(schema)
    {}
    
    ~BuildingSegmentDocumentDeduper() {}

public:
    void Init(const index_base::InMemorySegmentPtr& buildingSegment,
              const partition::PartitionModifierPtr& modifier)
    {
        mBuildingSegment = buildingSegment;
        mModifier = modifier;
    }
    
    void Dedup() override;

private:
    template<typename Key>
    static void RecordDocids(
        typename index::InMemPrimaryKeySegmentReaderTyped<Key>::Iterator& iter,
        util::ExpandableBitmap& bitmap, docid_t& maxDocid);

    template<typename Key>
    void DedupDocuments();

private:
    index_base::InMemorySegmentPtr mBuildingSegment;
    partition::PartitionModifierPtr mModifier;

private:
    friend class BuildingSegmentDocumentDeduperTest;
    IE_LOG_DECLARE();
};
DEFINE_SHARED_PTR(BuildingSegmentDocumentDeduper);
//////////////////////////////////////////////////////////////
template<typename Key>
inline void BuildingSegmentDocumentDeduper::DedupDocuments()
{
    assert(mBuildingSegment);
    docid_t buildingBaseDocid = mBuildingSegment->GetSegmentData().GetBaseDocId();
    const index::InMemorySegmentReaderPtr& inMemSegReader = mBuildingSegment->GetSegmentReader();
    if (!inMemSegReader)
    {
        return;
    }
    
    index::IndexSegmentReaderPtr pkIndexReader =
        inMemSegReader->GetSingleIndexSegmentReader(
                mSchema->GetIndexSchema()->GetPrimaryKeyIndexConfig()->GetIndexName());

    typedef std::tr1::shared_ptr<index::InMemPrimaryKeySegmentReaderTyped<Key> > InMemPrimaryKeySegmentReaderPtr;
    typedef typename index::InMemPrimaryKeySegmentReaderTyped<Key>::Iterator InMemPrimaryKeySegmentIterator;
    InMemPrimaryKeySegmentReaderPtr inMemPkSegReader = DYNAMIC_POINTER_CAST(
        index::InMemPrimaryKeySegmentReaderTyped<Key>, pkIndexReader);
    InMemPrimaryKeySegmentIterator iter = inMemPkSegReader->CreateIterator();

    util::ExpandableBitmap bitmap;
    docid_t maxDocid;
    RecordDocids<Key>(iter, bitmap, maxDocid);
    for (docid_t docid = 0; docid <= maxDocid; docid++)
    {
        if (!bitmap.Test(docid))
        {
            mModifier->RemoveDocument(docid + buildingBaseDocid);
        }
    }
}

template<typename Key>
inline void BuildingSegmentDocumentDeduper::RecordDocids(
        typename index::InMemPrimaryKeySegmentReaderTyped<Key>::Iterator& iter, 
        util::ExpandableBitmap& bitmap, docid_t& maxDocid)
{
    maxDocid = INVALID_DOCID;
    typename index::InMemPrimaryKeySegmentReaderTyped<Key>::KeyValuePair pair;
    while (iter.HasNext())
    {
        pair = iter.Next();
        docid_t docid = pair.second;
        if (maxDocid < docid)
        {
            maxDocid = docid;
        }
        bitmap.Set(docid);
    }
}


IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_BUILDING_SEGMENT_DOCUMENT_DEDUPER_H
