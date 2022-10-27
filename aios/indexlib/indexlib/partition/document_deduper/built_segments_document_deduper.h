#ifndef __INDEXLIB_BUILT_SEGMENTS_DOCUMENT_DEDUPER_H
#define __INDEXLIB_BUILT_SEGMENTS_DOCUMENT_DEDUPER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_config.h"
#include "indexlib/partition/modifier/partition_modifier.h"
#include "indexlib/index/normal/primarykey/on_disk_ordered_primary_key_iterator.h"
#include "indexlib/index/normal/primarykey/primary_key_index_reader_typed.h"
#include "indexlib/partition/document_deduper/document_deduper.h"

IE_NAMESPACE_BEGIN(partition);

class BuiltSegmentsDocumentDeduper : public DocumentDeduper
{
public:
    BuiltSegmentsDocumentDeduper(const config::IndexPartitionSchemaPtr& schema)
        : DocumentDeduper(schema)
    {
    }
    ~BuiltSegmentsDocumentDeduper() {}

public:
    void Init(const partition::PartitionModifierPtr& modifier)
    {
        assert(modifier);
        mModifier = modifier;
    }

    void Dedup() override;

private:
    template<typename Key>
    void DedupDocuments();

private:
    partition::PartitionModifierPtr mModifier;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(BuiltSegmentsDocumentDeduper);

///////////////////////////////////////////////////////
template<typename Key>
void BuiltSegmentsDocumentDeduper::DedupDocuments()
{
    typedef index::PrimaryKeyIndexReaderTyped<Key> PKReader;
    DEFINE_SHARED_PTR(PKReader);
    PKReaderPtr pkReader = DYNAMIC_POINTER_CAST(PKReader, 
            mModifier->GetPrimaryKeyIndexReader());

    index::OnDiskOrderedPrimaryKeyIterator<Key> iter = 
        pkReader->CreateOnDiskOrderedIterator();
    typename index::OnDiskOrderedPrimaryKeyIterator<Key>::PKPair lastPKPair;
    if (iter.HasNext())
    {
        iter.Next(lastPKPair);
    }

    typename index::OnDiskOrderedPrimaryKeyIterator<Key>::PKPair currentPKPair;
    while (iter.HasNext())
    {
        iter.Next(currentPKPair);
        if (currentPKPair.key == lastPKPair.key)
        {
            mModifier->RemoveDocument(lastPKPair.docid);
        }
        lastPKPair = currentPKPair;
    }   
}

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_BUILT_SEGMENTS_DOCUMENT_DEDUPER_H
