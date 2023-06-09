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
#ifndef __INDEXLIB_BUILDING_SEGMENT_DOCUMENT_DEDUPER_H
#define __INDEXLIB_BUILDING_SEGMENT_DOCUMENT_DEDUPER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index/normal/framework/multi_field_index_segment_reader.h"
#include "indexlib/index/normal/primarykey/in_mem_primary_key_segment_reader_typed.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/segment/in_memory_segment_reader.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/document_deduper/document_deduper.h"
#include "indexlib/partition/modifier/partition_modifier.h"
#include "indexlib/util/ExpandableBitmap.h"

namespace indexlib { namespace partition {

class BuildingSegmentDocumentDeduper : public DocumentDeduper
{
public:
    BuildingSegmentDocumentDeduper(const config::IndexPartitionSchemaPtr& schema) : DocumentDeduper(schema) {}

    ~BuildingSegmentDocumentDeduper() {}

public:
    void Init(const index_base::InMemorySegmentPtr& buildingSegment, const partition::PartitionModifierPtr& modifier)
    {
        mBuildingSegment = buildingSegment;
        mModifier = modifier;
    }

    void Dedup() override;

private:
    template <typename Key>
    static void RecordDocids(typename index::InMemPrimaryKeySegmentReaderTyped<Key>::Iterator& iter,
                             util::ExpandableBitmap& bitmap, docid_t& maxDocid);

    template <typename Key>
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
template <typename Key>
inline void BuildingSegmentDocumentDeduper::DedupDocuments()
{
    assert(mBuildingSegment);
    docid_t buildingBaseDocid = mBuildingSegment->GetSegmentData().GetBaseDocId();
    const index_base::InMemorySegmentReaderPtr& inMemSegReader = mBuildingSegment->GetSegmentReader();
    if (!inMemSegReader) {
        return;
    }
    const std::string& indexName = mSchema->GetIndexSchema()->GetPrimaryKeyIndexConfig()->GetIndexName();
    index::IndexSegmentReaderPtr pkIndexReader =
        inMemSegReader->GetMultiFieldIndexSegmentReader()->GetIndexSegmentReader(indexName);

    typedef std::shared_ptr<index::InMemPrimaryKeySegmentReaderTyped<Key>> InMemPrimaryKeySegmentReaderPtr;
    typedef typename index::InMemPrimaryKeySegmentReaderTyped<Key>::Iterator InMemPrimaryKeySegmentIterator;
    InMemPrimaryKeySegmentReaderPtr inMemPkSegReader =
        DYNAMIC_POINTER_CAST(index::InMemPrimaryKeySegmentReaderTyped<Key>, pkIndexReader);
    InMemPrimaryKeySegmentIterator iter = inMemPkSegReader->CreateIterator();

    util::ExpandableBitmap bitmap;
    docid_t maxDocid;
    RecordDocids<Key>(iter, bitmap, maxDocid);
    for (docid_t docid = 0; docid <= maxDocid; docid++) {
        if (!bitmap.Test(docid)) {
            mModifier->RemoveDocument(docid + buildingBaseDocid);
        }
    }
}

template <typename Key>
inline void
BuildingSegmentDocumentDeduper::RecordDocids(typename index::InMemPrimaryKeySegmentReaderTyped<Key>::Iterator& iter,
                                             util::ExpandableBitmap& bitmap, docid_t& maxDocid)
{
    maxDocid = INVALID_DOCID;
    typename index::InMemPrimaryKeySegmentReaderTyped<Key>::KeyValuePair pair;
    while (iter.HasNext()) {
        pair = iter.Next();
        docid_t docid = pair.second;
        if (maxDocid < docid) {
            maxDocid = docid;
        }
        assert(docid != INVALID_DOCID);
        bitmap.Set(docid);
    }
}
}} // namespace indexlib::partition

#endif //__INDEXLIB_BUILDING_SEGMENT_DOCUMENT_DEDUPER_H
