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
#pragma once

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/index_config.h"
#include "indexlib/index/normal/primarykey/legacy_primary_key_reader.h"
#include "indexlib/index/normal/primarykey/on_disk_ordered_primary_key_iterator.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/document_deduper/document_deduper.h"
#include "indexlib/partition/modifier/partition_modifier.h"

namespace indexlib { namespace partition {

class BuiltSegmentsDocumentDeduper : public DocumentDeduper
{
public:
    BuiltSegmentsDocumentDeduper(const config::IndexPartitionSchemaPtr& schema) : DocumentDeduper(schema) {}
    ~BuiltSegmentsDocumentDeduper() {}

public:
    void Init(const partition::PartitionModifierPtr& modifier)
    {
        assert(modifier);
        mModifier = modifier;
    }

    void Dedup() override;

private:
    template <typename Key>
    void DedupDocuments();

private:
    partition::PartitionModifierPtr mModifier;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(BuiltSegmentsDocumentDeduper);

///////////////////////////////////////////////////////
template <typename Key>
void BuiltSegmentsDocumentDeduper::DedupDocuments()
{
    using namespace indexlibv2::index;
    typedef PrimaryKeyReader<Key, index::LegacyPrimaryKeyReader<Key>> PKReader;
    DEFINE_SHARED_PTR(PKReader);
    PKReaderPtr pkReader = DYNAMIC_POINTER_CAST(PKReader, mModifier->GetPrimaryKeyIndexReader());
    assert(pkReader);

    std::unique_ptr<OnDiskOrderedPrimaryKeyIterator<Key>> iter = pkReader->LEGACY_CreateOnDiskOrderedIterator();
    typename OnDiskOrderedPrimaryKeyIterator<Key>::PKPairTyped lastPKPair, currentPKPair;
    bool hasLast = false;
    while (iter->HasNext()) {
        if (!iter->Next(currentPKPair)) {
            INDEXLIB_FATAL_ERROR(FileIO, "OnDiskOrderedPrimaryKeyIterator do next failed");
        }
        if (hasLast && lastPKPair.key == currentPKPair.key) {
            mModifier->RemoveDocument(lastPKPair.docid);
        }
        hasLast = true;
        lastPKPair = currentPKPair;
    }
}
}} // namespace indexlib::partition
