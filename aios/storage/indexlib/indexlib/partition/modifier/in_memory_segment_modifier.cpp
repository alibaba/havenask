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
#include "indexlib/partition/modifier/in_memory_segment_modifier.h"

#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/index/normal/attribute/accessor/in_memory_attribute_segment_writer.h"
#include "indexlib/index/normal/deletionmap/deletion_map_segment_writer.h"
#include "indexlib/index/normal/inverted_index/accessor/in_memory_index_segment_writer.h"

using namespace std;
using namespace autil;
using namespace indexlib::index;
using namespace indexlib::document;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, InMemorySegmentModifier);

InMemorySegmentModifier::InMemorySegmentModifier() {}

InMemorySegmentModifier::~InMemorySegmentModifier() {}

void InMemorySegmentModifier::Init(DeletionMapSegmentWriterPtr deletionMapSegmentWriter,
                                   InMemoryAttributeSegmentWriterPtr attributeWriters,
                                   InMemoryIndexSegmentWriterPtr indexWriters)
{
    mDeletionMapSegmentWriter = deletionMapSegmentWriter;
    mAttributeWriters = attributeWriters;
    mIndexWriters = indexWriters;
}

bool InMemorySegmentModifier::UpdateDocument(docid_t localDocId, const NormalDocumentPtr& doc)
{
    bool update = false;
    if (mAttributeWriters && mAttributeWriters->UpdateDocument(localDocId, doc)) {
        update = true;
    }
    if (mIndexWriters && mIndexWriters->UpdateDocument(localDocId, doc)) {
        update = true;
    }
    return update;
}

bool InMemorySegmentModifier::UpdateEncodedFieldValue(docid_t docId, fieldid_t fieldId, const StringView& value)
{
    if (mAttributeWriters) {
        return mAttributeWriters->UpdateEncodedFieldValue(docId, fieldId, value);
    }
    IE_LOG(WARN, "no attribute writers");
    return false;
}

void InMemorySegmentModifier::RemoveDocument(docid_t localDocId)
{
    assert(mDeletionMapSegmentWriter);
    mDeletionMapSegmentWriter->Delete(localDocId);
}
}} // namespace indexlib::partition
