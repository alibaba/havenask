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
#include "indexlib/index/normal/attribute/accessor/single_field_patch_work_item.h"

#include "indexlib/index/normal/attribute/accessor/attribute_segment_reader.h"
#include "indexlib/index/normal/attribute/accessor/inplace_attribute_modifier.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"

using namespace std;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, SingleFieldPatchWorkItem);

SingleFieldPatchWorkItem::~SingleFieldPatchWorkItem() {}

bool SingleFieldPatchWorkItem::Init(const index::DeletionMapReaderPtr& deletionMapReader,
                                    const index::InplaceAttributeModifierPtr& attrModifier)
{
    if (!deletionMapReader) {
        IE_LOG(ERROR, "Init failed: DeletionMap reader is NULL");
        return false;
    }
    if (!attrModifier) {
        IE_LOG(ERROR, "Init failed: InplaceAttributeModifier is NULL");
        return false;
    }
    mDeletionMapReader = deletionMapReader;
    mAttrSegReader = attrModifier->GetAttributeSegmentReader(mFieldId, mBaseDocId);
    // TODO: check attr is updatable

    if (!mAttrSegReader) {
        IE_LOG(ERROR, "get AttributeSegmentReader failed for field[%u], baseDocId[%d]", mFieldId, mBaseDocId);
        return false;
    }
    uint32_t maxItemLen = mPatchSegIter->GetMaxPatchItemLen();
    IE_LOG(INFO, "max patch item length for PatchWorkItem[%s] is %u", _identifier.c_str(), maxItemLen);
    mBuffer.Reserve(maxItemLen);
    return true;
}

bool SingleFieldPatchWorkItem::HasNext() const { return mPatchSegIter->HasNext(); }

size_t SingleFieldPatchWorkItem::ProcessNext()
{
    assert(mPatchSegIter);
    assert(mDeletionMapReader);
    assert(mAttrSegReader);
    assert(mPatchSegIter->HasNext());

    size_t valueSize = 0;
    docid_t localDocId = INVALID_DOCID;
    bool isNull = false;
    valueSize = mPatchSegIter->Next(localDocId, (uint8_t*)mBuffer.GetBuffer(), mBuffer.GetBufferSize(), isNull);
    if (localDocId == INVALID_DOCID || mDeletionMapReader->IsDeleted(mBaseDocId + localDocId)) {
        return 1;
    }
    mAttrSegReader->UpdateField(localDocId, (uint8_t*)mBuffer.GetBuffer(), valueSize, isNull);
    return 1;
}
}} // namespace indexlib::index
