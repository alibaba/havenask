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
#ifndef __INDEXLIB_SINGLE_FIELD_PATCH_WORK_ITEM_H
#define __INDEXLIB_SINGLE_FIELD_PATCH_WORK_ITEM_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/attribute_patch_reader.h"
#include "indexlib/index/normal/attribute/accessor/attribute_patch_work_item.h"
#include "indexlib/index/normal/util/patch_work_item.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/MemBuffer.h"

DECLARE_REFERENCE_CLASS(index, AttributeSegmentReader);

namespace indexlib { namespace index {

class SingleFieldPatchWorkItem : public AttributePatchWorkItem
{
public:
    SingleFieldPatchWorkItem(const std::string& id, size_t patchItemCount, bool isSub, PatchWorkItemType itemType,
                             const AttributeSegmentPatchIteratorPtr& patchSegIterator, docid_t baseDocId,
                             uint32_t fieldId)
        : AttributePatchWorkItem(id, patchItemCount, isSub, itemType)
        , mPatchSegIter(patchSegIterator)
        , mBaseDocId(baseDocId)
        , mFieldId(fieldId)
    {
    }
    ~SingleFieldPatchWorkItem();

public:
    bool Init(const index::DeletionMapReaderPtr& deletionMapReader,
              const index::InplaceAttributeModifierPtr& attrModifier) override;
    bool HasNext() const override;
    size_t ProcessNext() override;

    void destroy() override {}
    void drop() override {}

private:
    AttributeSegmentPatchIteratorPtr mPatchSegIter;
    docid_t mBaseDocId;
    uint32_t mFieldId;
    index::DeletionMapReaderPtr mDeletionMapReader;
    AttributeSegmentReaderPtr mAttrSegReader;
    util::MemBuffer mBuffer;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SingleFieldPatchWorkItem);
}} // namespace indexlib::index

#endif //__INDEXLIB_SINGLE_FIELD_PATCH_WORK_ITEM_H
