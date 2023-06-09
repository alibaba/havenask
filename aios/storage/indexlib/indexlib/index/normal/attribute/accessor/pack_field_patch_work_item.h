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
#ifndef __INDEXLIB_PACK_FIELD_PATCH_WORK_ITEM_H
#define __INDEXLIB_PACK_FIELD_PATCH_WORK_ITEM_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/attr_field_value.h"
#include "indexlib/index/normal/attribute/accessor/attribute_patch_work_item.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(index, PackFieldPatchIterator);
DECLARE_REFERENCE_CLASS(index, PackAttributeReader);
namespace indexlib { namespace index {

class PackFieldPatchWorkItem : public AttributePatchWorkItem
{
public:
    PackFieldPatchWorkItem(const std::string& id, size_t patchItemCount, bool isSub,
                           PackFieldPatchIterator* packIterator);
    ~PackFieldPatchWorkItem();

public:
    bool Init(const index::DeletionMapReaderPtr& deletionMapReader,
              const index::InplaceAttributeModifierPtr& attrModifier) override;

    void destroy() override {}
    void drop() override {}
    bool HasNext() const override;
    size_t ProcessNext() override;

private:
    PackFieldPatchIterator* mIter;
    index::DeletionMapReaderPtr mDeletionMapReader;
    PackAttributeReaderPtr mPackAttrReader;
    AttrFieldValue mBuffer;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PackFieldPatchWorkItem);
}} // namespace indexlib::index

#endif //__INDEXLIB_PACK_FIELD_PATCH_WORK_ITEM_H
