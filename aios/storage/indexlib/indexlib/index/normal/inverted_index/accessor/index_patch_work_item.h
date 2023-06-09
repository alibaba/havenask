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
#include "indexlib/index/normal/util/patch_work_item.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(index, DeletionMapReader);
DECLARE_REFERENCE_CLASS(index, InplaceIndexModifier);
DECLARE_REFERENCE_CLASS(index, SingleFieldIndexSegmentPatchIterator);

namespace indexlib { namespace index {

class IndexPatchWorkItem : public PatchWorkItem
{
public:
    IndexPatchWorkItem(const std::string& id, size_t patchItemCount, bool isSub, PatchWorkItemType itemType,
                       const std::shared_ptr<indexlib::index::SingleFieldIndexSegmentPatchIterator>& patchSegIter)
        : PatchWorkItem(id, patchItemCount, isSub, itemType)
        , _patchSegIter(patchSegIter)
        , _hasNext(true)
    {
    }
    ~IndexPatchWorkItem() {}

public:
    bool Init(const index::DeletionMapReaderPtr& deletionMapReader, index::InplaceIndexModifier* indexModifier);
    bool HasNext() const override;
    size_t ProcessNext() override;
    void destroy() override {}
    void drop() override {}

private:
    std::shared_ptr<indexlib::index::SingleFieldIndexSegmentPatchIterator> _patchSegIter;
    DeletionMapReaderPtr _deletionMapReader;
    InplaceIndexModifier* _indexModifier;
    bool _hasNext;

private:
    IE_LOG_DECLARE();
};
}} // namespace indexlib::index
