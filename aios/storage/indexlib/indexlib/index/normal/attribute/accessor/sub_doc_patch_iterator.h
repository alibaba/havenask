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
#ifndef __INDEXLIB_SUB_DOC_PATCH_ITERATOR_H
#define __INDEXLIB_SUB_DOC_PATCH_ITERATOR_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index/normal/attribute/accessor/join_docid_attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/multi_field_patch_iterator.h"
#include "indexlib/index/normal/attribute/accessor/patch_iterator.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class SubDocPatchIterator : public PatchIterator
{
public:
    SubDocPatchIterator(const config::IndexPartitionSchemaPtr& schema);
    ~SubDocPatchIterator();

public:
    void Init(const index_base::PartitionDataPtr& partitionData, bool ignorePatchToOldIncSegment,
              const index_base::Version& lastLoadVersion, segmentid_t startLoadSegment = INVALID_SEGMENTID);

    void Next(AttrFieldValue& value) override;

    bool HasNext() const override { return mMainIterator.HasNext() || mSubIterator.HasNext(); }

    void Reserve(AttrFieldValue& value) override
    {
        mMainIterator.Reserve(value);
        mSubIterator.Reserve(value);
    }

    size_t GetPatchLoadExpandSize() const override
    {
        return mMainIterator.GetPatchLoadExpandSize() + mSubIterator.GetPatchLoadExpandSize();
    }
    void CreateIndependentPatchWorkItems(std::vector<PatchWorkItem*>& workItems) override;

private:
    bool LessThen(docid_t mainDocId, docid_t subDocId) const;

private:
    config::IndexPartitionSchemaPtr mSchema;
    MultiFieldPatchIterator mMainIterator;
    MultiFieldPatchIterator mSubIterator;
    JoinDocidAttributeReaderPtr mSubJoinAttributeReader;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SubDocPatchIterator);
}} // namespace indexlib::index

#endif //__INDEXLIB_SUB_DOC_PATCH_ITERATOR_H
