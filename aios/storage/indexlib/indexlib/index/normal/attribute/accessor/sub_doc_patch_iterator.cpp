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
#include "indexlib/index/normal/attribute/accessor/sub_doc_patch_iterator.h"

#include "indexlib/index/normal/attribute/accessor/attr_field_value.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader_factory.h"

using namespace std;
using namespace indexlib::config;
using namespace indexlib::index_base;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, SubDocPatchIterator);

SubDocPatchIterator::SubDocPatchIterator(const IndexPartitionSchemaPtr& schema)
    : mSchema(schema)
    , mMainIterator(schema)
    , mSubIterator(schema->GetSubIndexPartitionSchema())
{
}

SubDocPatchIterator::~SubDocPatchIterator() {}

void SubDocPatchIterator::Init(const PartitionDataPtr& partitionData, bool ignorePatchToOldIncSegment,
                               const Version& lastLoadVersion, segmentid_t startLoadSegment)
{
    mMainIterator.Init(partitionData, ignorePatchToOldIncSegment, lastLoadVersion, startLoadSegment, false);

    PartitionDataPtr subPartitionData = partitionData->GetSubPartitionData();
    assert(subPartitionData);
    mSubIterator.Init(subPartitionData, ignorePatchToOldIncSegment, lastLoadVersion, startLoadSegment, true);

    mSubJoinAttributeReader =
        AttributeReaderFactory::CreateJoinAttributeReader(mSchema, SUB_DOCID_TO_MAIN_DOCID_ATTR_NAME, partitionData);
    assert(mSubJoinAttributeReader);
}

void SubDocPatchIterator::CreateIndependentPatchWorkItems(vector<PatchWorkItem*>& workItems)
{
    mMainIterator.CreateIndependentPatchWorkItems(workItems);
    mSubIterator.CreateIndependentPatchWorkItems(workItems);
}

void SubDocPatchIterator::Next(AttrFieldValue& value)
{
    if (!HasNext()) {
        // TODO: need assert?
        value.Reset();
        return;
    }
    if (!mMainIterator.HasNext()) {
        assert(mSubIterator.HasNext());
        mSubIterator.Next(value);
        return;
    }

    if (!mSubIterator.HasNext()) {
        assert(mMainIterator.HasNext());
        mMainIterator.Next(value);
        return;
    }

    docid_t mainDocId = mMainIterator.GetCurrentDocId();
    docid_t subDocId = mSubIterator.GetCurrentDocId();
    assert(mainDocId != INVALID_DOCID);
    assert(subDocId != INVALID_DOCID);

    if (LessThen(mainDocId, subDocId)) {
        mMainIterator.Next(value);
    } else {
        mSubIterator.Next(value);
    }
}

bool SubDocPatchIterator::LessThen(docid_t mainDocId, docid_t subDocId) const
{
    docid_t subJoinDocId = mSubJoinAttributeReader->GetJoinDocId(subDocId);
    return mainDocId < subJoinDocId;
}
}} // namespace indexlib::index
