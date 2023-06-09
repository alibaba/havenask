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
#include "indexlib/index/normal/attribute/accessor/pack_field_patch_iterator.h"

#include <memory>

#include "indexlib/common/field_format/pack_attribute/pack_attribute_formatter.h"
#include "indexlib/index/normal/attribute/accessor/attr_field_value.h"
#include "indexlib/index/normal/attribute/accessor/pack_attr_update_doc_size_calculator.h"
#include "indexlib/index/normal/attribute/accessor/pack_field_patch_work_item.h"
#include "indexlib/index_base/index_meta/version.h"

using namespace std;
using namespace autil;

using namespace indexlib::index_base;
using namespace indexlib::common;
using namespace indexlib::config;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, PackFieldPatchIterator);

PackFieldPatchIterator::~PackFieldPatchIterator()
{
    while (!mHeap.empty()) {
        SingleFieldPatchIterator* iter = mHeap.top();
        mHeap.pop();
        assert(iter);
        delete iter;
    }
}

void PackFieldPatchIterator::Init(const PartitionDataPtr& partitionData, bool ignorePatchToOldIncSegment,
                                  const index_base::Version& lastLoadedVersion, segmentid_t startLoadSegment)
{
    const vector<AttributeConfigPtr>& attrConfVec = mPackAttrConfig->GetAttributeConfigVec();

    for (size_t i = 0; i < attrConfVec.size(); ++i) {
        SingleFieldPatchIterator* singleFieldIter =
            CreateSingleFieldPatchIterator(partitionData, ignorePatchToOldIncSegment, startLoadSegment, attrConfVec[i]);
        if (singleFieldIter) {
            mPatchLoadExpandSize += singleFieldIter->GetPatchLoadExpandSize();
            attrid_t attrId = singleFieldIter->GetAttributeId();
            AttrFieldValuePtr attrValue(new AttrFieldValue);
            singleFieldIter->Reserve(*attrValue);
            if ((size_t)attrId >= mPatchValues.size()) {
                mPatchValues.resize(attrId + 1);
            }
            mPatchValues[attrId] = attrValue;
            mHeap.push(singleFieldIter);
            mPatchItemCount += singleFieldIter->GetPatchItemCount();
        }
    }

    Version diffVersion = partitionData->GetVersion() - lastLoadedVersion;
    mPatchLoadExpandSize += PackAttrUpdateDocSizeCalculator::EsitmateOnePackAttributeUpdateDocSize(
        mPackAttrConfig, partitionData, diffVersion);
    mDocId = GetNextDocId();
}

void PackFieldPatchIterator::CreateIndependentPatchWorkItems(vector<PatchWorkItem*>& workItems)
{
    stringstream ss;
    ss << mPackAttrConfig->GetAttrName() << "_";
    if (mIsSub) {
        ss << "sub";
    } else {
        ss << "main";
    }
    PatchWorkItem* item = new PackFieldPatchWorkItem(ss.str(), mPatchItemCount, mIsSub, this);
    workItems.push_back(item);
}

SingleFieldPatchIterator* PackFieldPatchIterator::CreateSingleFieldPatchIterator(const PartitionDataPtr& partitionData,
                                                                                 bool ignorePatchToOldIncSegment,
                                                                                 segmentid_t startLoadSegment,
                                                                                 const AttributeConfigPtr& attrConf)
{
    if (!attrConf->IsAttributeUpdatable()) {
        return NULL;
    }
    unique_ptr<SingleFieldPatchIterator> singleFieldIter(new SingleFieldPatchIterator(attrConf, mIsSub));
    singleFieldIter->Init(partitionData, ignorePatchToOldIncSegment, startLoadSegment);
    if (!singleFieldIter->HasNext()) {
        return NULL;
    }
    return singleFieldIter.release();
}

void PackFieldPatchIterator::Reserve(AttrFieldValue& value)
{
    PackAttributeFormatter::PackAttributeFields patchFields;
    for (size_t i = 0; i < mPatchValues.size(); i++) {
        if (!mPatchValues[i]) {
            continue;
        }

        uint32_t buffSize = mPatchValues[i]->BufferLength();
        if (buffSize > 0) {
            patchFields.push_back(
                make_pair(INVALID_ATTRID, StringView((const char*)mPatchValues[i]->Data(), buffSize)));
        }
    }
    uint32_t maxLength = PackAttributeFormatter::GetEncodePatchValueLen(patchFields);
    value.ReserveBuffer(maxLength);
    IE_LOG(INFO, "Reserve Buffer size[%lu] for AttrFieldValue", value.BufferLength());
}

void PackFieldPatchIterator::Next(AttrFieldValue& value)
{
    value.SetIsPackAttr(true);
    value.SetIsSubDocId(mIsSub);

    if (!HasNext()) {
        value.SetDocId(INVALID_DOCID);
        value.SetPackAttrId(INVALID_PACK_ATTRID);
        return;
    }

    PackAttributeFormatter::PackAttributeFields patchFields;
    assert(!mHeap.empty());
    do {
        unique_ptr<SingleFieldPatchIterator> iter(mHeap.top());
        assert(iter);
        mHeap.pop();
        attrid_t attrId = iter->GetAttributeId();
        assert(mPatchValues[attrId]);
        iter->Next(*mPatchValues[attrId]);
        patchFields.push_back(make_pair(
            attrId, StringView((const char*)mPatchValues[attrId]->Data(), mPatchValues[attrId]->GetDataSize())));
        if (iter->HasNext()) {
            mHeap.push(iter.release());
        } else {
            iter.reset();
        }
    } while (!mHeap.empty() && mDocId == mHeap.top()->GetCurrentDocId());

    size_t valueLen = PackAttributeFormatter::EncodePatchValues(patchFields, value.Data(), value.BufferLength());
    if (valueLen == 0) {
        value.SetDocId(INVALID_DOCID);
        value.SetPackAttrId(INVALID_PACK_ATTRID);
        IE_LOG(ERROR, "encode patch value for pack attribute [%s] failed.", mPackAttrConfig->GetAttrName().c_str());
        return;
    }

    value.SetDocId(mDocId);
    value.SetPackAttrId(mPackAttrConfig->GetPackAttrId());
    value.SetDataSize(valueLen);
    mDocId = GetNextDocId();
}
}} // namespace indexlib::index
