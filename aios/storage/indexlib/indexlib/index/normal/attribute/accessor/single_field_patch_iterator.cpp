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
#include "indexlib/index/normal/attribute/accessor/single_field_patch_iterator.h"

#include "indexlib/index/normal/attribute/accessor/attr_field_value.h"
#include "indexlib/index/normal/attribute/accessor/attribute_segment_patch_iterator_creator.h"
#include "indexlib/index/normal/attribute/accessor/single_field_patch_work_item.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_patch_reader.h"
#include "indexlib/index_base/patch/patch_file_filter.h"
#include "indexlib/index_base/patch/patch_file_finder.h"
#include "indexlib/index_base/patch/patch_file_finder_creator.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace indexlib::index_base;
using namespace indexlib::util;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, SingleFieldPatchIterator);

SingleFieldPatchIterator::SingleFieldPatchIterator(const config::AttributeConfigPtr& attrConfig, bool isSub)
    : AttributePatchIterator(APT_SINGLE, isSub)
    , mAttrConfig(attrConfig)
    , mCursor(0)
    , mPatchLoadExpandSize(0)
    , mPatchItemCount(0)
{
    assert(mAttrConfig);
    assert(mAttrConfig->IsAttributeUpdatable());
    mAttrIdentifier = mAttrConfig->GetFieldId();
}

SingleFieldPatchIterator::~SingleFieldPatchIterator() {}

void SingleFieldPatchIterator::Init(const PartitionDataPtr& partitionData, bool isIncConsistentWithRealtime,
                                    segmentid_t startLoadSegment)
{
    PatchInfos attrPatchInfos;
    PatchFileFinderPtr patchFinder = PatchFileFinderCreator::Create(partitionData.get());

    patchFinder->FindAttrPatchFiles(mAttrConfig, &attrPatchInfos);

    PatchFileFilter attrPatchFilter(partitionData, isIncConsistentWithRealtime, startLoadSegment);
    PatchInfos validPatchInfos = attrPatchFilter.Filter(attrPatchInfos);
    PatchInfos::const_iterator iter = validPatchInfos.begin();
    for (; iter != validPatchInfos.end(); iter++) {
        AttributeSegmentPatchIteratorPtr segmentPatchReader = CreateSegmentPatchIterator(iter->second);
        if (segmentPatchReader->HasNext()) {
            if (mAttrConfig->IsLoadPatchExpand()) {
                mPatchLoadExpandSize += segmentPatchReader->GetPatchFileLength();
            }
            SegmentData segmentData = partitionData->GetSegmentData(iter->first);
            mPatchItemCount += segmentPatchReader->GetPatchItemCount();
            mSegmentPatchIteratorInfos.push_back(make_pair(segmentData.GetBaseDocId(), segmentPatchReader));
        }
    }

    mDocId = GetNextDocId();
}

void SingleFieldPatchIterator::CreateIndependentPatchWorkItems(vector<PatchWorkItem*>& workItems)
{
    for (const auto& segPatchIterInfo : mSegmentPatchIteratorInfos) {
        stringstream ss;
        ss << mAttrConfig->GetAttrName() << "_" << segPatchIterInfo.first << "_";
        if (mIsSub) {
            ss << "sub";
        } else {
            ss << "main";
        }
        PatchWorkItem::PatchWorkItemType itemType =
            mAttrConfig->IsLengthFixed() ? PatchWorkItem::PWIT_FIX_LENGTH : PatchWorkItem::PWIT_VAR_NUM;
        PatchWorkItem* item =
            new SingleFieldPatchWorkItem(ss.str(), segPatchIterInfo.second->GetPatchItemCount(), mIsSub, itemType,
                                         segPatchIterInfo.second, segPatchIterInfo.first, mAttrIdentifier);
        workItems.push_back(item);
    }
}

docid_t SingleFieldPatchIterator::CalculateGlobalDocId() const
{
    docid_t baseDocid = mSegmentPatchIteratorInfos[mCursor].first;
    const AttributeSegmentPatchIteratorPtr& segmentPatchReader = mSegmentPatchIteratorInfos[mCursor].second;
    return baseDocid + segmentPatchReader->GetCurrentDocId();
}

PatchFileInfoVec SingleFieldPatchIterator::FilterLoadedPatchFileInfos(PatchFileInfoVec& patchInfosVec,
                                                                      segmentid_t startLoadSegment)
{
    PatchFileInfoVec validPatchInfos;
    for (size_t i = 0; i < patchInfosVec.size(); i++) {
        if (patchInfosVec[i].srcSegment >= startLoadSegment) {
            validPatchInfos.push_back(patchInfosVec[i]);
        }
    }
    return validPatchInfos;
}

AttributeSegmentPatchIteratorPtr
SingleFieldPatchIterator::CreateSegmentPatchIterator(const PatchFileInfoVec& patchInfoVec)
{
    AttributeSegmentPatchIteratorPtr patchIter(AttributeSegmentPatchIteratorCreator::Create(mAttrConfig));
    assert(patchIter);
    for (size_t i = 0; i < patchInfoVec.size(); i++) {
        patchIter->AddPatchFile(patchInfoVec[i].patchDirectory, patchInfoVec[i].patchFileName,
                                patchInfoVec[i].srcSegment);
    }
    return patchIter;
}

void SingleFieldPatchIterator::Next(AttrFieldValue& value)
{
    assert(HasNext());
    const AttributeSegmentPatchIteratorPtr& patchIter = mSegmentPatchIteratorInfos[mCursor].second;

    size_t valueSize = 0;
    docid_t docid = INVALID_DOCID;
    bool isNull = false;
    valueSize = patchIter->Next(docid, value.Data(), value.BufferLength(), isNull);
    docid += mSegmentPatchIteratorInfos[mCursor].first;

    value.SetFieldId(mAttrIdentifier);
    value.SetDocId(docid);
    value.SetIsPackAttr(false);
    value.SetDataSize(valueSize);
    value.SetIsSubDocId(mIsSub);
    value.SetIsNull(isNull);

    if (!patchIter->HasNext()) {
        mCursor++;
    }
    mDocId = GetNextDocId();
}

void SingleFieldPatchIterator::Reserve(AttrFieldValue& value)
{
    uint32_t maxItemLen = 0;
    for (size_t i = 0; i < mSegmentPatchIteratorInfos.size(); i++) {
        const AttributeSegmentPatchIteratorPtr& patchIter = mSegmentPatchIteratorInfos[i].second;
        if (patchIter) {
            maxItemLen = max(maxItemLen, patchIter->GetMaxPatchItemLen());
        }
    }
    value.ReserveBuffer(maxItemLen);
}

size_t SingleFieldPatchIterator::GetPatchLoadExpandSize() const { return mPatchLoadExpandSize; }

attrid_t SingleFieldPatchIterator::GetAttributeId() const
{
    assert(mAttrConfig);
    return mAttrConfig->GetAttrId();
}
}} // namespace indexlib::index
