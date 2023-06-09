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
#include "indexlib/index/attribute/patch/SingleFieldPatchIterator.h"

#include "indexlib/framework/Segment.h"
#include "indexlib/index/attribute/AttributeFieldValue.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/attribute/patch/AttributePatchFileFinder.h"
#include "indexlib/index/attribute/patch/AttributePatchReaderCreator.h"
#include "indexlib/index/attribute/patch/SingleValueAttributePatchReader.h"
#include "indexlib/util/PathUtil.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, SingleFieldPatchIterator);

SingleFieldPatchIterator::SingleFieldPatchIterator(const std::shared_ptr<config::AttributeConfig>& attrConfig,
                                                   bool isSub)
    : AttributePatchIterator(APT_SINGLE, isSub)
    , _attrConfig(attrConfig)
    , _cursor(0)
    , _patchLoadExpandSize(0)
    , _patchItemCount(0)
{
    assert(_attrConfig);
    assert(_attrConfig->IsAttributeUpdatable());
    _attrIdentifier = _attrConfig->GetFieldId();
}

SingleFieldPatchIterator::~SingleFieldPatchIterator() {}

Status SingleFieldPatchIterator::Init(const std::vector<std::shared_ptr<framework::Segment>>& segments)
{
    auto attrPatchFinder = std::make_shared<AttributePatchFileFinder>();
    indexlibv2::index::PatchInfos allPatchInfos;
    auto status = attrPatchFinder->FindAllPatchFiles(segments, _attrConfig, &allPatchInfos);
    RETURN_IF_STATUS_ERROR(status, "find patch file fail for attribute field [%s].",
                           _attrConfig->GetAttrName().c_str());

    docid_t baseDocId = 0;
    for (auto segment : segments) {
        auto segId = segment->GetSegmentId();
        auto segmentInfo = segment->GetSegmentInfo();
        assert(segmentInfo != nullptr);
        auto docCount = segmentInfo->docCount;
        auto iter = allPatchInfos.find(segId);
        if (iter == allPatchInfos.end()) {
            baseDocId += docCount;
            continue;
        }
        std::shared_ptr<AttributePatchReader> segmentPatchReader = CreateSegmentPatchReader(iter->second);
        if (segmentPatchReader->HasNext()) {
            if (_attrConfig->IsLoadPatchExpand()) {
                _patchLoadExpandSize += segmentPatchReader->GetPatchFileLength();
            }
            _patchItemCount += segmentPatchReader->GetPatchItemCount();
            _segmentPatchReaderInfos.push_back(std::make_pair(baseDocId, segmentPatchReader));
        }
        baseDocId += docCount;
    }
    _docId = GetNextDocId();
    return Status::OK();
}

// void SingleFieldPatchIterator::CreateIndependentPatchWorkItems(vector<PatchWorkItem*>& workItems)
// {
//     for (const auto& segPatchIterInfo : _segmentPatchReaderInfos) {
//         stringstream ss;
//         ss << _attrConfig->GetAttrName() << "_" << segPatchIterInfo.first << "_";
//         if (mIsSub) {
//             ss << "sub";
//         } else {
//             ss << "main";
//         }
//         PatchWorkItem::PatchWorkItemType itemType =
//             _attrConfig->IsLengthFixed() ? PatchWorkItem::PWIT_FIX_LENGTH : PatchWorkItem::PWIT_VAR_NUM;
//         PatchWorkItem* item =
//             new SingleFieldPatchWorkItem(ss.str(), segPatchIterInfo.second->GetPatchItemCount(), mIsSub, itemType,
//                                          segPatchIterInfo.second, segPatchIterInfo.first, _attrIdentifier);
//         workItems.push_back(item);
//     }
// }

docid_t SingleFieldPatchIterator::CalculateGlobalDocId() const
{
    docid_t baseDocid = _segmentPatchReaderInfos[_cursor].first;
    const std::shared_ptr<AttributePatchReader>& segmentPatchReader = _segmentPatchReaderInfos[_cursor].second;
    return baseDocid + segmentPatchReader->GetCurrentDocId();
}

PatchFileInfos SingleFieldPatchIterator::FilterLoadedPatchFileInfos(PatchFileInfos& patchFileInfos,
                                                                    segmentid_t startLoadSegment)
{
    PatchFileInfos validPatchFileInfos;
    for (size_t i = 0; i < patchFileInfos.Size(); i++) {
        if (patchFileInfos[i].srcSegment >= startLoadSegment) {
            validPatchFileInfos.PushBack(patchFileInfos[i]);
        }
    }
    return validPatchFileInfos;
}

std::shared_ptr<AttributePatchReader>
SingleFieldPatchIterator::CreateSegmentPatchReader(const PatchFileInfos& patchFileInfos)
{
    std::shared_ptr<AttributePatchReader> patchIter(AttributePatchReaderCreator::Create(_attrConfig));
    assert(patchIter);
    for (size_t i = 0; i < patchFileInfos.Size(); i++) {
        auto status = patchIter->AddPatchFile(patchFileInfos[i].patchDirectory, patchFileInfos[i].patchFileName,
                                              patchFileInfos[i].srcSegment);
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "create attribute patch reader fail.");
            return nullptr;
        }
    }
    return patchIter;
}

Status SingleFieldPatchIterator::Next(AttributeFieldValue& value)
{
    assert(HasNext());
    const std::shared_ptr<AttributePatchReader>& patchIter = _segmentPatchReaderInfos[_cursor].second;

    docid_t docid = INVALID_DOCID;
    bool isNull = false;
    auto [status, valueSize] = patchIter->Next(docid, value.Data(), value.BufferLength(), isNull);
    RETURN_IF_STATUS_ERROR(status, "read next attribute patch value fail.");
    docid += _segmentPatchReaderInfos[_cursor].first;

    value.SetFieldId(_attrIdentifier);
    value.SetDocId(docid);
    value.SetIsPackAttr(false);
    value.SetDataSize(valueSize);
    value.SetIsSubDocId(_isSub);
    value.SetIsNull(isNull);

    if (!patchIter->HasNext()) {
        _cursor++;
    }
    _docId = GetNextDocId();
    return Status::OK();
}

void SingleFieldPatchIterator::Reserve(AttributeFieldValue& value)
{
    uint32_t maxItemLen = 0;
    for (size_t i = 0; i < _segmentPatchReaderInfos.size(); i++) {
        const std::shared_ptr<AttributePatchReader>& patchIter = _segmentPatchReaderInfos[i].second;
        if (patchIter) {
            maxItemLen = std::max(maxItemLen, patchIter->GetMaxPatchItemLen());
        }
    }
    value.ReserveBuffer(maxItemLen);
}

size_t SingleFieldPatchIterator::GetPatchLoadExpandSize() const { return _patchLoadExpandSize; }

attrid_t SingleFieldPatchIterator::GetAttributeId() const
{
    assert(_attrConfig);
    return _attrConfig->GetAttrId();
}

} // namespace indexlibv2::index
