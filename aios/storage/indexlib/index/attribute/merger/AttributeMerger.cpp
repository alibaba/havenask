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
#include "indexlib/index/attribute/merger/AttributeMerger.h"

#include "autil/StringUtil.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/framework/index_task/IndexTaskResourceManager.h"
#include "indexlib/index/DocMapper.h"
#include "indexlib/index/attribute/AttributeDiskIndexer.h"
#include "indexlib/index/attribute/SliceInfo.h"
#include "indexlib/index/attribute/patch/AttributePatchReader.h"
#include "indexlib/index/attribute/patch/AttributePatchReaderCreator.h"
#include "indexlib/util/PathUtil.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, AttributeMerger);
Status AttributeMerger::Init(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                             const std::map<std::string, std::any>& params)
{
    _attributeConfig = std::dynamic_pointer_cast<AttributeConfig>(indexConfig);
    assert(_attributeConfig != nullptr);
    //可更新的attribute
    //如果多slice情况下，只有第一个slice负责merge patch，其他不merge
    //如果没有slice，sliceIdx == 0，也负责merge patch
    if (_attributeConfig->IsAttributeUpdatable()) {
        if (_attributeConfig->GetSliceCount() <= 1) {
            _needMergePatch = true;
        } else {
            if (_attributeConfig->GetSliceIdx() == 0) {
                _needMergePatch = true;
            }
        }
    }

    auto iter = params.find(DocMapper::GetDocMapperType());
    if (iter == params.end()) {
        AUTIL_LOG(ERROR, "could't find doc mapper, type [%s]", DocMapper::GetDocMapperType().c_str());
        return Status::Corruption();
    }
    _docMapperName = std::any_cast<std::string>(iter->second);
    return Status::OK();
}

Status AttributeMerger::Merge(const SegmentMergeInfos& segMergeInfos,
                              const std::shared_ptr<framework::IndexTaskResourceManager>& taskResourceManager)
{
    std::stringstream ss;
    std::for_each(segMergeInfos.targetSegments.begin(), segMergeInfos.targetSegments.end(),
                  [this, &ss, delim = ""](const std::shared_ptr<framework::SegmentMeta>& segMeta) {
                      ss << delim << GetAttributePath(segMeta->segmentDir->DebugString());
                  });
    AUTIL_LOG(DEBUG, "Start merge segments, dir : %s", ss.str().c_str());
    RETURN_IF_STATUS_ERROR(LoadPatchReader(segMergeInfos.srcSegments), "load patch information failed.");

    std::shared_ptr<DocMapper> docMapper;
    auto status =
        taskResourceManager->LoadResource<DocMapper>(/*name=*/_docMapperName,
                                                     /*resourceType=*/DocMapper::GetDocMapperType(), docMapper);
    RETURN_IF_STATUS_ERROR(status, "load doc mappaer failed");
    status = DoMerge(segMergeInfos, docMapper);
    RETURN_IF_STATUS_ERROR(status, "do attribute merge operation fail");
    if (_attributeConfig->GetSliceCount() > 1 && _attributeConfig->GetSliceIdx() == 0) {
        status = StoreSliceInfo(segMergeInfos);
        RETURN_IF_STATUS_ERROR(status, "store slice infos");
    }
    AUTIL_LOG(DEBUG, "Finish sort by weight merging to dir : %s", ss.str().c_str());
    return Status::OK();
}

Status AttributeMerger::StoreSliceInfo(const SegmentMergeInfos& segMergeInfos)
{
    if (segMergeInfos.targetSegments.size() > 1) {
        RETURN_IF_STATUS_ERROR(Status::Corruption(), "split segment not support attribute merge to multi slices");
    }
    auto segIDir = segMergeInfos.targetSegments[0]->segmentDir->GetIDirectory();
    auto [status, attributeDir] =
        segIDir->MakeDirectory(index::ATTRIBUTE_INDEX_TYPE_STR, indexlib::file_system::DirectoryOption()).StatusWith();
    RETURN_IF_STATUS_ERROR(status, "make directory for attribute dir failed");
    auto [dirStatus, fieldDir] =
        attributeDir->MakeDirectory(_attributeConfig->GetAttrName(), indexlib::file_system::DirectoryOption())
            .StatusWith();
    RETURN_IF_STATUS_ERROR(dirStatus, "make directory for attribute dir failed");
    AttributeDataInfo attrDataInfo;
    attrDataInfo.sliceCount = _attributeConfig->GetSliceCount();
    return attrDataInfo.Store(fieldDir);
}

std::string AttributeMerger::GetAttributePath(const std::string& dir) const
{
    return indexlib::util::PathUtil::JoinPath(dir, _attributeConfig->GetAttrName());
}

Status AttributeMerger::LoadPatchReader(const std::vector<IIndexMerger::SourceSegment>& srcSegments)
{
    std::map<segmentid_t, std::shared_ptr<AttributePatchReader>> segId2PatchReader;
    auto st = CreatePatchReader(srcSegments, segId2PatchReader);
    RETURN_IF_STATUS_ERROR(st, "load patch reader failed.");
    for (const auto& [_, segment] : srcSegments) {
        auto segId = segment->GetSegmentId();
        auto indexerPair = segment->GetIndexer(_attributeConfig->GetIndexType(), _attributeConfig->GetIndexName());
        if (!indexerPair.first.IsOK()) {
            auto segmentSchema = segment->GetSegmentSchema();
            if (indexerPair.first.IsNotFound() && segmentSchema &&
                segmentSchema->GetIndexConfig(ATTRIBUTE_INDEX_TYPE_STR, _attributeConfig->GetAttrName()) == nullptr) {
                continue;
            }
            return indexerPair.first;
        }
        auto diskIndexer = std::dynamic_pointer_cast<index::AttributeDiskIndexer>(indexerPair.second);
        if (diskIndexer == nullptr) {
            continue;
        }
        auto it = segId2PatchReader.find(segId);
        if (it == segId2PatchReader.end()) {
            continue;
        }
        auto& patchReader = it->second;
        auto status = diskIndexer->SetPatchReader(patchReader, 0);
        RETURN_IF_STATUS_ERROR(status, "set patch reader fail, segId[%d] attribute field [%s].", segId,
                               _attributeConfig->GetAttrName().c_str());
    }
    return Status::OK();
}

Status
AttributeMerger::CreatePatchReader(const std::vector<IIndexMerger::SourceSegment>& srcSegments,
                                   std::map<segmentid_t, std::shared_ptr<AttributePatchReader>>& segId2PatchReader)
{
    for (const auto& [_, segment] : srcSegments) {
        auto segId = segment->GetSegmentId();
        auto it = _allPatchInfos.find(segId);
        if (it == _allPatchInfos.end()) {
            continue;
        }
        auto patchReader = index::AttributePatchReaderCreator::Create(_attributeConfig);
        const index::PatchFileInfos& patchInfoVec = it->second;
        for (size_t i = 0; i < patchInfoVec.Size(); ++i) {
            auto status = patchReader->AddPatchFile(patchInfoVec[i].patchDirectory, patchInfoVec[i].patchFileName,
                                                    patchInfoVec[i].srcSegment);
            RETURN_IF_STATUS_ERROR(status, "add patch file fail for segment [%d]", segId);
        }
        segId2PatchReader[segId] = patchReader;
    }
    return Status::OK();
}

} // namespace indexlibv2::index
