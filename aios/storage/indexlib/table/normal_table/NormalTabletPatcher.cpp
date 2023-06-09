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
#include "indexlib/table/normal_table/NormalTabletPatcher.h"

#include "indexlib/config/IIndexConfig.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/index/attribute/AttributeDiskIndexer.h"
#include "indexlib/index/attribute/AttributeFieldValue.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/attribute/patch/AttributePatchFileFinder.h"
#include "indexlib/index/attribute/patch/AttributePatchReader.h"
#include "indexlib/index/attribute/patch/AttributePatchReaderCreator.h"
#include "indexlib/index/attribute/patch/PatchIterator.h"
#include "indexlib/index/attribute/patch/PatchIteratorCreator.h"
#include "indexlib/index/common/patch/PatchFileFinder.h"
#include "indexlib/index/deletionmap/DeletionMapModifier.h"
#include "indexlib/index/deletionmap/DeletionMapPatcher.h"
#include "indexlib/index/inverted_index/InvertedDiskIndexer.h"
#include "indexlib/index/inverted_index/patch/IInvertedIndexPatchIterator.h"
#include "indexlib/index/inverted_index/patch/IndexUpdateTermIterator.h"
#include "indexlib/index/inverted_index/patch/InvertedIndexPatchIteratorCreator.h"
#include "indexlib/table/normal_table/Common.h"
#include "indexlib/table/normal_table/NormalTabletModifier.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, NormalTabletPatcher);

NormalTabletPatcher::NormalTabletPatcher() {}

NormalTabletPatcher::~NormalTabletPatcher() {}

Status NormalTabletPatcher::LoadPatch(const std::vector<std::shared_ptr<framework::Segment>>& diffSegments,
                                      const framework::TabletData& newTabletData,
                                      const std::shared_ptr<config::TabletSchema>& schema,
                                      const std::shared_ptr<indexlib::file_system::IDirectory>& opLog2PatchRootDir,
                                      NormalTabletModifier* modifier)
{
    RETURN_IF_STATUS_ERROR(LoadPatchForDeletionMap(diffSegments, newTabletData, schema),
                           "load patch for deletion map failed");
    RETURN_IF_STATUS_ERROR(LoadPatchForAttribute(diffSegments, schema, opLog2PatchRootDir, modifier),
                           "load patch for attribute indexer failed");
    RETURN_IF_STATUS_ERROR(LoadPatchForInveredIndex(diffSegments, schema, opLog2PatchRootDir, modifier),
                           "load patch for inverted index indexer failed");
    return Status::OK();
}

Status
NormalTabletPatcher::LoadPatchForDeletionMap(const std::vector<std::shared_ptr<framework::Segment>>& diffSegments,
                                             const framework::TabletData& newTabletData,
                                             const std::shared_ptr<config::TabletSchema>& schema)
{
    auto deletionMapConfigs = schema->GetIndexConfigs(index::DELETION_MAP_INDEX_TYPE_STR);
    for (auto deletionMapConfig : deletionMapConfigs) {
        std::vector<std::pair<std::shared_ptr<index::IIndexer>, segmentid_t>> indexers;
        for (auto segment : diffSegments) {
            auto indexerPair =
                segment->GetIndexer(deletionMapConfig->GetIndexType(), deletionMapConfig->GetIndexName());
            if (!indexerPair.first.IsOK()) {
                return indexerPair.first;
            }
            indexers.emplace_back(indexerPair.second, segment->GetSegmentId());
        }

        auto slice = newTabletData.CreateSlice(framework::Segment::SegmentStatus::ST_BUILT);
        std::vector<segmentid_t> targetSegmentIds;
        for (auto segmentPtr : slice) {
            targetSegmentIds.push_back(segmentPtr->GetSegmentId());
        }
        index::DeletionMapModifier modifier;
        RETURN_IF_STATUS_ERROR(modifier.Open(deletionMapConfig, &newTabletData), "open deletionmap modifier failed");
        RETURN_IF_STATUS_ERROR(
            index::DeletionMapPatcher::LoadPatch(indexers, targetSegmentIds, /*applyInBranch*/ true, &modifier),
            "load patch for deletion map failed");
    }
    return Status::OK();
}

Status
NormalTabletPatcher::LoadPatchForAttribute(const std::vector<std::shared_ptr<framework::Segment>>& segments,
                                           const std::shared_ptr<config::TabletSchema>& schema,
                                           const std::shared_ptr<indexlib::file_system::IDirectory>& opLog2PatchRootDir,
                                           NormalTabletModifier* modifier)
{
    if (modifier) {
        return LoadAttributePatchWithModifier(segments, schema, modifier);
    }
    return LoadAttributePatch(segments, schema, opLog2PatchRootDir);
}

Status
NormalTabletPatcher::LoadAttributePatchWithModifier(const std::vector<std::shared_ptr<framework::Segment>>& segments,
                                                    const std::shared_ptr<config::TabletSchema>& schema,
                                                    NormalTabletModifier* modifier)
{
    auto patchIter = index::PatchIteratorCreator::Create(schema, segments);
    index::AttributeFieldValue value;
    patchIter->Reserve(value);
    while (patchIter->HasNext()) {
        auto status = patchIter->Next(value);
        RETURN_IF_STATUS_ERROR(status, "read patch value fail. ");
        auto fieldName = schema->GetFieldConfig(value.GetFieldId())->GetFieldName();
        modifier->UpdateFieldValue(value.GetDocId(), fieldName, *value.GetConstStringData(), value.IsNull());
    }
    return Status::OK();
}

Status
NormalTabletPatcher::LoadAttributePatch(const std::vector<std::shared_ptr<framework::Segment>>& segments,
                                        const std::shared_ptr<config::TabletSchema>& schema,
                                        const std::shared_ptr<indexlib::file_system::IDirectory>& opLog2PatchRootDir)
{
    auto attrConfigs = schema->GetIndexConfigs(index::ATTRIBUTE_INDEX_TYPE_STR);
    auto patchFinder = std::make_shared<index::AttributePatchFileFinder>();
    for (auto indexConfig : attrConfigs) {
        auto attrConfig = std::dynamic_pointer_cast<indexlibv2::config::AttributeConfig>(indexConfig);
        if (attrConfig == nullptr) {
            continue;
        }
        indexlibv2::index::PatchInfos allPatchInfos;
        auto status = patchFinder->FindAllPatchFiles(segments, attrConfig, &allPatchInfos);
        RETURN_IF_STATUS_ERROR(status, "find all attribute [%s] patch failed", attrConfig->GetIndexName().c_str());
        if (opLog2PatchRootDir) {
            auto [status, indexDir] = patchFinder->GetIndexDirectory(opLog2PatchRootDir, attrConfig);
            RETURN_IF_STATUS_ERROR(status, "get index directory failed [%s]", attrConfig->GetIndexName().c_str());
            if (indexDir) {
                AUTIL_LOG(DEBUG, "get attribute index [%s] patch from path [%s]", attrConfig->GetIndexName().c_str(),
                          indexDir->DebugString().c_str());
                auto status = patchFinder->FindPatchFiles(indexDir, INVALID_SEGMENTID, &allPatchInfos);
                RETURN_IF_STATUS_ERROR(status, "find patch files for [%s]", indexDir->DebugString().c_str());
            }
        }

        for (auto segment : segments) {
            auto segId = segment->GetSegmentId();
            auto [status, indexer] = segment->GetIndexer(attrConfig->GetIndexType(), attrConfig->GetIndexName());
            if (!status.IsOK()) {
                auto segmentSchema = segment->GetSegmentSchema();
                if (segmentSchema->GetIndexConfig(attrConfig->GetIndexType(), attrConfig->GetIndexName()) == nullptr) {
                    continue;
                }
                RETURN_IF_STATUS_ERROR(status, "get indexer [%s] from segment [%d] failed",
                                       attrConfig->GetIndexName().c_str(), segId);
            }
            auto diskIndexer = std::dynamic_pointer_cast<index::AttributeDiskIndexer>(indexer);
            assert(diskIndexer);
            auto patchReader = index::AttributePatchReaderCreator::Create(attrConfig);
            auto it = allPatchInfos.find(segId);
            if (it == allPatchInfos.end()) {
                continue;
            }
            const index::PatchFileInfos& patchFileInfos = it->second;
            for (size_t i = 0; i < patchFileInfos.Size(); ++i) {
                auto status = patchReader->AddPatchFile(patchFileInfos[i].patchDirectory,
                                                        patchFileInfos[i].patchFileName, patchFileInfos[i].srcSegment);
                RETURN_IF_STATUS_ERROR(status, "add patch file fail for segment [%d].", segId);
            }
            status = diskIndexer->SetPatchReader(patchReader, 0);
            RETURN_IF_STATUS_ERROR(status, "set patch reader fail for attribute field [%s].",
                                   attrConfig->GetAttrName().c_str());
        }
    }
    return Status::OK();
}

Status NormalTabletPatcher::LoadPatchForInveredIndex(
    const std::vector<std::shared_ptr<framework::Segment>>& segments,
    const std::shared_ptr<config::TabletSchema>& schema,
    const std::shared_ptr<indexlib::file_system::IDirectory>& opLog2PatchRootDir, NormalTabletModifier* modifier)
{
    if (modifier) {
        return LoadInvertedIndexPatchWithModifier(segments, schema, modifier);
    }
    return LoadInvertedIndexPatch(segments, schema, opLog2PatchRootDir);
}

Status NormalTabletPatcher::LoadInvertedIndexPatchWithModifier(
    const std::vector<std::shared_ptr<framework::Segment>>& segments,
    const std::shared_ptr<config::TabletSchema>& schema, NormalTabletModifier* modifier)
{
    auto patchIter =
        indexlib::index::InvertedIndexPatchIteratorCreator::Create(schema, segments, nullptr /*patchExtraDir*/);
    if (not patchIter) {
        AUTIL_LOG(ERROR, "Crreate inverted patch iterator failed");
        return Status::InternalError("Crreate inverted patch iterator failed");
    }
    while (true) {
        std::unique_ptr<indexlib::index::IndexUpdateTermIterator> termIter = patchIter->Next();
        if (termIter) {
            [[maybe_unused]] bool isSuccess = modifier->UpdateIndexTerms(termIter.get());
        } else {
            break;
        }
    }
    return Status::OK();
}

Status NormalTabletPatcher::LoadInvertedIndexPatch(
    const std::vector<std::shared_ptr<framework::Segment>>& segments,
    const std::shared_ptr<config::TabletSchema>& schema,
    const std::shared_ptr<indexlib::file_system::IDirectory>& opLog2PatchRootDir)
{
    std::map<segmentid_t, std::map<indexid_t, std::shared_ptr<indexlib::index::IInvertedDiskIndexer>>> indexers;
    for (const auto& segment : segments) {
        indexers[segment->GetSegmentId()] = {};
    }
    for (auto indexConfig : schema->GetIndexConfigs(indexlib::index::INVERTED_INDEX_TYPE_STR)) {
        auto invertedIndexConfig = std::dynamic_pointer_cast<indexlibv2::config::InvertedIndexConfig>(indexConfig);
        if (invertedIndexConfig && invertedIndexConfig->IsIndexUpdatable()) {
            for (const auto& segment : segments) {
                auto [status, indexer] =
                    segment->GetIndexer(invertedIndexConfig->GetIndexType(), invertedIndexConfig->GetIndexName());
                if (!status.IsOK()) {
                    auto segmentSchema = segment->GetSegmentSchema();
                    if (segmentSchema->GetIndexConfig(invertedIndexConfig->GetIndexType(),
                                                      invertedIndexConfig->GetIndexName()) == nullptr) {
                        continue;
                    }
                    RETURN_IF_STATUS_ERROR(status, "get indexer [%s] from segment [%d] failed",
                                           invertedIndexConfig->GetIndexName().c_str(), segment->GetSegmentId());
                }
                auto diskIndexer = std::dynamic_pointer_cast<indexlib::index::IInvertedDiskIndexer>(indexer);
                if (not diskIndexer) {
                    return Status::InternalError("get indexer [%s] from segment [%d] is not inverted disk index",
                                                 invertedIndexConfig->GetIndexName().c_str(), segment->GetSegmentId());
                }
                indexers[segment->GetSegmentId()][invertedIndexConfig->GetIndexId()] = diskIndexer;
            }
        }
    }

    auto patchIter = indexlib::index::InvertedIndexPatchIteratorCreator::Create(schema, segments, opLog2PatchRootDir);
    if (not patchIter) {
        AUTIL_LOG(ERROR, "Crreate inverted patch iterator failed");
        return Status::InternalError("Crreate inverted patch iterator failed");
    }
    while (true) {
        std::unique_ptr<indexlib::index::IndexUpdateTermIterator> termIter = patchIter->Next();
        if (termIter) {
            auto segmentIndexersIter = indexers.find(termIter->GetSegmentId());
            if (segmentIndexersIter == indexers.end()) {
                assert(false); // TODO: @qingran remove
                continue;
            }
            auto indexerIter = segmentIndexersIter->second.find(termIter->GetIndexId());
            if (indexerIter == segmentIndexersIter->second.end()) {
                return Status::InternalError("get indexId [%s] from segment [%d] is not inverted disk index",
                                             termIter->GetIndexId(), termIter->GetSegmentId());
            }
            std::shared_ptr<indexlib::index::IInvertedDiskIndexer> indexer = indexerIter->second;
            indexer->UpdateTerms(termIter.get());
        } else {
            break;
        }
    }
    return Status::OK();
}

} // namespace indexlibv2::table
