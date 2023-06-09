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
#include "indexlib/index/orc/OrcMerger.h"

#include <algorithm>

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/SegmentMeta.h"
#include "indexlib/index/DocMapper.h"
#include "indexlib/index/IIndexFactory.h"
#include "indexlib/index/IndexFactoryCreator.h"
#include "indexlib/index/orc/IOrcIterator.h"
#include "indexlib/index/orc/OrcDiskIndexer.h"
#include "indexlib/index/orc/OrcLeafReader.h"
#include "indexlib/index/orc/OrcReaderOptions.h"
#include "indexlib/index/orc/OutputStreamImpl.h"
#include "indexlib/index/orc/RowGroup.h"
#include "indexlib/index/orc/TypeUtils.h"
#include "orc/Writer.hh"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, OrcMerger);

Status OrcMerger::Init(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                       const std::map<std::string, std::any>& params)
{
    _config = std::dynamic_pointer_cast<config::OrcConfig>(indexConfig);
    if (_config == nullptr) {
        AUTIL_LOG(ERROR, "IndexConfig failed to dynamic cast to OrcConfig [%s][%s]",
                  indexConfig->GetIndexName().c_str(), indexConfig->GetIndexType().c_str());
        return Status::InvalidArgs();
    }

    auto iter = params.find(DocMapper::GetDocMapperType());
    if (iter == params.end()) {
        AUTIL_LOG(ERROR, "not find doc mapper name by type [%s]", DocMapper::GetDocMapperType().c_str());
        return Status::Corruption("not find doc mapper name by type");
    }
    _docMapperName = std::any_cast<std::string>(iter->second);

    _pool = orc::getMemoryPool();
    AUTIL_LOG(INFO, "orc merge init success, indexName[%s], indexType[%s]", indexConfig->GetIndexName().c_str(),
              indexConfig->GetIndexType().c_str());
    return Status::OK();
}

Status OrcMerger::PrepareDocMapper(const std::shared_ptr<framework::IndexTaskResourceManager>& resourceManager,
                                   std::shared_ptr<DocMapper>& docMapper)
{
    return resourceManager->LoadResource<DocMapper>(/*name=*/_docMapperName,
                                                    /*resourceType=*/DocMapper::GetDocMapperType(), docMapper);
}

Status OrcMerger::PrepareSrcSegmentIterators(const IIndexMerger::SegmentMergeInfos& segMergeInfos)
{
    for (const auto& sourceSegment : segMergeInfos.srcSegments) {
        const std::shared_ptr<framework::Segment>& segment = sourceSegment.segment;
        auto indexerPair = segment->GetIndexer(_config->GetIndexType(), _config->GetIndexName());
        if (!indexerPair.first.IsOK()) {
            AUTIL_LOG(ERROR, "no orc indexer for [%s] in segment [%d]", _config->GetIndexName().c_str(),
                      segment->GetSegmentId());
            return Status::InternalError();
        }
        auto indexer = indexerPair.second;
        auto segStatus = segment->GetSegmentStatus();
        std::shared_ptr<IOrcReader> reader;
        if (segStatus == framework::Segment::SegmentStatus::ST_BUILT) {
            OrcDiskIndexer* diskIndexer = dynamic_cast<OrcDiskIndexer*>(indexer.get());
            if (diskIndexer != nullptr) {
                reader = diskIndexer->GetReader();
            }
        }
        if (!reader) {
            AUTIL_LOG(ERROR, "create reader failed, segment id [%d] ", segment->GetSegmentId());
            return Status::InternalError();
        }
        auto status = PrepareSrcSegmentIterator(reader, segment->GetSegmentId(), sourceSegment.baseDocid);
        if (!status.IsOK()) {
            return status;
        }
    }
    return Status::OK();
}

Status OrcMerger::PrepareSrcSegmentIterator(const std::shared_ptr<IOrcReader>& reader, segmentid_t srcSegmentId,
                                            docid_t baseDocId)
{
    ReaderOptions opts;
    std::unique_ptr<IOrcIterator> iter;
    opts.pool = _pool;
    auto status = reader->CreateIterator(opts, iter);
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "create iterator failed, segment id[%d] ", srcSegmentId);
        return status;
    }
    std::shared_ptr<IOrcIterator> tmp = std::move(iter);
    auto leafIter = std::dynamic_pointer_cast<OrcLeafIterator>(tmp);
    if (leafIter == nullptr) {
        AUTIL_LOG(ERROR, "dynamic cast to OrcLeafIterator failed, segment id[%d] ", srcSegmentId);
        return Status::InternalError();
    }
    _srcSegIdToIterMap.emplace(srcSegmentId, std::make_pair(baseDocId, leafIter));
    return Status::OK();
}

Status OrcMerger::PrepareTargetSegmentWriters(const IIndexMerger::SegmentMergeInfos& segMergeInfos)
{
    auto indexFactoryCreator = index::IndexFactoryCreator::GetInstance();
    const std::string& indexType = _config->GetIndexType();
    auto [status, indexFactory] = indexFactoryCreator->Create(indexType);
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "create index factory for index type [%s] failed", indexType.c_str());
        return status;
    }
    std::string indexDirPath = indexFactory->GetIndexPath();

    for (const auto& segMeta : segMergeInfos.targetSegments) {
        indexlib::file_system::RemoveOption removeOption = indexlib::file_system::RemoveOption::MayNonExist();
        status =
            segMeta->segmentDir->GetIDirectory()->RemoveDirectory(indexDirPath, /*mayNonExist*/ removeOption).Status();
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "remove dir [%s] failed: %s", indexDirPath.c_str(), status.ToString().c_str());
            return status;
        }

        std::shared_ptr<indexlib::file_system::IDirectory> indexDirectory;
        std::tie(status, indexDirectory) = segMeta->segmentDir->GetIDirectory()
                                               ->MakeDirectory(indexDirPath, indexlib::file_system::DirectoryOption())
                                               .StatusWith();
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "make directory fail. file: [%s], error: [%s]", indexDirPath.c_str(),
                      status.ToString().c_str());
            return status;
        }

        std::shared_ptr<indexlib::file_system::IDirectory> directory;
        const auto& indexName = _config->GetIndexName();
        std::tie(status, directory) =
            indexDirectory->MakeDirectory(indexName, indexlib::file_system::DirectoryOption()).StatusWith();
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "make directory failed. file: [%s], error: [%s]", indexName.c_str(),
                      status.ToString().c_str());
            return status;
        }

        std::shared_ptr<indexlib::file_system::FileWriter> fileWriter;
        std::tie(status, fileWriter) =
            directory->CreateFileWriter(config::OrcConfig::GetOrcFileName(), indexlib::file_system::WriterOption())
                .StatusWith();
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "Create file writer failed. file: [%s], error: [%s]",
                      config::OrcConfig::GetOrcFileName().c_str(), status.ToString().c_str());
            return status;
        }
        PrepareTargetSegmentWriter(segMeta->segmentId, fileWriter, _config);
    }
    return Status::OK();
}

void OrcMerger::PrepareTargetSegmentWriter(segmentid_t targetSegId,
                                           const std::shared_ptr<indexlib::file_system::FileWriter>& fileWriter,
                                           const std::shared_ptr<indexlibv2::config::OrcConfig>& orcConfig)
{
    auto outputStream = std::make_shared<OutputStreamImpl>(fileWriter);
    _outputStreams.push_back(outputStream);
    std::unique_ptr<orc::Type> schema = TypeUtils::MakeOrcTypeFromConfig(_config.get(), /*fieldIds=*/ {});
    orc::WriterOptions options = _config->GetWriterOptions();
    options.setMemoryPool(_pool);
    _segIdToWriter[targetSegId] = orc::createWriter(*schema, outputStream.get(), options);
}

Status OrcMerger::Merge(const IIndexMerger::SegmentMergeInfos& segMergeInfos,
                        const std::shared_ptr<framework::IndexTaskResourceManager>& taskResourceManager)
{
    AUTIL_LOG(INFO, "start orc merge.");
    std::shared_ptr<DocMapper> docMapper;
    auto status = PrepareDocMapper(taskResourceManager, docMapper);
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "prepare doc mapper failed.");
        return status;
    }
    status = PrepareSrcSegmentIterators(segMergeInfos);
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "prepare src segment orc iterator failed");
        return status;
    }
    status = PrepareTargetSegmentWriters(segMergeInfos);
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "prepare target segment orc writer failed.");
        return status;
    }
    docid_t newDocId = 0;
    for (const auto& segMeta : segMergeInfos.targetSegments) {
        const auto& segInfo = segMeta->segmentInfo;
        std::vector<std::shared_ptr<OrcMerger::ReadPlan>> plans;
        bool ret = GenerateReadPlans(segInfo->docCount, newDocId, docMapper, &plans);
        if (!ret) {
            return Status::InternalError("generate read plans failed");
        }
        orc::Writer* writer = _segIdToWriter[segMeta->segmentId].get();
        status = MergeOneSegment(plans, writer);
        writer->close();
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "terminate orc merge, fail to merge segment[%d].", segMeta->segmentId);
            return status;
        }
        // TODO(tianxiao.ttx) seg info docCount is not accurate, need to recalculate
        newDocId += segInfo->docCount;
    }
    AUTIL_LOG(INFO, "finished orc merge.");
    return Status::OK();
}

bool OrcMerger::GenerateReadPlans(size_t totalDocCount, docid_t globalNewDocId,
                                  const std::shared_ptr<DocMapper>& docMapper,
                                  std::vector<std::shared_ptr<OrcMerger::ReadPlan>>* plans) const
{
    const size_t maxBatchCount = 4096;
    size_t alreadyPlannedDocCount = 0;
    while (alreadyPlannedDocCount < totalDocCount) {
        size_t docCount = std::min(totalDocCount - alreadyPlannedDocCount, maxBatchCount);
        auto newPlan = GenerateReadPlan(globalNewDocId, docCount, docMapper);
        if (newPlan == nullptr || newPlan->GetSizeToRead() == 0) {
            AUTIL_LOG(ERROR, "generate invalid read plan");
            return false;
        }
        alreadyPlannedDocCount += newPlan->GetSizeToRead();
        globalNewDocId += newPlan->GetSizeToRead();
        plans->emplace_back(newPlan);
    }
    if (alreadyPlannedDocCount != totalDocCount) {
        AUTIL_LOG(ERROR, "alreadyPlannedDocCount[%lu] doesn't match totalDocCount[%lu]", alreadyPlannedDocCount,
                  totalDocCount);
    }
    return (alreadyPlannedDocCount == totalDocCount);
}

std::shared_ptr<OrcMerger::ReadPlan> OrcMerger::GenerateReadPlan(docid_t globalNewDocId, size_t leftDocCount,
                                                                 const std::shared_ptr<DocMapper>& docMapper) const
{
    size_t sizeToRead = 0;
    segmentid_t segId = INVALID_SEGMENTID;
    docid_t firstDocId = INVALID_DOCID;
    docid_t preDocId = INVALID_DOCID;
    for (sizeToRead = 0; sizeToRead < leftDocCount; sizeToRead++) {
        auto [currentSegId, localOldDocId] = docMapper->ReverseMap(globalNewDocId++);
        if (sizeToRead == 0) {
            firstDocId = localOldDocId;
            segId = currentSegId;
        } else if (segId != currentSegId || preDocId + 1 != localOldDocId) {
            // not in same segment or doc id not continuous, break in advance
            break;
        }
        preDocId = localOldDocId;
    }
    if (sizeToRead == 0 || segId == INVALID_SEGMENTID || firstDocId == INVALID_DOCID) {
        AUTIL_LOG(ERROR, "generate invalid read plan, sizeToRead[%lu], segId[%d], firstDocId[%d]", sizeToRead, segId,
                  firstDocId);
        return nullptr;
    }
    return std::make_shared<OrcMerger::ReadPlan>(segId, firstDocId, sizeToRead);
}

Status OrcMerger::MergeOneSegment(const std::vector<std::shared_ptr<ReadPlan>>& readPlans, orc::Writer* writer) const
{
    for (const auto& plan : readPlans) {
        auto iter = _srcSegIdToIterMap.find(plan->GetSegmentId());
        if (iter == _srcSegIdToIterMap.end()) {
            AUTIL_LOG(ERROR, "can not find segId [%d] iterator", plan->GetSegmentId())
            return Status::InternalError();
        }
        auto [baseDocId, leafIter] = iter->second;
        docid_t localDocId = plan->GetFirstDocId();
        // only seek when docId is skipped
        if (localDocId != leafIter->GetCurrentRowId()) {
            auto status = leafIter->Seek(localDocId);
            if (!status.IsOK()) {
                AUTIL_LOG(ERROR, "fail to seek segId [%d], localDocId[%d] ", plan->GetSegmentId(), localDocId);
                return status;
            }
        }
        IOrcIterator::Result result = leafIter->Next(plan->GetSizeToRead());
        auto status = result.first;
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "fail to read segId [%d], localDocId[%d], sizeToRead[%lu] ", plan->GetSegmentId(),
                      localDocId, plan->GetSizeToRead());
            return status;
        }
        orc::ColumnVectorBatch* batch = result.second->GetRowBatch();
        writer->add(*batch);
    }
    return Status::OK();
}

orc::Writer* OrcMerger::TEST_GetOrcWriter(segmentid_t targetSegId) const
{
    auto iter = _segIdToWriter.find(targetSegId);
    if (iter == _segIdToWriter.end()) {
        return nullptr;
    }
    return iter->second.get();
}

} // namespace indexlibv2::index
