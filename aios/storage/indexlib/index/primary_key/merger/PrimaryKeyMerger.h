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

#include "autil/NoCopyable.h"
#include "indexlib/config/IIndexConfig.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/SegmentMeta.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/framework/index_task/IndexTaskResourceManager.h"
#include "indexlib/index/DocMapper.h"
#include "indexlib/index/IIndexFactory.h"
#include "indexlib/index/IIndexMerger.h"
#include "indexlib/index/IndexFactoryCreator.h"
#include "indexlib/index/primary_key/PrimaryKeyFileWriterCreator.h"
#include "indexlib/index/primary_key/PrimaryKeyIterator.h"
#include "indexlib/index/primary_key/config/PrimaryKeyIndexConfig.h"
#include "indexlib/index/primary_key/merger/OnDiskHashPrimaryKeyIterator.h"
#include "indexlib/index/primary_key/merger/OnDiskOrderedPrimaryKeyIterator.h"
#include "indexlib/index/primary_key/merger/PrimaryKeyAttributeMerger.h"

namespace indexlibv2::index {

template <typename Key>
class PrimaryKeyMerger : public autil::NoCopyable, public index::IIndexMerger
{
public:
    PrimaryKeyMerger() {}

    ~PrimaryKeyMerger() {}

    Status Init(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                const std::map<std::string, std::any>& params) override;

    Status Merge(const IIndexMerger::SegmentMergeInfos& segMergeInfos,
                 const std::shared_ptr<framework::IndexTaskResourceManager>& taskResourceManager) override;

private:
    Status DoMerge(const IIndexMerger::SegmentMergeInfos& segMergeInfos,
                   const std::shared_ptr<framework::IndexTaskResourceManager>& taskResourceManager);
    Status MergePkData(const std::shared_ptr<DocMapper>& docMapper,
                       const IIndexMerger::SegmentMergeInfos& segMergeInfos);
    std::shared_ptr<IPrimaryKeyIterator<Key>>
    CreatePkIterator(const std::shared_ptr<indexlibv2::index::PrimaryKeyIndexConfig>& pkConfig,
                     const IIndexMerger::SegmentMergeInfos& segMergeInfos);

private:
    std::shared_ptr<config::IIndexConfig> _indexConfig;
    indexlib::util::SimplePool _pool;
    std::string _docMapperName;
    std::shared_ptr<PrimaryKeyAttributeMerger<Key>> _pkAttributeMerger;

    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, PrimaryKeyMerger, Key);

template <typename Key>
Status PrimaryKeyMerger<Key>::Init(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                   const std::map<std::string, std::any>& params)
{
    auto pkConfig = std::dynamic_pointer_cast<PrimaryKeyIndexConfig>(indexConfig);
    assert(pkConfig);
    if (pkConfig->HasPrimaryKeyAttribute()) {
        _pkAttributeMerger = std::make_shared<PrimaryKeyAttributeMerger<Key>>(pkConfig);
        auto status = _pkAttributeMerger->Init(pkConfig->GetPKAttributeConfig(), params);
        RETURN_IF_STATUS_ERROR(status, "pkAttribute merger init failed");
    }

    _indexConfig = indexConfig;

    auto iter = params.find(DocMapper::GetDocMapperType());
    if (iter == params.end()) {
        AUTIL_LOG(ERROR, "no doc mapper name");
        return Status::Corruption("no doc mapper name");
    }
    _docMapperName = std::any_cast<std::string>(iter->second);
    return Status::OK();
}

template <typename Key>
Status PrimaryKeyMerger<Key>::Merge(const IIndexMerger::SegmentMergeInfos& segMergeInfos,
                                    const std::shared_ptr<framework::IndexTaskResourceManager>& taskResourceManager)
{
    // TODO(makuo.mnb) remove try catch, use iDirectory
    try {
        return DoMerge(segMergeInfos, taskResourceManager);
    } catch (const std::exception& e) {
        AUTIL_LOG(ERROR, "pk merge failed, indexName[%s] exception[%s]", _indexConfig->GetIndexName().c_str(),
                  e.what());
    } catch (...) {
        AUTIL_LOG(ERROR, "pk merge failed, indexName[%s] unknown exception", _indexConfig->GetIndexName().c_str());
    }
    return Status::IOError("pk merge failed");
}

template <typename Key>
Status PrimaryKeyMerger<Key>::DoMerge(const IIndexMerger::SegmentMergeInfos& segMergeInfos,
                                      const std::shared_ptr<framework::IndexTaskResourceManager>& taskResourceManager)
{
    std::shared_ptr<DocMapper> docMapper;
    auto status =
        taskResourceManager->LoadResource<DocMapper>(_docMapperName, DocMapper::GetDocMapperType(), docMapper);
    if (!status.IsOK()) {
        return status;
    }
    status = MergePkData(docMapper, segMergeInfos);
    if (!status.IsOK()) {
        return status;
    }
    if (_pkAttributeMerger) {
        auto status = _pkAttributeMerger->Merge(segMergeInfos, taskResourceManager);
        RETURN_IF_STATUS_ERROR(status, "pk attribute merge failed");
    }

    _pool.reset();
    return Status::OK();
}

template <typename Key>
Status PrimaryKeyMerger<Key>::MergePkData(const std::shared_ptr<DocMapper>& docMapper,
                                          const IIndexMerger::SegmentMergeInfos& segMergeInfos)
{
    AUTIL_LOG(INFO, "merge primary key data begin");
    std::shared_ptr<indexlibv2::index::PrimaryKeyIndexConfig> pkConfig =
        std::dynamic_pointer_cast<indexlibv2::index::PrimaryKeyIndexConfig>(_indexConfig);

    auto pkIter = CreatePkIterator(pkConfig, segMergeInfos);
    if (pkIter == nullptr) {
        AUTIL_LOG(ERROR, "create pk iterator failed.");
        return Status::Corruption("create pk iterator failed.");
    }
    std::map<segmentid_t, std::shared_ptr<PrimaryKeyFileWriter<Key>>> segIdToWriter;

    auto indexFactoryCreator = index::IndexFactoryCreator::GetInstance();
    const std::string& indexType = _indexConfig->GetIndexType();
    auto [status, indexFactory] = indexFactoryCreator->Create(indexType);
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "create index factory for index type [%s] failed", indexType.c_str());
        return status;
    }
    std::string indexDirPath = indexFactory->GetIndexPath();

    size_t idx = 0;
    for (const auto& segMeta : segMergeInfos.targetSegments) {
        std::shared_ptr<indexlib::file_system::IDirectory> indexDirectory;
        std::tie(status, indexDirectory) = segMeta->segmentDir->GetIDirectory()
                                               ->MakeDirectory(indexDirPath, indexlib::file_system::DirectoryOption())
                                               .StatusWith();
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "make diretory fail. file: [%s], error: [%s]", indexDirPath.c_str(),
                      status.ToString().c_str());
            return status;
        }

        const auto& indexName = _indexConfig->GetIndexName();
        indexlib::file_system::RemoveOption removeOption = indexlib::file_system::RemoveOption::MayNonExist();
        status = indexDirectory->RemoveDirectory(indexName, /*mayNonExist*/ removeOption).Status();
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "remove dir [%s] failed: %s", indexName.c_str(), status.ToString().c_str());
            return status;
        }

        std::shared_ptr<indexlib::file_system::IDirectory> pkDirectory;
        std::tie(status, pkDirectory) =
            indexDirectory->MakeDirectory(indexName, indexlib::file_system::DirectoryOption()).StatusWith();
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "make diretory fail. file: [%s], error: [%s]", indexName.c_str(),
                      status.ToString().c_str());
            return status;
        }

        std::shared_ptr<indexlib::file_system::FileWriter> fileWriter;
        std::tie(status, fileWriter) =
            pkDirectory->CreateFileWriter(PRIMARY_KEY_DATA_FILE_NAME, indexlib::file_system::WriterOption())
                .StatusWith();
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "Create file writer failed. file: [%s], error: [%s].", PRIMARY_KEY_DATA_FILE_NAME,
                      status.ToString().c_str());
            return status;
        }

        auto primaryKeyFileWriter = PrimaryKeyFileWriterCreator<Key>::CreatePKFileWriter(pkConfig);
        if (docMapper->GetTargetSegmentId(idx) != segMeta->segmentId) {
            AUTIL_LOG(ERROR, "docMapper target segment id [%d] not match segMeta segment id [%d]",
                      docMapper->GetTargetSegmentId(idx), segMeta->segmentId);
            return Status::Corruption("docMaper segId not match");
        }
        auto docCount = docMapper->GetTargetSegmentDocCount(idx++);
        segMeta->segmentMetrics->SetKeyCount(docCount);
        primaryKeyFileWriter->Init(docCount, docCount, fileWriter, &_pool);
        segIdToWriter[segMeta->segmentId] = primaryKeyFileWriter;
    }

    typename PrimaryKeyIterator<Key>::PKPairTyped pkPair;
    while (pkIter->HasNext()) {
        pkIter->Next(pkPair);
        auto localInfo = docMapper->Map(pkPair.docid);
        if (localInfo.second == INVALID_DOCID) {
            continue;
        }
        auto iter = segIdToWriter.find(localInfo.first);
        if (iter == segIdToWriter.end()) {
            AUTIL_LOG(ERROR, "not found output file writer");
            continue;
        }
        auto fileWriter = iter->second;
        Status status;
        if (pkConfig->GetPrimaryKeyIndexType() == pk_hash_table) {
            status = fileWriter->AddPKPair(pkPair.key, localInfo.second);
        } else {
            status = fileWriter->AddSortedPKPair(pkPair.key, localInfo.second);
        }
        if (!status.IsOK()) {
            return status;
        }
    }
    for (const auto& iter : segIdToWriter) {
        auto status = iter.second->Close();
        if (!status.IsOK()) {
            return status;
        }
    }
    AUTIL_LOG(INFO, "merge primary key data end");
    return Status::OK();
}

template <typename Key>
std::shared_ptr<IPrimaryKeyIterator<Key>>
PrimaryKeyMerger<Key>::CreatePkIterator(const std::shared_ptr<indexlibv2::index::PrimaryKeyIndexConfig>& pkConfig,
                                        const IIndexMerger::SegmentMergeInfos& segMergeInfos)
{
    std::shared_ptr<IPrimaryKeyIterator<Key>> iter;
    if (pkConfig->GetPrimaryKeyIndexType() == pk_hash_table) {
        iter.reset(new OnDiskHashPrimaryKeyIterator<Key>());
    } else {
        iter.reset(new OnDiskOrderedPrimaryKeyIterator<Key>);
    }
    std::vector<SegmentDataAdapter::SegmentDataType> segments;
    for (const auto& sourceSegment : segMergeInfos.srcSegments) {
        SegmentDataAdapter::SegmentDataType segmentData;
        segmentData._segmentInfo = sourceSegment.segment->GetSegmentInfo();
        segmentData._directory = sourceSegment.segment->GetSegmentDirectory();
        segmentData._segmentId = sourceSegment.segment->GetSegmentId();
        segmentData._baseDocId = sourceSegment.baseDocid;
        segmentData._segment = sourceSegment.segment.get();
        segments.push_back(segmentData);
    }
    if (!iter->Init(pkConfig, segments)) {
        return nullptr;
    }
    return iter;
}

} // namespace indexlibv2::index
