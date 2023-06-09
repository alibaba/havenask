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
#include "indexlib/table/plain/PlainDiskSegment.h"

#include "indexlib/config/IIndexConfig.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/framework/cleaner/DropIndexCleaner.h"
#include "indexlib/index/IDiskIndexer.h"
#include "indexlib/index/IIndexFactory.h"
#include "indexlib/index/IIndexReader.h"
#include "indexlib/index/IndexFactoryCreator.h"
#include "kmonitor/client/MetricsReporter.h"

namespace indexlibv2::plain {
AUTIL_LOG_SETUP(indexlib.plain, PlainDiskSegment);

PlainDiskSegment::PlainDiskSegment(const std::shared_ptr<config::TabletSchema>& schema,
                                   const framework::SegmentMeta& segmentMeta, const framework::BuildResource& resource)
    : framework::DiskSegment(segmentMeta)
    , _schema(schema)
    , _buildResource(resource)
    , _mode(OpenMode::NORMAL)
{
}

std::pair<Status, std::shared_ptr<indexlibv2::index::IDiskIndexer>>
PlainDiskSegment::CreateSingleIndexer(index::IIndexFactory* indexFactory,
                                      const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig)
{
    using RetType = std::pair<Status, std::shared_ptr<indexlibv2::index::IDiskIndexer>>;
    const auto& indexName = indexConfig->GetIndexName();
    auto indexerParam = GenerateIndexerParameter();
    auto indexer = indexFactory->CreateDiskIndexer(indexConfig, indexerParam);
    if (indexer == nullptr) {
        AUTIL_LOG(WARN, "get null indexer [%s]", indexName.c_str());
        return RetType(Status::Unimplement(), nullptr);
    }
    return RetType(Status::OK(), indexer);
}

std::pair<Status, std::vector<DiskIndexerItem>>
PlainDiskSegment::OpenIndexer(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig)
{
    auto indexFactoryCreator = index::IndexFactoryCreator::GetInstance();
    auto [factoryStatus, indexFactory] = indexFactoryCreator->Create(indexConfig->GetIndexType());
    if (!factoryStatus.IsOK()) {
        AUTIL_LOG(ERROR, "create index factory for index type [%s] failed", indexConfig->GetIndexType().c_str());
        return std::make_pair(factoryStatus, std::vector<DiskIndexerItem> {});
    }

    const auto& indexName = indexConfig->GetIndexName();
    auto [status, indexer] = CreateSingleIndexer(indexFactory, indexConfig);
    RETURN2_IF_STATUS_ERROR(status, std::vector<DiskIndexerItem> {}, "prepare indexer[%s] failed.", indexName.c_str());
    auto segDir = GetSegmentDirectory();
    auto [dirStatus, indexDir] = GetIndexDirectory(segDir, indexConfig, indexFactory);
    if (!dirStatus.IsOK()) {
        AUTIL_LOG(ERROR, "get index directory fail, error [%s]", dirStatus.ToString().c_str());
        return std::make_pair(dirStatus, std::vector<DiskIndexerItem> {});
    }
    status = indexer->Open(indexConfig, (indexDir ? indexDir->GetIDirectory() : nullptr));
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "create segment [%d] built index [%s] failed", GetSegmentId(), indexName.c_str());
        return std::make_pair(status, std::vector<DiskIndexerItem> {});
    }
    return std::make_pair(status, std::vector<DiskIndexerItem> {{indexConfig->GetIndexType(), indexName, indexer}});
}

Status PlainDiskSegment::Open(const std::shared_ptr<MemoryQuotaController>& memoryQuotaController,
                              framework::DiskSegment::OpenMode mode)
{
    auto segmentMetrics = GetSegmentMetrics();
    RETURN_IF_STATUS_ERROR(segmentMetrics->LoadSegmentMetrics(GetSegmentDirectory()->GetIDirectory()),
                           "load segment metrics failed");
    _mode = mode;
    if (mode == OpenMode::LAZY) {
        AUTIL_LOG(INFO, "open segment [%d] with lazy mode", GetSegmentId());
        return Status::OK();
    }
    auto indexConfigs = _schema->GetIndexConfigs();
    for (const auto& indexConfig : indexConfigs) {
        auto [status, indexerItems] = OpenIndexer(indexConfig);
        if (!status.IsOK()) {
            return status;
        }
        for (const auto& [indexType, indexName, indexer] : indexerItems) {
            if (indexer != nullptr) {
                auto indexMapKey = std::make_pair(indexType, indexName);
                autil::ScopedWriteLock lock(_indexMapLock);
                _indexMap[indexMapKey] = indexer;
            }
        }
    }
    return Status::OK();
}

bool PlainDiskSegment::NeedDrop(const std::string& indexType, const std::string& indexName,
                                const std::vector<std::shared_ptr<config::TabletSchema>>& schemas)
{
    for (auto schema : schemas) {
        auto indexConfig = schema->GetIndexConfig(indexType, indexName);
        if (!indexConfig) {
            return true;
        }
    }
    return false;
}

Status PlainDiskSegment::Reopen(const std::vector<std::shared_ptr<config::TabletSchema>>& schemas)
{
    auto schema = schemas[schemas.size() - 1];
    auto directory = GetSegmentDirectory()->GetIDirectory();
    std::map<IndexMapKey, std::shared_ptr<index::IDiskIndexer>> indexMap;
    for (auto [indexMapKey, indexer] : _indexMap) {
        if (NeedDrop(indexMapKey.first, indexMapKey.second, schemas)) {
            AUTIL_LOG(INFO, "drop index:[%s,%s]", indexMapKey.first.c_str(), indexMapKey.second.c_str());
            continue;
        }
        indexMap[indexMapKey] = indexer;
    }
    if (_mode != OpenMode::LAZY) {
        auto indexConfigs = schema->GetIndexConfigs();
        for (auto indexConfig : indexConfigs) {
            auto iter = indexMap.find(std::make_pair(indexConfig->GetIndexType(), indexConfig->GetIndexName()));
            if (iter == indexMap.end()) {
                auto [status, indexerItems] = OpenIndexer(indexConfig);
                RETURN_IF_STATUS_ERROR(status, "open indexer failed");
                for (const auto& [indexType, indexName, indexer] : indexerItems) {
                    if (indexer != nullptr) {
                        auto indexMapKey = std::make_pair(indexType, indexName);
                        indexMap[indexMapKey] = indexer;
                    }
                }
            }
        }
    }
    autil::ScopedWriteLock lock(_indexMapLock);
    _indexMap.swap(indexMap);
    _schema = schema;
    _segmentMeta.schema = schema;
    return Status::OK();
}

std::pair<Status, std::shared_ptr<index::IIndexer>> PlainDiskSegment::GetIndexer(const std::string& type,
                                                                                 const std::string& indexName)
{
    auto indexMapKey = std::make_pair(type, indexName);
    {
        autil::ScopedReadLock lock(_indexMapLock);
        auto iter = _indexMap.find(indexMapKey);
        if (iter != _indexMap.end()) {
            return std::make_pair(Status::OK(), iter->second);
        }
    }
    if (_mode != OpenMode::LAZY) {
        AUTIL_LOG(WARN, "indexName[%s] missing", indexName.c_str());
        return std::make_pair(Status::NotFound(), nullptr);
    }
    auto indexConfig = _schema->GetIndexConfig(type, indexName);
    if (!indexConfig) {
        AUTIL_LOG(ERROR, "get indexer from schema failed, indexName[%s]", indexName.c_str());
        return std::make_pair(Status::NotFound(), nullptr);
    }
    auto [status, indexerItems] = OpenIndexer(indexConfig);
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "open indexer failed, indexName[%s]", indexName.c_str());
        return std::make_pair(status, nullptr);
    }

    std::shared_ptr<index::IIndexer> actualIndexer = nullptr;
    for (const auto& [indexType, name, indexer] : indexerItems) {
        if (indexer != nullptr) {
            autil::ScopedWriteLock lock(_indexMapLock);
            _indexMap[std::make_pair(indexType, name)] = indexer;
        }
        if (indexName == name) {
            actualIndexer = indexer;
        }
    }

    if (actualIndexer != nullptr) {
        return std::make_pair(Status::OK(), actualIndexer);
    }
    return std::make_pair(Status::NotFound(), actualIndexer);
}

void PlainDiskSegment::AddIndexer(const std::string& type, const std::string& indexName,
                                  std::shared_ptr<indexlibv2::index::IIndexer> indexer)
{
    auto diskIndexer = std::dynamic_pointer_cast<index::IDiskIndexer>(indexer);
    if (!diskIndexer) {
        return;
    }
    auto indexMapKey = std::make_pair(type, indexName);
    {
        autil::ScopedWriteLock lock(_indexMapLock);
        _indexMap[indexMapKey] = diskIndexer;
    }
}

void PlainDiskSegment::DeleteIndexer(const std::string& type, const std::string& indexName)
{
    autil::ScopedWriteLock lock(_indexMapLock);
    auto indexMapKey = std::make_pair(type, indexName);
    auto iter = _indexMap.find(indexMapKey);
    if (iter != _indexMap.end()) {
        _indexMap.erase(iter);
    }
}

// TODO: if exception occurs, how-to ?
size_t PlainDiskSegment::EstimateMemUsed(const std::shared_ptr<config::TabletSchema>& schema)
{
    auto segDir = GetSegmentDirectory();
    size_t totalMemUsed = 0;
    auto indexerParam = GenerateIndexerParameter();
    auto indexFactoryCreator = index::IndexFactoryCreator::GetInstance();
    auto indexConfigs = schema->GetIndexConfigs();
    for (const auto& indexConfig : indexConfigs) {
        std::string indexType = indexConfig->GetIndexType();
        std::string indexName = indexConfig->GetIndexName();
        auto [status, indexFactory] = indexFactoryCreator->Create(indexType);
        if (!status.IsOK()) {
            continue;
        }
        auto indexer = indexFactory->CreateDiskIndexer(indexConfig, indexerParam);
        if (!indexer) {
            continue;
        }
        auto [dirStatus, indexDir] = GetIndexDirectory(segDir, indexConfig, indexFactory);
        if (!dirStatus.IsOK()) {
            AUTIL_LOG(ERROR, "get index directory fail. error [%s]", dirStatus.ToString().c_str());
            continue;
        }

        try {
            totalMemUsed += indexer->EstimateMemUsed(indexConfig, (indexDir ? indexDir->GetIDirectory() : nullptr));
        } catch (...) {
            AUTIL_LOG(ERROR, "fail to estimate mem use for indexer [%s]", indexName.c_str());
            continue;
        }
    }
    return totalMemUsed;
}

index::IndexerParameter PlainDiskSegment::GenerateIndexerParameter() const
{
    index::IndexerParameter indexerParam;
    indexerParam.segmentId = GetSegmentId();
    indexerParam.docCount = GetSegmentInfo()->docCount;
    indexerParam.counterMap = _buildResource.counterMap;
    indexerParam.counterPrefix = _buildResource.counterPrefix;
    indexerParam.metricsManager = _buildResource.metricsManager;
    indexerParam.indexMemoryReclaimer = _buildResource.indexMemoryReclaimer;
    indexerParam.segmentInfo = GetSegmentInfo();
    indexerParam.segmentMetrics = GetSegmentMetrics();

    return indexerParam;
}

size_t PlainDiskSegment::EvaluateCurrentMemUsed()
{
    std::vector<std::shared_ptr<index::IDiskIndexer>> indexers;
    {
        autil::ScopedReadLock lock(_indexMapLock);
        indexers.reserve(_indexMap.size());
        for (auto& kv : _indexMap) {
            indexers.emplace_back(kv.second);
        }
    }
    size_t ret = 0;
    for (auto& indexer : indexers) {
        if (indexer) {
            ret += indexer->EvaluateCurrentMemUsed();
        }
    }
    return ret;
}

std::pair<Status, std::shared_ptr<indexlib::file_system::Directory>>
PlainDiskSegment::GetIndexDirectory(const std::shared_ptr<indexlib::file_system::Directory>& segmentDir,
                                    const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                                    index::IIndexFactory* indexFactory)
{
    auto indexPath = indexFactory->GetIndexPath();
    auto indexDir = segmentDir->GetDirectory(indexPath, /*throwExceptionIfNotExist=*/false);
    if (indexDir == nullptr) {
        return std::make_pair(Status::InternalError("fail to get built index dir [%s]", indexPath.c_str()), nullptr);
    }
    return std::make_pair(Status::OK(), indexDir);
}

} // namespace indexlibv2::plain
