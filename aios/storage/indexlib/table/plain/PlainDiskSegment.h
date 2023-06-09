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

#include <map>
#include <memory>
#include <vector>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "indexlib/framework/BuildResource.h"
#include "indexlib/framework/DiskSegment.h"
#include "indexlib/framework/SegmentMeta.h"
#include "indexlib/index/IndexerParameter.h"

namespace indexlibv2::index {
class IIndexer;
class IDiskIndexer;
class IIndexFactory;
} // namespace indexlibv2::index

namespace indexlibv2::config {
class TabletSchema;
class IIndexConfig;
} // namespace indexlibv2::config

namespace indexlibv2::plain {

// DiskIndexerItem {IndexType, IndexName, Indexer}
using DiskIndexerItem = std::tuple<std::string, std::string, std::shared_ptr<indexlibv2::index::IDiskIndexer>>;
class PlainDiskSegment : public framework::DiskSegment
{
public:
    using IndexMapKey = std::pair<std::string, std::string>;

    PlainDiskSegment(const std::shared_ptr<indexlibv2::config::TabletSchema>& schema,
                     const framework::SegmentMeta& segmentMeta, const framework::BuildResource& buildResource);
    ~PlainDiskSegment() = default;

    Status Open(const std::shared_ptr<MemoryQuotaController>& memoryQuotaController,
                framework::DiskSegment::OpenMode mode) override;
    Status Reopen(const std::vector<std::shared_ptr<config::TabletSchema>>& schemas) override;
    std::pair<Status, std::shared_ptr<index::IIndexer>> GetIndexer(const std::string& type,
                                                                   const std::string& indexName) override;
    void AddIndexer(const std::string& type, const std::string& indexName,
                    std::shared_ptr<indexlibv2::index::IIndexer> indexer) override;
    void DeleteIndexer(const std::string& type, const std::string& indexName) override;

    size_t EstimateMemUsed(const std::shared_ptr<config::TabletSchema>& schema) override;
    size_t EvaluateCurrentMemUsed() override;

protected:
    std::pair<Status, std::shared_ptr<indexlibv2::index::IDiskIndexer>>
    CreateSingleIndexer(index::IIndexFactory* indexFactory,
                        const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig);

    virtual std::pair<Status, std::vector<DiskIndexerItem>>
    OpenIndexer(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig);
    virtual std::pair<Status, std::shared_ptr<indexlib::file_system::Directory>>
    GetIndexDirectory(const std::shared_ptr<indexlib::file_system::Directory>& segmentDir,
                      const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                      index::IIndexFactory* indexFactory);

    index::IndexerParameter GenerateIndexerParameter() const;
    virtual bool NeedDrop(const std::string& indexType, const std::string& indexName,
                          const std::vector<std::shared_ptr<config::TabletSchema>>& schemas);

protected:
    std::shared_ptr<indexlibv2::config::TabletSchema> _schema;
    std::map<IndexMapKey, std::shared_ptr<index::IDiskIndexer>> _indexMap;
    mutable autil::ReadWriteLock _indexMapLock;
    framework::BuildResource _buildResource;
    framework::DiskSegment::OpenMode _mode;
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::plain
