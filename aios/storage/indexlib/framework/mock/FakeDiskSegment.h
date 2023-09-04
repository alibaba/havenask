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

#include "autil/Log.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/framework/DiskSegment.h"
#include "indexlib/framework/SegmentMeta.h"

namespace indexlibv2::index {
class IDiskIndexer;
}

namespace indexlibv2::framework {

class FakeDiskSegment final : public DiskSegment
{
public:
    FakeDiskSegment(const SegmentMeta& segmentMeta);
    ~FakeDiskSegment();

public:
    Status Open(const std::shared_ptr<MemoryQuotaController>& memoryQuotaController, OpenMode mode) override;
    Status Reopen(const std::vector<std::shared_ptr<config::ITabletSchema>>& schemas) override;
    std::pair<Status, std::shared_ptr<index::IIndexer>> GetIndexer(const std::string& type,
                                                                   const std::string& indexName) override;
    void AddIndexer(const std::string& type, const std::string& indexName,
                    std::shared_ptr<index::IIndexer> indexer) override;
    void DeleteIndexer(const std::string& type, const std::string& indexName) override;
    size_t EstimateMemUsed(const std::shared_ptr<config::ITabletSchema>& schema) override;
    size_t EvaluateCurrentMemUsed() override;

public:
    void SetSegmentId(segmentid_t segId);
    void SetSegmentSchema(const std::shared_ptr<config::ITabletSchema>& schema);

public:
    // <type, name> --> IIndexer
    std::map<std::pair<std::string, std::string>, std::shared_ptr<index::IDiskIndexer>> _indexers;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::framework
