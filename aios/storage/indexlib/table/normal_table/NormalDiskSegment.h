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
#include "autil/NoCopyable.h"
#include "indexlib/table/plain/PlainDiskSegment.h"

namespace indexlibv2::table {

class NormalDiskSegment : public plain::PlainDiskSegment
{
public:
    NormalDiskSegment(const std::shared_ptr<indexlibv2::config::TabletSchema>& schema,
                      const framework::SegmentMeta& segmentMeta, const framework::BuildResource& buildResource);
    ~NormalDiskSegment();

public:
    size_t EstimateMemUsed(const std::shared_ptr<config::TabletSchema>& schema) override;

private:
    std::pair<Status, std::vector<plain::DiskIndexerItem>>
    OpenIndexer(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig) override;
    std::pair<Status, std::shared_ptr<indexlib::file_system::Directory>>
    GetIndexDirectory(const std::shared_ptr<indexlib::file_system::Directory>& segmentDir,
                      const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                      index::IIndexFactory* indexFactory) override;
    bool NeedDrop(const std::string& indexType, const std::string& indexName,
                  const std::vector<std::shared_ptr<config::TabletSchema>>& schemas) override;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
