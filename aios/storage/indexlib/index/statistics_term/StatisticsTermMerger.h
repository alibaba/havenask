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
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"
#include "indexlib/config/IIndexConfig.h"
#include "indexlib/index/IIndexMerger.h"

namespace indexlib::file_system {
class IDirectory;
class FileWriter;
} // namespace indexlib::file_system

namespace indexlibv2::index {

class StatisticsTermIndexConfig;

class StatisticsTermMerger : public IIndexMerger
{
public:
    StatisticsTermMerger() = default;
    ~StatisticsTermMerger() = default;

public:
    Status Init(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                const std::map<std::string, std::any>& params) override;
    Status Merge(const SegmentMergeInfos& segMergeInfos,
                 const std::shared_ptr<framework::IndexTaskResourceManager>& taskResourceManager) override;

private:
    std::pair<Status, std::shared_ptr<indexlib::file_system::FileWriter>>
    CreateFileWriter(const std::shared_ptr<indexlib::file_system::IDirectory>& directory,
                     const std::string& fileName) const;

    std::string GetIndexName() const;

private:
    std::shared_ptr<StatisticsTermIndexConfig> _statConfig;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
