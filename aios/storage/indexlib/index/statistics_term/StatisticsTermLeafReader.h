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
#include "indexlib/index/statistics_term/StatisticsTermIndexConfig.h"

namespace indexlib::file_system {
class IDirectory;
class FileReader;
} // namespace indexlib::file_system

namespace indexlibv2::index {

class StatisticsTermLeafReader
{
public:
    StatisticsTermLeafReader() = default;
    ~StatisticsTermLeafReader() = default;

public:
    Status Open(const std::shared_ptr<StatisticsTermIndexConfig>& indexConfig,
                const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory);

    Status GetTermStatistics(const std::string& invertedIndexName,
                             std::unordered_map<size_t, std::string>& termStatistics);

private:
    Status LoadMeta(const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory);

private:
    std::shared_ptr<StatisticsTermIndexConfig> _indexConfig;
    std::shared_ptr<indexlib::file_system::FileReader> _dataFile;
    std::map<std::string, std::pair<size_t, size_t>> _termMetas; // index1 -> <offset, blockLen>

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
