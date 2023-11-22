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

#include <memory>
#include <stdint.h>
#include <utility>
#include <vector>

#include "autil/Log.h"
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"

namespace indexlib::file_system {
class IDirectory;
}

namespace indexlibv2::config {
class IIndexConfig;
} // namespace indexlibv2::config

namespace indexlibv2::index {

class IndexerDirectoryIterator
{
public:
    using IDirectoryPtr = std::shared_ptr<indexlib::file_system::IDirectory>;

public:
    IndexerDirectoryIterator(std::vector<std::pair<segmentid_t, std::vector<IDirectoryPtr>>> indexDirs);

    // return INVALID_SEGMENTID if reach end
    std::pair<segmentid_t, std::vector<IDirectoryPtr>> Next();

private:
    std::vector<std::pair<segmentid_t, std::vector<IDirectoryPtr>>> _indexDirs;
    int64_t _cursor;
};

class IndexerDirectories
{
public:
    IndexerDirectories(std::shared_ptr<config::IIndexConfig> indexConfig);

    Status AddSegment(segmentid_t segId, const std::shared_ptr<indexlib::file_system::IDirectory>& segmetnDir);

    IndexerDirectoryIterator CreateIndexerDirectoryIterator() const;

private:
    std::shared_ptr<config::IIndexConfig> _indexConfig;
    std::vector<std::pair<segmentid_t, std::vector<IndexerDirectoryIterator::IDirectoryPtr>>> _indexDirs;

private:
    AUTIL_LOG_DECLARE();
};
} // namespace indexlibv2::index
