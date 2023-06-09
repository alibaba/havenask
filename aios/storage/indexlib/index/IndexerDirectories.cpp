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
#include "indexlib/index/IndexerDirectories.h"

#include "indexlib/base/Constant.h"
#include "indexlib/config/IIndexConfig.h"
#include "indexlib/file_system/IDirectory.h"

using indexlib::file_system::IDirectory;

namespace indexlibv2::index {

AUTIL_LOG_SETUP(indexlib.index, IndexerDirectories);

IndexerDirectoryIterator::IndexerDirectoryIterator(
    std::vector<std::pair<segmentid_t, std::vector<std::shared_ptr<IDirectory>>>> indexDirs)
    : _indexDirs(std::move(indexDirs))
    , _cursor(-1)
{
}

std::pair<segmentid_t, std::vector<std::shared_ptr<IDirectory>>> IndexerDirectoryIterator::Next()
{
    ++_cursor;
    if (static_cast<size_t>(_cursor) >= _indexDirs.size()) {
        return {INVALID_SEGMENTID, {}};
    }
    return _indexDirs[_cursor];
}

IndexerDirectories::IndexerDirectories(std::shared_ptr<config::IIndexConfig> indexConfig)
    : _indexConfig(std::move(indexConfig))
{
}

Status IndexerDirectories::AddSegment(segmentid_t segId, const std::shared_ptr<IDirectory>& segmentDir)
{
    assert(segId != INVALID_SEGMENTID);
    std::vector<std::shared_ptr<IDirectory>> dirs;
    auto indexPath = _indexConfig->GetIndexPath();
    for (const auto& path : indexPath) {
        auto [existStatus, exist] = segmentDir->IsExist(path).StatusWith();
        RETURN_IF_STATUS_ERROR(existStatus, "is exist [%s] in [%s] failed", path.c_str(),
                               segmentDir->DebugString().c_str());
        if (!exist) {
            continue;
        }
        auto [status, dir] = segmentDir->GetDirectory(path).StatusWith();
        RETURN_IF_STATUS_ERROR(status, "get dir[%s] from [%s] failed", path.c_str(), segmentDir->DebugString().c_str());
        dirs.push_back(dir);
    }
    _indexDirs.push_back({segId, std::move(dirs)});
    return Status::OK();
}

IndexerDirectoryIterator IndexerDirectories::CreateIndexerDirectoryIterator() const
{
    return IndexerDirectoryIterator(_indexDirs);
}

} // namespace indexlibv2::index
