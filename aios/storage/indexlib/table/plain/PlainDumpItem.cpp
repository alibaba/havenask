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
#include "indexlib/table/plain/PlainDumpItem.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/index/IMemIndexer.h"

namespace indexlibv2::plain {
AUTIL_LOG_SETUP(indexlib.plain, PlainDumpItem);

PlainDumpItem::PlainDumpItem(const std::shared_ptr<autil::mem_pool::PoolBase>& dumpPool,
                             const std::shared_ptr<index::IMemIndexer>& buildingIndex,
                             const std::shared_ptr<indexlib::file_system::Directory>& dir,
                             const std::shared_ptr<framework::DumpParams>& dumpParams)
    : _dumpPool(dumpPool)
    , _buildingIndex(buildingIndex)
    , _dir(dir)
    , _params(dumpParams)
    , _dumped(false)
{
}

Status PlainDumpItem::Dump() noexcept
{
    AUTIL_LOG(INFO, "begin dump index [%s]", _buildingIndex->GetIndexName().c_str());
    auto status = _buildingIndex->Dump(_dumpPool.get(), _dir, _params);
    if (status.IsOK()) {
        _dumped = true;
    }
    AUTIL_LOG(INFO, "end dump index [%s]", _buildingIndex->GetIndexName().c_str());
    return status;
}

} // namespace indexlibv2::plain
