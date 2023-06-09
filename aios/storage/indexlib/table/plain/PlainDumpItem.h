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
#include "autil/mem_pool/Pool.h"
#include "indexlib/framework/SegmentDumpItem.h"

namespace indexlibv2::index {
class IMemIndexer;
} // namespace indexlibv2::index

namespace indexlibv2::framework {
struct DumpParams;
} // namespace indexlibv2::framework

namespace indexlib::file_system {
class Directory;
}

namespace indexlibv2::plain {

class PlainDumpItem : public framework::SegmentDumpItem
{
public:
    PlainDumpItem(const std::shared_ptr<autil::mem_pool::PoolBase>& dumpPool,
                  const std::shared_ptr<index::IMemIndexer>& buildingIndex,
                  const std::shared_ptr<indexlib::file_system::Directory>& dir,
                  const std::shared_ptr<framework::DumpParams>& dumpParams);

    PlainDumpItem(const PlainDumpItem&) = delete;
    PlainDumpItem& operator=(const PlainDumpItem&) = delete;
    PlainDumpItem(PlainDumpItem&&) = delete;
    PlainDumpItem& operator=(PlainDumpItem&&) = delete;

    ~PlainDumpItem() = default;

    Status Dump() noexcept override;
    bool IsDumped() const override { return _dumped; }

private:
    std::shared_ptr<autil::mem_pool::PoolBase> _dumpPool;
    std::shared_ptr<index::IMemIndexer> _buildingIndex;
    std::shared_ptr<indexlib::file_system::Directory> _dir;
    std::shared_ptr<framework::DumpParams> _params;
    bool _dumped;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::plain
