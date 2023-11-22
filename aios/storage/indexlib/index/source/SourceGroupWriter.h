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

#include "autil/ConstString.h"
#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "indexlib/base/Status.h"
#include "indexlib/index/common/GroupFieldDataWriter.h"
#include "indexlib/index/common/data_structure/VarLenDataAccessor.h"

namespace indexlibv2::framework {
struct DumpParams;
}
namespace indexlib { namespace file_system {
class IDirectory;
}} // namespace indexlib::file_system
namespace autil::mem_pool {
class PoolBase;
} // namespace autil::mem_pool
namespace indexlibv2::index {
class GroupFieldDataWriter;
}
namespace indexlib::config {
class SourceGroupConfig;
}

namespace indexlibv2::index {
class BuildingIndexMemoryUseUpdater;

class SourceGroupWriter : private autil::NoCopyable
{
public:
    SourceGroupWriter();
    ~SourceGroupWriter();

public:
    void Init(const std::shared_ptr<indexlib::config::SourceGroupConfig>& groupConfig);

    void UpdateMemUse(BuildingIndexMemoryUseUpdater* memUpdater);

    // If any validation needs to be done, add a preprocess check instead of returning false here.
    void AddDocument(const autil::StringView& data);

    Status Dump(const std::shared_ptr<indexlib::file_system::IDirectory>& directory,
                autil::mem_pool::PoolBase* dumpPool, const std::shared_ptr<framework::DumpParams>& params);
    VarLenDataAccessor* GetDataAccessor() const { return _dataWriter->GetDataAccessor(); }

private:
    std::shared_ptr<indexlib::config::SourceGroupConfig> _groupConfig;
    std::shared_ptr<GroupFieldDataWriter> _dataWriter;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
