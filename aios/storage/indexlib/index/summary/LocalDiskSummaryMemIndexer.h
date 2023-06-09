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
#include "autil/StringView.h"
#include "indexlib/index/common/GroupFieldDataWriter.h"
#include "indexlib/index/summary/config/SummaryGroupConfig.h"

namespace indexlib { namespace file_system {
class IDirectory;
}} // namespace indexlib::file_system
namespace autil { namespace mem_pool {
class PoolBase;
}} // namespace autil::mem_pool

namespace indexlibv2::framework {
struct DumpParams;
}
namespace indexlibv2::index {
class SummaryMemReader;
class BuildingIndexMemoryUseUpdater;

class LocalDiskSummaryMemIndexer : private autil::NoCopyable
{
public:
    LocalDiskSummaryMemIndexer() = default;
    ~LocalDiskSummaryMemIndexer() = default;

public:
    void Init(const std::shared_ptr<indexlibv2::config::SummaryGroupConfig>& summaryGroupConfig);
    void AddDocument(const autil::StringView& data);
    Status Dump(const std::shared_ptr<indexlib::file_system::IDirectory>& directory,
                autil::mem_pool::PoolBase* dumpPool, const std::shared_ptr<framework::DumpParams>& params);
    const std::shared_ptr<SummaryMemReader> CreateInMemReader();
    void UpdateMemUse(BuildingIndexMemoryUseUpdater* memUpdater);
    uint32_t GetNumDocuments() const { return _dataWriter->GetNumDocuments(); }
    const std::string& GetGroupName() const { return _summaryGroupConfig->GetGroupName(); }

private:
    std::shared_ptr<indexlibv2::config::SummaryGroupConfig> _summaryGroupConfig;
    std::shared_ptr<GroupFieldDataWriter> _dataWriter;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
