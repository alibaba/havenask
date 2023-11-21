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
#include "indexlib/base/Status.h"
#include "indexlib/index/common/data_structure/VarLenDataAccessor.h"
#include "indexlib/index/common/data_structure/VarLenDataParam.h"

namespace indexlib {
namespace file_system {
class IDirectory;
}

namespace util {
class MMapAllocator;
}
} // namespace indexlib

namespace autil::mem_pool {
class PoolBase;
class Pool;
} // namespace autil::mem_pool

namespace indexlibv2::framework {
struct DumpParams;
}

namespace indexlibv2::config {
class FileCompressConfigV2;
}

namespace indexlibv2::index {
class VarLenDataAccessor;
class BuildingIndexMemoryUseUpdater;

class GroupFieldDataWriter : private autil::NoCopyable
{
public:
    GroupFieldDataWriter(const std::shared_ptr<config::FileCompressConfigV2>& fileCompressConfig);
    ~GroupFieldDataWriter();

public:
    void Init(const std::string& dataFileName, const std::string& offsetFileName, const VarLenDataParam& param);

    // If any validation needs to be done, add a preprocess check instead of returning false here.
    void AddDocument(const autil::StringView& data);

    Status Dump(const std::shared_ptr<indexlib::file_system::IDirectory>& directory,
                autil::mem_pool::PoolBase* dumpPool, const std::shared_ptr<framework::DumpParams>& params);
    uint32_t GetNumDocuments() const { return _accessor->GetDocCount(); }
    VarLenDataAccessor* GetDataAccessor() const { return _accessor; }
    void UpdateMemUse(BuildingIndexMemoryUseUpdater* memUpdater);

private:
    void InitMemoryPool();

private:
    std::shared_ptr<config::FileCompressConfigV2> _fileCompressConfig;
    std::string _dataFileName;
    std::string _offsetFileName;
    VarLenDataAccessor* _accessor;
    VarLenDataParam _outputParam;
    std::shared_ptr<indexlib::util::MMapAllocator> _allocator;
    std::shared_ptr<autil::mem_pool::Pool> _pool;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
