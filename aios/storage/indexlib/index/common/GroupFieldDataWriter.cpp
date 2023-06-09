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
#include "indexlib/index/common/GroupFieldDataWriter.h"

#include "autil/ConstString.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/config/FileCompressConfig.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/index/BuildingIndexMemoryUseUpdater.h"
#include "indexlib/index/DocMapDumpParams.h"
#include "indexlib/index/common/FileCompressParamHelper.h"
#include "indexlib/index/common/data_structure/VarLenDataDumper.h"
#include "indexlib/index/common/data_structure/VarLenDataParamHelper.h"
#include "indexlib/util/MMapAllocator.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, GroupFieldDataWriter);

GroupFieldDataWriter::GroupFieldDataWriter(const std::shared_ptr<config::FileCompressConfigV2>& fileCompressConfig)
    : _fileCompressConfig(fileCompressConfig)
    , _accessor(nullptr)
{
}

GroupFieldDataWriter::~GroupFieldDataWriter()
{
    if (_accessor) {
        _accessor->~VarLenDataAccessor();
    }
    _pool.reset();
}

void GroupFieldDataWriter::InitMemoryPool()
{
    _allocator.reset(new indexlib::util::MMapAllocator);
    _pool.reset(new autil::mem_pool::Pool(_allocator.get(), DEFAULT_CHUNK_SIZE * 1024 * 1024));
}

void GroupFieldDataWriter::Init(const std::string& dataFileName, const std::string& offsetFileName,
                                const VarLenDataParam& param)
{
    _dataFileName = dataFileName;
    _offsetFileName = offsetFileName;
    _outputParam = param;

    InitMemoryPool();
    assert(_pool);
    char* buffer = (char*)_pool->allocate(sizeof(VarLenDataAccessor));
    _accessor = new (buffer) VarLenDataAccessor();
    _accessor->Init(_pool.get(), param.dataItemUniqEncode);
}

void GroupFieldDataWriter::AddDocument(const autil::StringView& data) { _accessor->AppendValue(data); }

Status GroupFieldDataWriter::Dump(const std::shared_ptr<indexlib::file_system::IDirectory>& directory,
                                  autil::mem_pool::PoolBase* dumpPool,
                                  const std::shared_ptr<framework::DumpParams>& params)
{
    FileCompressParamHelper::SyncParam(_fileCompressConfig, nullptr, _outputParam);
    VarLenDataDumper dumper;
    dumper.Init(_accessor, _outputParam);
    auto orderParams = std::dynamic_pointer_cast<DocMapDumpParams>(params);
    std::vector<docid_t>* new2old = orderParams ? &orderParams->new2old : nullptr;
    return dumper.Dump(directory, _offsetFileName, _dataFileName, nullptr, new2old, dumpPool);
}

void GroupFieldDataWriter::UpdateMemUse(BuildingIndexMemoryUseUpdater* memUpdater)
{
    if (!memUpdater) {
        return;
    }

    int64_t poolSize = _pool->getUsedBytes();
    int64_t dumpFileSize = poolSize;
    memUpdater->UpdateCurrentMemUse(poolSize);
    memUpdater->EstimateDumpTmpMemUse(0);
    memUpdater->EstimateDumpExpandMemUse(0);
    memUpdater->EstimateDumpedFileSize(dumpFileSize);
}

} // namespace indexlibv2::index
