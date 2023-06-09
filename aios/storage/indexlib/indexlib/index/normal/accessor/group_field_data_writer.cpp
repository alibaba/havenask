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
#include "indexlib/index/normal/accessor/group_field_data_writer.h"

#include "indexlib/index/common/FSWriterParamDecider.h"
#include "indexlib/index/data_structure/var_len_data_dumper.h"
#include "indexlib/index/data_structure/var_len_data_param_helper.h"
#include "indexlib/index/util/file_compress_param_helper.h"
#include "indexlib/util/MMapAllocator.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/memory_control/BuildResourceMetrics.h"

using namespace std;
using autil::StringView;
using autil::mem_pool::Pool;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, GroupFieldDataWriter);

void GroupFieldDataWriter::InitMemoryPool()
{
    mAllocator.reset(new util::MMapAllocator);
    mPool.reset(new autil::mem_pool::Pool(mAllocator.get(), DEFAULT_CHUNK_SIZE * 1024 * 1024));
}

void GroupFieldDataWriter::Init(const std::string& dataFileName, const std::string& offsetFileName,
                                const VarLenDataParam& param, util::BuildResourceMetrics* buildResourceMetrics)
{
    mDataFileName = dataFileName;
    mOffsetFileName = offsetFileName;
    mOutputParam = param;

    InitMemoryPool();
    assert(mPool);
    char* buffer = (char*)mPool->allocate(sizeof(VarLenDataAccessor));
    mAccessor = new (buffer) VarLenDataAccessor();
    mAccessor->Init(mPool.get(), param.dataItemUniqEncode);

    if (buildResourceMetrics) {
        mBuildResourceMetricsNode = buildResourceMetrics->AllocateNode();
        IE_LOG(INFO, "allocate build resource node [id:%d] for GroupFieldDataWriter",
               mBuildResourceMetricsNode->GetNodeId());
        UpdateBuildResourceMetrics();
    }
}

void GroupFieldDataWriter::AddDocument(const StringView& data)
{
    mAccessor->AppendValue(data);
    UpdateBuildResourceMetrics();
}

void GroupFieldDataWriter::Dump(const file_system::DirectoryPtr& directory, autil::mem_pool::PoolBase* dumpPool,
                                const std::string& temperatureLayer)
{
    FileCompressParamHelper::SyncParam(mFileCompressConfig, temperatureLayer, mOutputParam);
    VarLenDataDumper dumper;
    dumper.Init(mAccessor, mOutputParam);
    dumper.Dump(directory, mOffsetFileName, mDataFileName, FSWriterParamDeciderPtr(), nullptr, dumpPool);
}

void GroupFieldDataWriter::UpdateBuildResourceMetrics()
{
    if (!mBuildResourceMetricsNode) {
        return;
    }

    int64_t poolSize = mPool->getUsedBytes();
    int64_t dumpFileSize = poolSize;
    mBuildResourceMetricsNode->Update(util::BMT_CURRENT_MEMORY_USE, poolSize);
    mBuildResourceMetricsNode->Update(util::BMT_DUMP_TEMP_MEMORY_SIZE, 0);
    mBuildResourceMetricsNode->Update(util::BMT_DUMP_EXPAND_MEMORY_SIZE, 0);
    mBuildResourceMetricsNode->Update(util::BMT_DUMP_FILE_SIZE, dumpFileSize);
}
}} // namespace indexlib::index
