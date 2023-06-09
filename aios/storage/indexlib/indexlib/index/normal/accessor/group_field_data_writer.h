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
#ifndef __INDEXLIB_GROUP_FIELD_DATA_WRITER_H
#define __INDEXLIB_GROUP_FIELD_DATA_WRITER_H

#include <memory>

#include "autil/ConstString.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/common_define.h"
#include "indexlib/config/FileCompressConfig.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/index/data_structure/var_len_data_dumper.h"
#include "indexlib/index/data_structure/var_len_data_writer.h"
#include "indexlib/index_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/memory_control/BuildResourceMetrics.h"

DECLARE_REFERENCE_CLASS(util, MMapAllocator);
namespace indexlib { namespace index {

class GroupFieldDataWriter
{
public:
    GroupFieldDataWriter(const std::shared_ptr<config::FileCompressConfig>& config)
        : mFileCompressConfig(config)
        , mAccessor(NULL)
        , mBuildResourceMetricsNode(NULL)
    {
    }

    ~GroupFieldDataWriter()
    {
        if (mAccessor) {
            mAccessor->~VarLenDataAccessor();
        }
        mPool.reset();
    }

public:
    void Init(const std::string& dataFileName, const std::string& offsetFileName, const VarLenDataParam& param,
              util::BuildResourceMetrics* buildResourceMetrics);

    // If any validation needs to be done, add a preprocess check instead of returning false here.
    void AddDocument(const autil::StringView& data);

    void Dump(const file_system::DirectoryPtr& directory, autil::mem_pool::PoolBase* dumpPool,
              const std::string& temperatureLayer);
    uint32_t GetNumDocuments() const { return mAccessor->GetDocCount(); }
    VarLenDataAccessor* GetDataAccessor() { return mAccessor; }

private:
    void InitMemoryPool();
    void UpdateBuildResourceMetrics();

private:
    std::shared_ptr<config::FileCompressConfig> mFileCompressConfig;
    std::string mDataFileName;
    std::string mOffsetFileName;
    VarLenDataAccessor* mAccessor;
    VarLenDataParam mOutputParam;
    util::MMapAllocatorPtr mAllocator;
    std::shared_ptr<autil::mem_pool::Pool> mPool;
    util::BuildResourceMetricsNode* mBuildResourceMetricsNode;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(GroupFieldDataWriter);
}} // namespace indexlib::index

#endif //__INDEXLIB_GROUP_FIELD_DATA_WRITER_H
