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
#include <unordered_map>

#include "indexlib/common_define.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/index/common/FSWriterParamDecider.h"
#include "indexlib/index/data_structure/adaptive_attribute_offset_dumper.h"
#include "indexlib/index/data_structure/var_len_data_param.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class VarLenDataWriter
{
public:
    VarLenDataWriter(autil::mem_pool::PoolBase* pool);
    ~VarLenDataWriter();

public:
    void Init(const file_system::DirectoryPtr& directory, const std::string& offsetFileName,
              const std::string& dataFileName, const FSWriterParamDeciderPtr& fsWriterParamDecider,
              const VarLenDataParam& dumpParam);

    void Init(const file_system::FileWriterPtr& offsetFile, const file_system::FileWriterPtr& dataFile,
              const VarLenDataParam& dumpParam);

    void Reserve(uint32_t docCount);

    uint64_t GetHashValue(const autil::StringView& data);
    void AppendValue(const autil::StringView& value);
    void AppendValue(const autil::StringView& value, uint64_t hash);
    void Close();

    uint32_t GetDataItemCount() const { return mDataItemCount; }
    uint64_t GetMaxItemLength() const { return mMaxItemLen; }
    uint64_t GetOffsetCount() const { return mOffsetDumper.Size(); }
    uint64_t GetOffset(size_t pos) const { return mOffsetDumper.GetOffset(pos); }

    const file_system::FileWriterPtr& GetOffsetFileWriter() const { return mOffsetWriter; }
    const file_system::FileWriterPtr& GetDataFileWriter() const { return mDataWriter; }

public:
    // for uniq var_num_merger
    uint64_t AppendValueWithoutOffset(const autil::StringView& value, uint64_t hash);
    void AppendOffset(uint64_t offset);
    void SetOffset(size_t pos, uint64_t offset);

private:
    void WriteOneDataItem(const autil::StringView& value);

private:
    typedef autil::mem_pool::pool_allocator<std::pair<const uint64_t, uint64_t>> AllocatorType;
    typedef std::unordered_map<uint64_t, uint64_t, std::hash<uint64_t>, std::equal_to<uint64_t>, AllocatorType>
        OffsetMap;
    DEFINE_SHARED_PTR(OffsetMap);

private:
    VarLenDataParam mOutputParam;
    uint32_t mDataItemCount;
    uint64_t mCurrentOffset;
    uint64_t mMaxItemLen;
    autil::mem_pool::PoolBase* mDumpPool;
    autil::mem_pool::PoolBase* mOffsetMapPool;
    file_system::FileWriterPtr mOffsetWriter;
    file_system::FileWriterPtr mDataWriter;

    AdaptiveAttributeOffsetDumper mOffsetDumper;
    OffsetMapPtr mOffsetMap;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(VarLenDataWriter);
}} // namespace indexlib::index
