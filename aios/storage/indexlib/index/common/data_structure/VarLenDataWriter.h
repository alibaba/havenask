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

#include "indexlib/base/Status.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/index/common/data_structure/AdaptiveAttributeOffsetDumper.h"
#include "indexlib/index/common/data_structure/VarLenDataParam.h"

namespace indexlib::index {
class FSWriterParamDecider;
}

namespace indexlibv2::index {

class VarLenDataWriter
{
public:
    explicit VarLenDataWriter(autil::mem_pool::PoolBase* pool);
    ~VarLenDataWriter();

public:
    Status Init(const std::shared_ptr<indexlib::file_system::IDirectory>& dir, const std::string& offsetFileName,
                const std::string& dataFileName,
                const std::shared_ptr<indexlib::index::FSWriterParamDecider>& fsWriterParamDecider,
                const VarLenDataParam& dumpParam);

    Status Init(const std::shared_ptr<indexlib::file_system::FileWriter>& offsetFile,
                const std::shared_ptr<indexlib::file_system::FileWriter>& dataFile, const VarLenDataParam& dumpParam);

    void Reserve(uint32_t docCount);

    uint64_t GetHashValue(const autil::StringView& data);
    Status AppendValue(const autil::StringView& value);
    Status AppendValue(const autil::StringView& value, uint64_t hash);
    Status Close();

    uint32_t GetDataItemCount() const { return _dataItemCount; }
    uint64_t GetMaxItemLength() const { return _maxItemLen; }
    uint64_t GetOffsetCount() const { return _offsetDumper.Size(); }
    uint64_t GetOffset(size_t pos) const { return _offsetDumper.GetOffset(pos); }

    const std::shared_ptr<indexlib::file_system::FileWriter>& GetOffsetFileWriter() const { return _offsetWriter; }
    const std::shared_ptr<indexlib::file_system::FileWriter>& GetDataFileWriter() const { return _dataWriter; }

public:
    // for uniq var_num_merger
    std::pair<Status, uint64_t> AppendValueWithoutOffset(const autil::StringView& value, uint64_t hash);
    void AppendOffset(uint64_t offset);
    void SetOffset(size_t pos, uint64_t offset);

private:
    Status WriteOneDataItem(const autil::StringView& value);

private:
    typedef autil::mem_pool::pool_allocator<std::pair<const uint64_t, uint64_t>> AllocatorType;
    typedef std::unordered_map<uint64_t, uint64_t, std::hash<uint64_t>, std::equal_to<uint64_t>, AllocatorType>
        OffsetMap;

private:
    VarLenDataParam _outputParam;
    uint32_t _dataItemCount;
    uint64_t _currentOffset;
    uint64_t _maxItemLen;
    autil::mem_pool::PoolBase* _dumpPool;
    autil::mem_pool::PoolBase* _offsetMapPool;
    std::shared_ptr<indexlib::file_system::FileWriter> _offsetWriter;
    std::shared_ptr<indexlib::file_system::FileWriter> _dataWriter;

    AdaptiveAttributeOffsetDumper _offsetDumper;
    std::shared_ptr<OffsetMap> _offsetMap;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
