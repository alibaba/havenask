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
#include "indexlib/index/kv/InMemoryValueWriter.h"

#include "autil/Log.h"
#include "autil/MultiValueType.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/WriterOption.h"
#include "indexlib/file_system/file/CompressFileWriter.h"
#include "indexlib/index/common/data_structure/ExpandableValueAccessor.h"
#include "indexlib/index/kv/KVCommonDefine.h"
#include "indexlib/index/kv/MemoryUsage.h"
#include "indexlib/index/kv/ReclaimedValueCollector.h"
#include "indexlib/index/kv/SegmentStatistics.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"
#include "indexlib/index/kv/config/KVIndexPreference.h"
#include "indexlib/index/kv/config/ValueConfig.h"
#include "indexlib/util/ValueWriter.h"

namespace indexlibv2::index {
AUTIL_DECLARE_AND_SETUP_LOGGER(indexlib.index, InMemoryValueWriter);

InMemoryValueWriter::InMemoryValueWriter(const std::shared_ptr<indexlibv2::config::KVIndexConfig>& indexConfig)
    : _indexConfig(indexConfig)
{
}

InMemoryValueWriter::~InMemoryValueWriter() {}

Status InMemoryValueWriter::Init(autil::mem_pool::PoolBase* pool, size_t maxValueMemUse, float lastValueCompressRatio,
                                 indexlibv2::framework::IIndexMemoryReclaimer* memReclaimer)
{
    bool enableRewrite = false;
    _lastValueCompressRatio = lastValueCompressRatio;
    if (memReclaimer != nullptr) {
        _reclaimedValueCollector = ReclaimedValueCollector<uint64_t>::Create(memReclaimer);
        enableRewrite = true;
    }

    const uint64_t kMaxSliceLen = 32 * 1024 * 1024; // 32MB;
    size_t sliceNum = (maxValueMemUse > kMaxSliceLen) ? std::ceil(maxValueMemUse * 1.0 / kMaxSliceLen) : 1;
    uint64_t sliceLen = maxValueMemUse / sliceNum; // 16MB ~ 32MB
    _valueAccessor = std::make_shared<indexlibv2::index::ExpandableValueAccessor<uint64_t>>(pool, enableRewrite);
    return _valueAccessor->Init(sliceLen, sliceNum);
}

bool InMemoryValueWriter::IsFull() const { return _valueAccessor->GetUsedBytes() >= _valueAccessor->GetReserveBytes(); }

Status InMemoryValueWriter::Write(const autil::StringView& data)
{
    return Status::Unimplement("use another Write method with offset param instead");
}

Status InMemoryValueWriter::Dump(const std::shared_ptr<indexlib::file_system::Directory>& directory)
{
    auto [status, valueFileWriter] = CreateValueFileWriter(directory);
    if (!status.IsOK()) {
        return status;
    }
    auto result = _valueAccessor->Dump(valueFileWriter);
    status = valueFileWriter->Close().Status();
    if (!status.IsOK()) {
        return status;
    }
    if (!result.OK()) {
        return result.Status();
    }

    if (NeedCompress()) {
        double ratio = 1.0 * result.Value() / GetLength();
        AUTIL_LOG(INFO, "value compression ratio for index %s is [%f]", _indexConfig->GetIndexName().c_str(), ratio);
    } else if (result.Value() != GetLength()) {
        return Status::IOError("write kv value failed, expected [%ld] actual [%lu]", GetLength(), result.Value());
    }
    return status;
}

const char* InMemoryValueWriter::GetBaseAddress() const
{
    assert(false);
    return nullptr;
}

int64_t InMemoryValueWriter::GetLength() const { return _valueAccessor->GetUsedBytes(); }

void InMemoryValueWriter::FillStatistics(SegmentStatistics& stat) const
{
    stat.valueMemoryUse = _valueAccessor->GetUsedBytes();
}

void InMemoryValueWriter::FillMemoryUsage(MemoryUsage& memUsage) const
{
    memUsage.buildMemory = _valueAccessor->GetExpandMemoryInBytes();
    memUsage.dumpMemory = 0;
    memUsage.dumpedFileSize = _valueAccessor->GetUsedBytes();
    if (NeedCompress()) {
        memUsage.dumpedFileSize *= _lastValueCompressRatio;
        const auto& valueParam = _indexConfig->GetIndexPreference().GetValueParam();
        memUsage.buildMemory += indexlib::file_system::CompressFileWriter::EstimateCompressBufferSize(
            valueParam.GetFileCompressType(), valueParam.GetFileCompressBufferSize(),
            valueParam.GetFileCompressParameter());
    }
}

bool InMemoryValueWriter::NeedCompress() const
{
    const auto& valueParam = _indexConfig->GetIndexPreference().GetValueParam();
    return valueParam.EnableFileCompress();
}

std::pair<Status, std::shared_ptr<indexlib::file_system::FileWriter>>
InMemoryValueWriter::CreateValueFileWriter(const std::shared_ptr<indexlib::file_system::Directory>& directory) const
{
    indexlib::file_system::WriterOption opts;
    FillWriterOption(*_indexConfig, opts);
    auto impl = directory->GetIDirectory();
    return impl->CreateFileWriter(KV_VALUE_FILE_NAME, opts).StatusWith();
}

Status InMemoryValueWriter::Write(const autil::StringView& data, uint64_t& valueOffset)
{
    if (_reclaimedValueCollector != nullptr) {
        size_t length = data.size();
        uint64_t reclaimedValueOffset = 0ul;
        if (_reclaimedValueCollector->PopLengthEqualTo(length, reclaimedValueOffset)) {
            valueOffset = reclaimedValueOffset;
            return _valueAccessor->Rewrite(data, reclaimedValueOffset);
        }
    }
    return Append(data, valueOffset);
}

Status InMemoryValueWriter::ReclaimValue(uint64_t valueOffset)
{
    if (_reclaimedValueCollector == nullptr) {
        return Status::Unimplement("no support for value offset reclaim");
    }
    if (valueOffset >= _valueAccessor->GetUsedBytes()) {
        return Status::OutOfRange("value offset [%lu] no smaller than used bytes [%lu]", valueOffset,
                                  _valueAccessor->GetUsedBytes());
    }

    int32_t valueLength = _indexConfig->GetValueConfig()->GetFixedLength();
    if (valueLength < 0) {
        autil::MultiChar mc(_valueAccessor->GetValue(valueOffset));
        valueLength = mc.length();
    }

    if (valueOffset + valueLength > _valueAccessor->GetUsedBytes()) {
        return Status::OutOfRange("addition of value offset [%lu] and length [%lu] larger than used bytes [%lu]",
                                  valueOffset, valueLength, _valueAccessor->GetUsedBytes());
    }
    _reclaimedValueCollector->Collect(valueLength, valueOffset);
    return Status::OK();
}

Status InMemoryValueWriter::Append(const autil::StringView& data, uint64_t& valueOffset)
{
    Status status = _valueAccessor->Append(data, valueOffset);
    if (status.IsNoMem()) {
        AUTIL_LOG(INFO, "append failed, message: [%s]", status.ToString().c_str());
        return Status::NeedDump("value no space");
    }
    return status;
}

const std::shared_ptr<indexlibv2::index::ExpandableValueAccessor<uint64_t>>&
InMemoryValueWriter::GetValueAccessor() const
{
    return _valueAccessor;
}

} // namespace indexlibv2::index
