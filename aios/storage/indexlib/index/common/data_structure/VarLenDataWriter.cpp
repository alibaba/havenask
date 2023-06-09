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
#include "indexlib/index/common/data_structure/VarLenDataWriter.h"

#include "indexlib/file_system/file/CompressFileWriter.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/index/common/DefaultFSWriterParamDecider.h"
#include "indexlib/index/common/FSWriterParamDecider.h"
#include "indexlib/index/common/field_format/attribute/MultiValueAttributeFormatter.h"
#include "indexlib/util/KeyHasherTyped.h"
using namespace autil;
using namespace autil::mem_pool;
using namespace indexlib::util;
using namespace indexlib::file_system;

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, VarLenDataWriter);

VarLenDataWriter::VarLenDataWriter(PoolBase* pool)
    : _dataItemCount(0)
    , _currentOffset(0)
    , _maxItemLen(0)
    , _dumpPool(pool)
    , _offsetDumper(_dumpPool)
{
    assert(_dumpPool);
    _offsetMapPool = new autil::mem_pool::Pool;
}

VarLenDataWriter::~VarLenDataWriter() { DELETE_AND_SET_NULL(_offsetMapPool); }

Status VarLenDataWriter::Init(const std::shared_ptr<indexlib::file_system::IDirectory>& dir,
                              const std::string& offsetFileName, const std::string& dataFileName,
                              const std::shared_ptr<indexlib::index::FSWriterParamDecider>& fsWriterParamDecider,
                              const VarLenDataParam& dumpParam)
{
    std::shared_ptr<indexlib::index::FSWriterParamDecider> paramDecider =
        fsWriterParamDecider
            ? fsWriterParamDecider
            : std::shared_ptr<indexlib::index::FSWriterParamDecider>(new indexlib::index::DefaultFSWriterParamDecider);
    indexlib::file_system::WriterOption writerOption = paramDecider->MakeParam(dataFileName);
    dumpParam.SyncCompressParam(writerOption);
    auto [dataSt, dataFile] = dir->CreateFileWriter(dataFileName, writerOption).StatusWith();
    RETURN_IF_STATUS_ERROR(dataSt, "create file writer failed, file:[%s]", dataFileName.c_str());

    writerOption = paramDecider->MakeParam(offsetFileName);
    dumpParam.SyncCompressParam(writerOption);
    auto [offsetSt, offsetFile] = dir->CreateFileWriter(offsetFileName, writerOption).StatusWith();
    RETURN_IF_STATUS_ERROR(offsetSt, "create offset writer failed, file:[%s]", offsetFileName.c_str());
    return Init(offsetFile, dataFile, dumpParam);
}

Status VarLenDataWriter::Init(const std::shared_ptr<indexlib::file_system::FileWriter>& offsetFile,
                              const std::shared_ptr<indexlib::file_system::FileWriter>& dataFile,
                              const VarLenDataParam& dumpParam)
{
    _offsetDumper.Init(dumpParam.enableAdaptiveOffset, dumpParam.equalCompressOffset, dumpParam.offsetThreshold);

    _outputParam = dumpParam;
    _offsetWriter = offsetFile;
    _dataWriter = dataFile;

    if (!dumpParam.dataCompressorName.empty()) {
        auto compressWriter = std::dynamic_pointer_cast<CompressFileWriter>(dataFile);
        if (compressWriter && compressWriter->GetCompressorName() != dumpParam.dataCompressorName) {
            AUTIL_LOG(ERROR, "mismatch data compressor, name in compress writer[%s], name in dump param[%s]",
                      compressWriter->GetCompressorName().c_str(), dumpParam.dataCompressorName.c_str());
            return Status::Corruption("mismatch data compressor name");
        }
        compressWriter = std::dynamic_pointer_cast<CompressFileWriter>(offsetFile);
        if (compressWriter && compressWriter->GetCompressorName() != dumpParam.dataCompressorName) {
            AUTIL_LOG(ERROR, "mismatch offset compressor, name in compress writer[%s], name in dump param[%s]",
                      compressWriter->GetCompressorName().c_str(), dumpParam.dataCompressorName.c_str());
            return Status::Corruption("mismatch offset compressor name");
        }
    }

    _dataItemCount = 0;
    _currentOffset = 0;
    _maxItemLen = 0;

    if (dumpParam.dataItemUniqEncode) {
        AllocatorType allocator(_offsetMapPool);
        _offsetMap.reset(new OffsetMap(10, OffsetMap::hasher(), OffsetMap::key_equal(), allocator));
    }
    return Status::OK();
}

void VarLenDataWriter::Reserve(uint32_t docCount) { _offsetDumper.Reserve(docCount + 1); }

Status VarLenDataWriter::AppendValue(const StringView& value)
{
    uint64_t hash = GetHashValue(value);
    return AppendValue(value, hash);
}

Status VarLenDataWriter::AppendValue(const StringView& value, uint64_t hash)
{
    auto [st, offset] = AppendValueWithoutOffset(value, hash);
    RETURN_IF_STATUS_ERROR(st, "append value without offset failed.");
    _offsetDumper.PushBack(offset);
    return Status::OK();
}

Status VarLenDataWriter::Close()
{
    _offsetMap.reset();
    DELETE_AND_SET_NULL(_offsetMapPool);
    if (!_dataWriter || !_offsetWriter) {
        return Status::OK();
    }

    auto st = _dataWriter->Close().Status();
    RETURN_IF_STATUS_ERROR(st, "close data writer failed.");
    if (!_outputParam.disableGuardOffset) {
        _offsetDumper.PushBack(_currentOffset);
    }
    if (_offsetDumper.Size() > 0) {
        st = _offsetDumper.Dump(_offsetWriter);
        RETURN_IF_STATUS_ERROR(st, "dump offset failed.");
        _offsetDumper.Clear();
    }
    st = _offsetWriter->Close().Status();
    RETURN_IF_STATUS_ERROR(st, "close offset writer failed.");
    _dataWriter.reset();
    _offsetWriter.reset();
    return Status::OK();
}

Status VarLenDataWriter::WriteOneDataItem(const StringView& value)
{
    assert(_dataWriter);
    size_t ret = 0;
    if (_outputParam.appendDataItemLength) {
        char buffer[10];
        ret = MultiValueAttributeFormatter::EncodeCount(value.size(), buffer, 10);
        auto [st, _] = _dataWriter->Write(buffer, ret).StatusWith();
        RETURN_IF_STATUS_ERROR(st, "var len data write item len failed, count[%lu]", ret);
    }

    if (value.size() > 0) {
        auto [st, _] = _dataWriter->Write(value.data(), value.size()).StatusWith();
        RETURN_IF_STATUS_ERROR(st, "var len data write failed, size[%lu]", value.size());
    }
    ret += value.size();
    ++_dataItemCount;
    _maxItemLen = std::max(_maxItemLen, ret);
    _currentOffset += ret;
    return Status::OK();
}

uint64_t VarLenDataWriter::GetHashValue(const StringView& data)
{
    uint64_t hashValue = 0;
    if (_offsetMap) {
        MurmurHasher::GetHashKey(data, hashValue);
    }
    return hashValue;
}

void VarLenDataWriter::SetOffset(size_t pos, uint64_t offset) { _offsetDumper.SetOffset(pos, offset); }

std::pair<Status, uint64_t> VarLenDataWriter::AppendValueWithoutOffset(const StringView& value, uint64_t hash)
{
    uint64_t offset = _currentOffset;
    if (!_offsetMap) {
        auto st = WriteOneDataItem(value);
        RETURN2_IF_STATUS_ERROR(st, 0, "write item without offset map failed.");
        return std::make_pair(st, offset);
    }

    OffsetMap::const_iterator offsetIter = _offsetMap->find(hash);
    if (offsetIter != _offsetMap->end()) {
        offset = offsetIter->second;
    } else {
        auto st = WriteOneDataItem(value);
        RETURN2_IF_STATUS_ERROR(st, 0, "write item failed.");
        _offsetMap->insert(std::make_pair(hash, offset));
    }
    return std::make_pair(Status::OK(), offset);
}

void VarLenDataWriter::AppendOffset(uint64_t offset) { _offsetDumper.PushBack(offset); }
} // namespace indexlibv2::index
