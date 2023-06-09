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
#include "indexlib/file_system/file/CompressDataDumper.h"

#include "autil/EnvUtil.h"
#include "indexlib/file_system/file/CompressFileAddressMapper.h"
#include "indexlib/file_system/file/CompressFileInfo.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/buffer_compressor/BufferCompressor.h"
#include "indexlib/util/buffer_compressor/BufferCompressorCreator.h"
#include "indexlib/util/metrics/KmonitorTagvNormalizer.h"

using namespace std;
using namespace indexlib::util;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, CompressDataDumper);

CompressDataDumper::CompressDataDumper(const std::shared_ptr<FileWriter>& fileWriter,
                                       const std::shared_ptr<FileWriter>& infoWriter,
                                       const std::shared_ptr<FileWriter>& metaWriter,
                                       FileSystemMetricsReporter* reporter) noexcept
    : _bufferSize(0)
    , _writeLength(0)
    , _dataWriter(fileWriter)
    , _infoWriter(infoWriter)
    , _metaWriter(metaWriter)
    , _reporter(reporter)
    , _expandBlockCount(0)
    , _expandWasteSize(0)
    , _encodeCompressAddressMapper(false)
{
    assert(_dataWriter);
    assert(_infoWriter);
}

CompressDataDumper::~CompressDataDumper() noexcept {}

FSResult<void> CompressDataDumper::Init(const string& compressorName, size_t bufferSize,
                                        const KeyValueMap& param) noexcept
{
    _bufferSize = bufferSize;
    _writeLength = 0;
    RETURN_IF_FS_EXCEPTION((_compressor = CreateCompressor(compressorName, bufferSize, param)),
                           "CreateCompressor failed");
    assert(_compressor);

    auto iter = param.find("encode_address_mapper");
    if (iter != param.end()) {
        if (iter->second == "true") {
            _encodeCompressAddressMapper = true;
        }
        if (iter->second == "false") {
            _encodeCompressAddressMapper = false;
        }
    }

    iter = param.find("address_mapper_batch_number");
    size_t addrMapperBatchNum = (iter == param.end()) ? 0 : autil::StringUtil::fromString<size_t>(iter->second);
    _compFileAddrMapper.reset(new CompressFileAddressMapper);
    RETURN_IF_FS_ERROR(_compFileAddrMapper->InitForWrite(bufferSize, addrMapperBatchNum), "InitForWrite failed");
    _compressParam = param;

    if (autil::EnvUtil::getEnv("INDEXLIB_GENERATE_MD5_IN_COMPRESS_INFO", false)) {
        // calculate md5, may cause longer dumpping interval
        _md5Stream.reset(new Md5Stream);
    }

    map<string, string> tagMap;
    if (_reporter) {
        _reporter->ExtractCompressTagInfo(_dataWriter->GetLogicalPath(), compressorName, bufferSize, param, tagMap);
    }
    for (auto kv : tagMap) {
        _kmonTags.AddTag(kv.first, util::KmonitorTagvNormalizer::GetInstance()->Normalize(kv.second));
    }
    return FSEC_OK;
}

FSResult<size_t> CompressDataDumper::Write(const char* buffer, size_t length) noexcept
{
    assert(_compressor);
    const char* cursor = (const char*)buffer;
    size_t leftLen = length;
    while (true) {
        uint32_t leftLenInBuffer = _bufferSize - _compressor->GetBufferInLen();
        uint32_t lengthToWrite = (leftLenInBuffer < leftLen) ? leftLenInBuffer : leftLen;
        _compressor->AddDataToBufferIn(cursor, lengthToWrite);
        cursor += lengthToWrite;
        _writeLength += lengthToWrite;
        leftLen -= lengthToWrite;
        if (leftLen <= 0) {
            assert(leftLen == 0);
            break;
        }

        if (_compressor->GetBufferInLen() == _bufferSize) {
            RETURN2_IF_FS_EXCEPTION(FlushCompressorData(), (length - leftLen), "FlushCompressorData failed");
        }
    }
    if (_md5Stream) {
        _md5Stream->Put((const uint8_t*)buffer, length);
    }
    return {FSEC_OK, length};
}

FSResult<void> CompressDataDumper::Close() noexcept
{
    if (!_compFileAddrMapper) {
        // already dump
        return FSEC_OK;
    }

    // flush data file
    RETURN_IF_FS_EXCEPTION(FlushCompressorData(), "FlushCompressorData failed");

    std::shared_ptr<FileWriter> writer = (_metaWriter != nullptr) ? _metaWriter : _dataWriter;
    size_t addrMapDataLen = 0;
    RETURN_IF_FS_EXCEPTION((addrMapDataLen = _compFileAddrMapper->Dump(writer, _encodeCompressAddressMapper)),
                           "Dump failed");
    RETURN_IF_FS_ERROR(_dataWriter->Close(), "close data writer failed");

    if (_metaWriter) {
        RETURN_IF_FS_ERROR(_metaWriter->Close(), "close meta writer failed");
    }

    // flush info file
    KeyValueMap addInfo = _compressParam;
    addInfo["address_mapper_data_size"] = autil::StringUtil::toString(addrMapDataLen);
    RETURN_IF_FS_EXCEPTION(FlushInfoFile(addInfo, _encodeCompressAddressMapper), "FlushInfoFile failed");
    _compFileAddrMapper.reset();
    return FSEC_OK;
}

void CompressDataDumper::FlushCompressorData() noexcept(false)
{
    assert(_compressor);
    if (_compressor->GetBufferInLen() == 0) {
        // empty buffer
        return;
    }

    {
        ScopedCompressLatencyReporter reporter(_reporter, &_kmonTags);
        if (!_compressor->Compress()) {
            INDEXLIB_FATAL_ERROR(FileIO, "compress fail!");
            return;
        }
    }
    WriteCompressorData(_compressor, false);
}

void CompressDataDumper::WriteCompressorData(const std::shared_ptr<BufferCompressor>& compressor,
                                             bool useHint) noexcept(false)
{
    size_t writeLen = compressor->GetBufferOutLen();
    assert(_dataWriter);
    auto [ec, ret] = _dataWriter->Write(compressor->GetBufferOut(), writeLen);
    THROW_IF_FS_ERROR(ec, "Write failed");
    compressor->Reset();
    if (ret != writeLen) {
        INDEXLIB_FATAL_ERROR(FileIO, "write compress data fail!");
        return;
    }
    assert(_compFileAddrMapper);
    _compFileAddrMapper->AddOneBlock(writeLen, useHint);

    if (writeLen > _bufferSize) {
        _expandBlockCount++;
        _expandWasteSize += (writeLen - _bufferSize);
    }
}

void CompressDataDumper::FlushInfoFile(const KeyValueMap& additionalInfo, bool enableCompress) noexcept(false)
{
    AUTIL_LOG(INFO, "flush compress info file[%s].", _infoWriter->DebugString().c_str());

    assert(_infoWriter);
    CompressFileInfo infoFile;
    infoFile.compressorName = _compressor->GetCompressorName();
    infoFile.blockCount = _compFileAddrMapper->GetBlockCount();
    infoFile.maxCompressBlockSize = _compFileAddrMapper->GetMaxCompressBlockSize();
    infoFile.blockSize = _bufferSize;
    infoFile.deCompressFileLen = _writeLength;
    infoFile.compressFileLen = _compFileAddrMapper->GetCompressFileLength();
    infoFile.additionalInfo = additionalInfo;
    infoFile.enableCompressAddressMapper = enableCompress;

    if (_expandBlockCount > 0) {
        infoFile.additionalInfo["expand_block_count"] = autil::StringUtil::toString(_expandBlockCount);
        infoFile.additionalInfo["expand_block_wasted_size"] = autil::StringUtil::toString(_expandWasteSize);
    }

    if (_compressor) {
        infoFile.additionalInfo["lib_version"] = _compressor->GetLibVersionString();
    }
    if (_md5Stream) {
        infoFile.additionalInfo["decompress_file_md5"] = _md5Stream->GetMd5String();
    }
    string content = infoFile.ToString();
    _infoWriter->Write(content.c_str(), content.size()).GetOrThrow();
    _infoWriter->Close().GetOrThrow();
}

std::shared_ptr<BufferCompressor> CompressDataDumper::CreateCompressor(const string& compressorName, size_t bufferSize,
                                                                       const KeyValueMap& param) noexcept(false)
{
    std::shared_ptr<BufferCompressor> compressor;
    compressor.reset(BufferCompressorCreator::CreateBufferCompressor(compressorName, param));
    if (!compressor) {
        INDEXLIB_FATAL_ERROR(BadParameter, "create buffer compressor [%s] failed!", compressorName.c_str());
    }
    compressor->SetBufferInLen(bufferSize);
    compressor->SetBufferOutLen(bufferSize);
    return compressor;
}

}} // namespace indexlib::file_system
