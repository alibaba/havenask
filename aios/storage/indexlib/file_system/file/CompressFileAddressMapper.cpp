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
#include "indexlib/file_system/file/CompressFileAddressMapper.h"

#include <algorithm>
#include <cstddef>
#include <memory>

#include "alog/Logger.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/ReaderOption.h"
#include "indexlib/file_system/WriterOption.h"
#include "indexlib/file_system/file/CompressFileInfo.h"
#include "indexlib/file_system/file/CompressFileReader.h"
#include "indexlib/file_system/file/CompressHintDataReader.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/buffer_compressor/BufferCompressor.h"
#include "indexlib/util/cache/BlockAccessCounter.h"

using namespace std;
using namespace indexlib::file_system;
using namespace indexlib::util;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, CompressFileAddressMapper);

CompressFileAddressMapper::CompressFileAddressMapper()
    : _baseAddr(NULL)
    , _powerOf2(0)
    , _powerMark(0)
    , _blockCount(0)
    , _maxCompressBlockSize(0)
    , _useHintBlockCount(0)
    , _isCompressed(false)
    , _needLoadResource(false)
{
}

CompressFileAddressMapper::~CompressFileAddressMapper() {}

CompressFileAddressMapper::CompressMapperResource::~CompressMapperResource()
{
    ARRAY_DELETE_AND_SET_NULL(mapperBaseAddress);
    ARRAY_DELETE_AND_SET_NULL(hintBitmapData);
    DELETE_AND_SET_NULL(hintDataReader);
}

void CompressFileAddressMapper::InitForRead(const std::shared_ptr<CompressFileInfo>& fileInfo,
                                            const std::shared_ptr<FileReader>& reader, IDirectory* directory)
{
    // init compress info meta
    InitMetaFromCompressInfo(fileInfo);

    InitResourceFile(fileInfo, reader, directory);
    std::unique_ptr<CompressMapperResource> resource;
    if (_needLoadResource) {
        resource.reset(new CompressMapperResource());
    } else if (_resourceFile != nullptr) {
        _resourceHandle = _resourceFile->GetResourceHandle();
    }

    // compressedFileData + addressMapperData + [hintBitmapData] + [hintData]
    InitAddressMapperData(fileInfo, reader, resource.get());
    InitHintBitmap(fileInfo, reader, resource.get());
    InitHintDataReader(fileInfo, reader, resource.get());

    if (resource) {
        assert(_resourceFile);
        _resourceFile->UpdateMemoryUse(resource->memoryUsed);
        _resourceFile->Reset(resource.release());
        _resourceHandle = _resourceFile->GetResourceHandle();
    }
}

FSResult<void> CompressFileAddressMapper::InitForWrite(size_t blockSize, size_t addressMapperBatchNum) noexcept
{
    RETURN_IF_FS_ERROR(InitBlockMask(blockSize), "InitBlockMask failed");

    _addrMapEncoder.reset(new CompressFileAddressMapperEncoder(addressMapperBatchNum));
    assert(_blockOffsetVec.empty());
    _blockOffsetVec.push_back(0);
    _baseAddr = (size_t*)_blockOffsetVec.data();
    _blockCount = 0;
    _isCompressed = false;
    return FSEC_OK;
}

void CompressFileAddressMapper::AddOneBlock(size_t compBlockSize, bool useHint)
{
    if (_blockOffsetVec.empty()) {
        INDEXLIB_FATAL_ERROR(UnSupported, "not support add block info!");
    }

    size_t lastOffset = *_blockOffsetVec.rbegin();
    if (useHint) {
        size_t hintValue = lastOffset | HINT_MASK;
        *_blockOffsetVec.rbegin() = hintValue;
    }
    _blockOffsetVec.push_back(lastOffset + compBlockSize);
    assert(!_isCompressed);
    _baseAddr = (size_t*)_blockOffsetVec.data();
    _maxCompressBlockSize = max(_maxCompressBlockSize, compBlockSize);
    ++_blockCount;
    if (useHint) {
        ++_useHintBlockCount;
    }
}

ErrorCode CompressFileAddressMapper::InitBlockMask(size_t blockSize) noexcept
{
    size_t alignedSize = 1;
    _powerOf2 = 0;
    while (alignedSize < blockSize) {
        alignedSize <<= 1;
        _powerOf2++;
    }
    _powerMark = alignedSize - 1;
    if (alignedSize != blockSize) {
        AUTIL_LOG(ERROR, "blockSize [%lu] must be 2^n!", blockSize);
        return FSEC_BADARGS;
    }
    return FSEC_OK;
}

size_t CompressFileAddressMapper::Dump(const std::shared_ptr<FileWriter>& writer, bool& enableCompress) noexcept(false)
{
    assert(writer);
    size_t beginLen = writer->GetLength();
    vector<char> encodeData;
    enableCompress = EncodeAddressMapperData(encodeData, enableCompress);
    if (!enableCompress) {
        writer->Write(_baseAddr, GetAddressMapperDataSize()).GetOrThrow();
        return writer->GetLength() - beginLen;
    }

    writer->Write(encodeData.data(), encodeData.size()).GetOrThrow();
    if (_useHintBlockCount > 0) {
        // encode use hint info by bitmap
        util::Bitmap bitmap((uint32_t)_blockCount);
        for (uint32_t i = 0; i < _blockCount; i++) {
            if (IsCompressBlockUseHintData(i)) {
                bitmap.Set(i);
            }
        }
        writer->Write(bitmap.GetData(), bitmap.GetSlotCount() * sizeof(uint32_t)).GetOrThrow();
    }
    return writer->GetLength() - beginLen;
}

size_t CompressFileAddressMapper::GetMaxCompressBlockSize() noexcept
{
    if (unlikely(_maxCompressBlockSize == 0)) {
        size_t maxCompressBlockSize = 0;
        for (size_t i = 0; i < _blockCount; i++) {
            maxCompressBlockSize = max(maxCompressBlockSize, CompressBlockLength(i));
        }
        _maxCompressBlockSize = maxCompressBlockSize;
    }
    return _maxCompressBlockSize;
}

string CompressFileAddressMapper::GetResourceFileName(const std::shared_ptr<FileReader>& reader,
                                                      const IDirectory* directory)
{
    string compressFileRelativePath;
    if (!PathUtil::GetRelativePath(directory->GetLogicalPath(), reader->GetLogicalPath(), compressFileRelativePath)) {
        INDEXLIB_FATAL_ERROR(InconsistentState,
                             "make compressFileRelativePath failed,"
                             " directory path [%s], file path [%s] ",
                             directory->DebugString().c_str(), reader->DebugString().c_str());
    }

    auto pos = compressFileRelativePath.find(COMPRESS_FILE_META_SUFFIX);
    if (pos != string::npos) {
        compressFileRelativePath = compressFileRelativePath.substr(0, pos);
    }

    // avoid to reuse resource file when same file open twice with different type (1st: mmap, 2nd: cache)
    // otherwise that will cause cached type reader reuse resource which has NULL _baseAddr in resource file
    string fileTypeSuffix = GetResourceFileSuffix(reader->GetType());
    return compressFileRelativePath + "." + fileTypeSuffix;
}

size_t CompressFileAddressMapper::EstimateMemUsed(const std::shared_ptr<IDirectory>& directory, const string& filePath,
                                                  FSOpenType openType)
{
    auto compressInfo = directory->GetCompressFileInfo(filePath).GetOrThrow(filePath);
    if (!compressInfo) {
        return 0;
    }

    string targetFilePath = filePath;
    bool useMetaFile = indexlib::util::GetTypeValueFromKeyValueMap(
        compressInfo->additionalInfo, COMPRESS_ENABLE_META_FILE, DEFAULT_COMPRESS_ENABLE_META_FILE);
    if (useMetaFile) {
        targetFilePath = filePath + COMPRESS_FILE_META_SUFFIX;
    }

    auto deduceFileType = directory->DeduceFileType(targetFilePath, openType).GetOrThrow();
    string fileTypeSuffix = GetResourceFileSuffix(deduceFileType);
    string resourceFilePath = filePath + "." + fileTypeSuffix;
    auto resourceFile = directory->GetResourceFile(resourceFilePath).GetOrThrow(resourceFilePath);
    if (resourceFile && !resourceFile->Empty()) {
        return 0;
    }

    size_t hintDataSize = GetTypeValueFromKeyValueMap(compressInfo->additionalInfo, "hint_data_size", (size_t)0);
    if (deduceFileType == FSFT_BLOCK) {
        size_t addressMapperSize =
            GetTypeValueFromKeyValueMap(compressInfo->additionalInfo, "address_mapper_data_size", (size_t)0);
        return hintDataSize + addressMapperSize;
    }
    // target file is integrated, address mapper will use baseAddress
    bool disableHintDataRef = CompressHintDataReader::DisableUseHintDataRef();
    if (useMetaFile) {
        size_t memoryUsage = directory->EstimateFileMemoryUse(targetFilePath, openType).GetOrThrow();
        return disableHintDataRef ? (memoryUsage + hintDataSize) : memoryUsage;
    }
    return disableHintDataRef ? hintDataSize : 0;
}

util::CompressHintData* CompressFileAddressMapper::GetHintData(int64_t blockIdx) const noexcept
{
    if (!IsCompressBlockUseHintData(blockIdx)) {
        return nullptr;
    }

    util::CompressHintData* hintData = nullptr;
    if (!_resourceFile || _resourceFile->Empty()) {
        AUTIL_LOG(ERROR, "block [id:%lu] has hint data but not valid hint data reader.", blockIdx);
        return nullptr;
    }
    CompressHintDataReader* reader = static_cast<CompressMapperResource*>(_resourceHandle.get())->hintDataReader;
    assert(reader);
    hintData = reader->GetHintData(blockIdx);
    if (!hintData) {
        AUTIL_LOG(ERROR, "block [id:%lu] has hint data but hint data is nullptr.", blockIdx);
        return nullptr;
    }
    return hintData;
}

size_t CompressFileAddressMapper::EvaluateCurrentMemUsed()
{
    return _needLoadResource ? _resourceFile->GetMemoryUse() : 0;
}

bool CompressFileAddressMapper::LoadHintDataReader(const std::shared_ptr<CompressFileInfo>& compressInfo,
                                                   const std::shared_ptr<FileReader>& reader, size_t hintBeginOffset,
                                                   CompressMapperResource* resource)
{
    CompressHintDataReader* hintDataReader = CreateHintDataReader(compressInfo, reader, hintBeginOffset);
    if (!hintDataReader) {
        return false;
    }
    resource->hintDataReader = hintDataReader;
    return true;
}

CompressHintDataReader*
CompressFileAddressMapper::CreateHintDataReader(const std::shared_ptr<CompressFileInfo>& compressInfo,
                                                const std::shared_ptr<FileReader>& fileReader, size_t beginOffset)
{
    AUTIL_LOG(INFO, "Begin loading hint data from file [%s]", fileReader->GetLogicalPath().c_str());
    std::unique_ptr<CompressHintDataReader> reader(new CompressHintDataReader());
    util::BufferCompressor* compressor =
        CompressFileReader::CreateCompressor(nullptr, compressInfo, compressInfo->blockSize);
    assert(compressor);
    if (!reader->Load(compressor, fileReader, beginOffset)) {
        POOL_COMPATIBLE_DELETE_CLASS(nullptr, compressor);
        return nullptr;
    }

    POOL_COMPATIBLE_DELETE_CLASS(nullptr, compressor);
    AUTIL_LOG(INFO, "Finish loading hint data from file [%s]", fileReader->GetLogicalPath().c_str());
    return reader.release();
}

void CompressFileAddressMapper::InitMetaFromCompressInfo(const std::shared_ptr<CompressFileInfo>& fileInfo)
{
    THROW_IF_FS_ERROR(InitBlockMask(fileInfo->blockSize), "InitBlockMask failed");
    _blockCount = fileInfo->blockCount;
    _isCompressed = fileInfo->enableCompressAddressMapper;
    auto iter = fileInfo->additionalInfo.find("use_hint_block_count");
    _useHintBlockCount =
        (iter == fileInfo->additionalInfo.end()) ? 0 : autil::StringUtil::fromString<size_t>(iter->second);

    iter = fileInfo->additionalInfo.find("address_mapper_batch_number");
    size_t addrMapperBatchNum =
        (iter == fileInfo->additionalInfo.end()) ? 0 : autil::StringUtil::fromString<size_t>(iter->second);
    _addrMapEncoder.reset(new CompressFileAddressMapperEncoder(addrMapperBatchNum));
}

void CompressFileAddressMapper::InitResourceFile(const std::shared_ptr<CompressFileInfo>& fileInfo,
                                                 const std::shared_ptr<FileReader>& reader,
                                                 IDirectory* directory) noexcept(false)
{
    bool needHintData =
        GetTypeValueFromKeyValueMap(fileInfo->additionalInfo, COMPRESS_ENABLE_HINT_PARAM_NAME, (bool)false);
    if (needHintData || reader->GetBaseAddress() == nullptr) {
        // needResourceFile
        string resourceFilePath = GetResourceFileName(reader, directory);
        _resourceFile = directory->GetResourceFile(resourceFilePath).GetOrThrow(resourceFilePath);
        if (!_resourceFile || _resourceFile->Empty()) {
            _resourceFile = directory->CreateResourceFile(resourceFilePath).GetOrThrow(resourceFilePath);
            _needLoadResource = true;
            return;
        }
    }
    _needLoadResource = false;
}

void CompressFileAddressMapper::InitAddressMapperData(const std::shared_ptr<CompressFileInfo>& fileInfo,
                                                      const std::shared_ptr<FileReader>& reader,
                                                      CompressMapperResource* resource) noexcept(false)
{
    bool useMetaFile = GetTypeValueFromKeyValueMap(fileInfo->additionalInfo, COMPRESS_ENABLE_META_FILE,
                                                   DEFAULT_COMPRESS_ENABLE_META_FILE);

    size_t beginOffset = useMetaFile ? 0 : fileInfo->compressFileLen;
    size_t mapperDataLength = GetAddressMapperDataSize();
    char* baseAddr = (char*)reader->GetBaseAddress();
    if (baseAddr != nullptr) {
        _baseAddr = (size_t*)(baseAddr + beginOffset);
    } else if (resource == nullptr) { // reuse resource file
        _baseAddr = (size_t*)(static_cast<CompressMapperResource*>(_resourceHandle.get())->mapperBaseAddress);
    } else {
        resource->mapperBaseAddress = new char[mapperDataLength];
        resource->memoryUsed += mapperDataLength;
        if (mapperDataLength != reader->Read(resource->mapperBaseAddress, mapperDataLength, beginOffset).GetOrThrow()) {
            INDEXLIB_FATAL_ERROR(FileIO, "read address mapper data failed from file[%s]",
                                 reader->DebugString().c_str());
        }
        _baseAddr = (size_t*)resource->mapperBaseAddress;
    }

    assert(fileInfo->maxCompressBlockSize == 0 || fileInfo->maxCompressBlockSize == GetMaxCompressBlockSize());
    _maxCompressBlockSize =
        (fileInfo->maxCompressBlockSize != 0) ? fileInfo->maxCompressBlockSize : GetMaxCompressBlockSize();
}

void CompressFileAddressMapper::InitHintBitmap(const std::shared_ptr<CompressFileInfo>& fileInfo,
                                               const std::shared_ptr<FileReader>& reader,
                                               CompressMapperResource* resource) noexcept(false)
{
    if (!NeedHintBitmap()) {
        return;
    }

    size_t mapperDataLength = GetAddressMapperDataSize();
    size_t bitmapSlotNum = util::Bitmap::GetSlotCount(_blockCount);
    size_t bitmapDataLength = bitmapSlotNum * sizeof(uint32_t);

    bool useMetaFile = GetTypeValueFromKeyValueMap(fileInfo->additionalInfo, COMPRESS_ENABLE_META_FILE,
                                                   DEFAULT_COMPRESS_ENABLE_META_FILE);
    size_t beginOffset = useMetaFile ? mapperDataLength : (mapperDataLength + fileInfo->compressFileLen);

    char* baseAddr = (char*)reader->GetBaseAddress();
    uint32_t* bitmapAddr = nullptr;
    if (baseAddr != nullptr) {
        bitmapAddr = (uint32_t*)(baseAddr + beginOffset);
    } else if (resource == nullptr) { // reuse resource file
        bitmapAddr = static_cast<CompressMapperResource*>(_resourceHandle.get())->hintBitmapData;
    } else {
        resource->hintBitmapData = new uint32_t[bitmapSlotNum];
        resource->memoryUsed += bitmapDataLength;
        if (bitmapDataLength != reader->Read(resource->hintBitmapData, bitmapDataLength, beginOffset).GetOrThrow()) {
            INDEXLIB_FATAL_ERROR(FileIO, "read hint bitmap data failed from file[%s]", reader->DebugString().c_str());
        }
        bitmapAddr = resource->hintBitmapData;
    }
    _hintBitmap.reset(new util::Bitmap);
    _hintBitmap->MountWithoutRefreshSetCount(_blockCount, bitmapAddr);
}

void CompressFileAddressMapper::InitHintDataReader(const std::shared_ptr<CompressFileInfo>& fileInfo,
                                                   const std::shared_ptr<FileReader>& reader,
                                                   CompressMapperResource* resource)
{
    size_t mapperDataLength = GetAddressMapperDataSize();
    bool needHintData =
        GetTypeValueFromKeyValueMap(fileInfo->additionalInfo, COMPRESS_ENABLE_HINT_PARAM_NAME, (bool)false);
    bool useMetaFile = GetTypeValueFromKeyValueMap(fileInfo->additionalInfo, COMPRESS_ENABLE_META_FILE,
                                                   DEFAULT_COMPRESS_ENABLE_META_FILE);
    size_t compressDataLen = useMetaFile ? 0 : fileInfo->compressFileLen;
    if (!needHintData) {
        if (!NeedHintBitmap() && reader->GetLength() != (compressDataLen + mapperDataLength)) {
            INDEXLIB_FATAL_ERROR(IndexCollapsed,
                                 "compress file [%s] is collapsed,"
                                 " compressLen [%lu], mapperLen [%lu], fileLen [%lu]!",
                                 reader->DebugString().c_str(), compressDataLen, mapperDataLength, reader->GetLength());
        }
        return;
    }

    if (!resource) { // no need load resource
        assert(_resourceFile != nullptr);
        return;
    }

    size_t bitmapDataLength = NeedHintBitmap() ? util::Bitmap::GetSlotCount(_blockCount) * sizeof(uint32_t) : 0;
    size_t hintBeginOffset = compressDataLen + mapperDataLength + bitmapDataLength;
    if (!LoadHintDataReader(fileInfo, reader, hintBeginOffset, resource)) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed,
                             "compress file [%s] is collapsed when load hint data reader,"
                             " compressLen [%lu], mapperLen [%lu], fileLen [%lu]!",
                             reader->DebugString().c_str(), compressDataLen, mapperDataLength, reader->GetLength());
    }
    resource->memoryUsed += resource->hintDataReader->GetMemoryUse();
}

bool CompressFileAddressMapper::EncodeAddressMapperData(vector<char>& encodeData, bool enableCompress)
{
    if (!enableCompress) {
        return false;
    }

    encodeData.resize(_addrMapEncoder->CalculateEncodeSize(_blockOffsetVec.size()));
    if (_useHintBlockCount == 0) { // no hint data, encode directly
        return _addrMapEncoder->Encode((size_t*)_blockOffsetVec.data(), _blockOffsetVec.size(),
                                       (char*)encodeData.data(), encodeData.size());
    }

    // has hint data, offset with hint mask
    vector<size_t> orginOffsetVec;
    orginOffsetVec.reserve(_blockOffsetVec.size());
    for (size_t i = 0; i < _blockOffsetVec.size(); i++) {
        orginOffsetVec.push_back(CompressBlockAddress(i));
    }
    return _addrMapEncoder->Encode((size_t*)orginOffsetVec.data(), orginOffsetVec.size(), (char*)encodeData.data(),
                                   encodeData.size());
}

string CompressFileAddressMapper::GetResourceFileSuffix(FSFileType type)
{
    return string("compress_resource_") + GetFileNodeTypeStr(type);
}

}} // namespace indexlib::file_system
