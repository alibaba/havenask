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

#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "indexlib/file_system/file/CompressFileAddressMapperEncoder.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/file_system/file/ResourceFile.h"
#include "indexlib/util/Bitmap.h"

namespace indexlib::util {
class CompressHintData;
}

namespace indexlib { namespace file_system {
class IDirectory;
class CompressHintDataReader;
class CompressFileInfo;

class CompressFileAddressMapper
{
public:
    CompressFileAddressMapper();
    ~CompressFileAddressMapper();
    struct CompressMapperResource {
        CompressMapperResource()
            : mapperBaseAddress(nullptr)
            , hintDataReader(nullptr)
            , hintBitmapData(nullptr)
            , memoryUsed(0)
        {
        }
        ~CompressMapperResource();
        char* mapperBaseAddress;
        CompressHintDataReader* hintDataReader;
        uint32_t* hintBitmapData;
        size_t memoryUsed;
    };

public:
    void InitForRead(const std::shared_ptr<CompressFileInfo>& fileInfo, const std::shared_ptr<FileReader>& reader,
                     IDirectory* directory);

    FSResult<void> InitForWrite(size_t blockSize, size_t addressMapperBatchNum) noexcept;

    size_t Dump(const std::shared_ptr<FileWriter>& writer, bool& enableCompress) noexcept(false);

public:
    void AddOneBlock(size_t compBlockSize, bool useHint);

    size_t GetBlockSize() const noexcept { return (size_t)(1 << _powerOf2); }

    size_t GetBlockCount() const noexcept { return _blockCount; }

    size_t GetCompressFileLength() const
    {
        assert(_baseAddr);
        return _isCompressed ? _addrMapEncoder->GetOffset((char*)_baseAddr, _blockCount) : _baseAddr[_blockCount];
    }

    size_t OffsetToBlockIdx(int64_t offset) const noexcept { return offset >> _powerOf2; }
    size_t OffsetToInBlockOffset(int64_t offset) const noexcept { return offset & _powerMark; }

    util::CompressHintData* GetHintData(int64_t blockIdx) const noexcept;

    size_t CompressBlockAddress(int64_t blockIdx) const noexcept
    {
        assert((size_t)blockIdx <= _blockCount);
        if (_isCompressed) {
            assert(_addrMapEncoder);
            return _addrMapEncoder->GetOffset((char*)_baseAddr, blockIdx);
        }
        return _baseAddr[blockIdx] & OFFSET_MASK;
    }

    size_t CompressBlockLength(int64_t blockIdx) const noexcept
    {
        assert((size_t)blockIdx < _blockCount);
        return CompressBlockAddress(blockIdx + 1) - CompressBlockAddress(blockIdx);
    }

    size_t GetMaxCompressBlockSize() noexcept;
    size_t GetUseHintBlockCount() const noexcept { return _useHintBlockCount; }

    bool IsCompressBlockUseHintData(int64_t blockIdx) const noexcept
    {
        assert((size_t)blockIdx < _blockCount);
        if (_hintBitmap) {
            return _hintBitmap->Test(blockIdx);
        }
        return _isCompressed ? false : (_baseAddr[blockIdx] & HINT_MASK) > 0;
    }
    size_t EvaluateCurrentMemUsed();

public:
    static size_t EstimateMemUsed(const std::shared_ptr<IDirectory>& directory, const std::string& filePath,
                                  FSOpenType openType);

private:
    void InitMetaFromCompressInfo(const std::shared_ptr<CompressFileInfo>& fileInfo);

    void InitResourceFile(const std::shared_ptr<CompressFileInfo>& fileInfo, const std::shared_ptr<FileReader>& reader,
                          IDirectory* directory) noexcept(false);

    void InitAddressMapperData(const std::shared_ptr<CompressFileInfo>& fileInfo,
                               const std::shared_ptr<FileReader>& reader,
                               CompressMapperResource* resource) noexcept(false);

    void InitHintBitmap(const std::shared_ptr<CompressFileInfo>& fileInfo, const std::shared_ptr<FileReader>& reader,
                        CompressMapperResource* resource) noexcept(false);

    void InitHintDataReader(const std::shared_ptr<CompressFileInfo>& fileInfo,
                            const std::shared_ptr<FileReader>& reader, CompressMapperResource* resource);

    bool EncodeAddressMapperData(std::vector<char>& encodeData, bool enableCompress);

    size_t GetAddressMapperDataSize() const
    {
        return _isCompressed ? _addrMapEncoder->CalculateEncodeSize(_blockCount + 1)
                             : sizeof(size_t) * (_blockCount + 1);
    }

    bool NeedHintBitmap() const { return _isCompressed && _useHintBlockCount > 0; }

    ErrorCode InitBlockMask(size_t blockSize) noexcept;

    std::string GetResourceFileName(const std::shared_ptr<FileReader>& reader, const IDirectory* directory);

    bool LoadHintDataReader(const std::shared_ptr<CompressFileInfo>& compressInfo,
                            const std::shared_ptr<FileReader>& reader, size_t hintBeginOffset,
                            CompressMapperResource* resource);

    CompressHintDataReader* CreateHintDataReader(const std::shared_ptr<CompressFileInfo>& compressInfo,
                                                 const std::shared_ptr<FileReader>& fileReader, size_t beginOffset);

    static std::string GetResourceFileSuffix(FSFileType type);

    static constexpr size_t OFFSET_MASK = 0x7FFFFFFFFFFFFFFF;
    static constexpr size_t HINT_MASK = 0x8000000000000000;

private:
    size_t* _baseAddr;
    uint32_t _powerOf2;
    uint32_t _powerMark;
    size_t _blockCount;
    std::unique_ptr<CompressFileAddressMapperEncoder> _addrMapEncoder;
    std::vector<size_t> _blockOffsetVec;
    size_t _maxCompressBlockSize;
    size_t _useHintBlockCount;
    bool _isCompressed;
    util::BitmapPtr _hintBitmap;
    bool _needLoadResource;
    ResourceFilePtr _resourceFile;
    std::shared_ptr<void> _resourceHandle;

private:
    friend class CompressFileAddressMapperTest;
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<CompressFileAddressMapper> CompressFileAddressMapperPtr;
}} // namespace indexlib::file_system
