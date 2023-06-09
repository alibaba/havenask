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
#include <memory>
#include <stddef.h>
#include <string>
#include <utility>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/PoolBase.h"
#include "future_lite/CoroInterface.h"
#include "future_lite/Future.h"
#include "future_lite/Unit.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/file/CompressFileAddressMapper.h"
#include "indexlib/file_system/file/CompressFileInfo.h"
#include "indexlib/file_system/file/DecompressMetricsReporter.h"
#include "indexlib/file_system/file/FileNode.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/ReadOption.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/PoolUtil.h"
#include "indexlib/util/byte_slice_list/ByteSliceList.h"
#include "indexlib/util/metrics/Monitor.h"

namespace indexlib { namespace util {
class BufferCompressor;
}} // namespace indexlib::util

namespace indexlib { namespace file_system {
class IDirectory;
class BlockFileAccessor;

class CompressFileReader : public FileReader
{
public:
    CompressFileReader(autil::mem_pool::Pool* pool = NULL) noexcept
        : _pool(pool)
        , _blockAccessor(nullptr)
        , _invalidBlockId(0)
        , _batchSize(DEFAULT_BATCH_SIZE)
        , _prefetchBuf(nullptr)
    {
    }

    virtual ~CompressFileReader() noexcept;

public:
    virtual bool Init(const std::shared_ptr<FileReader>& fileReader, const std::shared_ptr<FileReader>& metaReader,
                      const std::shared_ptr<CompressFileInfo>& compressInfo, IDirectory* directory) noexcept(false);
    virtual CompressFileReader* CreateSessionReader(autil::mem_pool::Pool* pool) noexcept = 0;
    virtual std::string GetLoadTypeString() const noexcept = 0;

public:
    FSResult<void> Open() noexcept override { return FSEC_OK; }
    FSResult<size_t> Read(void* buffer, size_t length, size_t offset, ReadOption option) noexcept override;
    FSResult<size_t> Read(void* buffer, size_t length, ReadOption option) noexcept override;
    util::ByteSliceList* ReadToByteSliceList(size_t length, size_t offset, ReadOption option) noexcept override;
    future_lite::Future<size_t> ReadAsync(void* buffer, size_t length, size_t offset,
                                          ReadOption option) noexcept(false) override;
    future_lite::Future<uint32_t> ReadUInt32Async(size_t offset, ReadOption option) noexcept(false) override;
    future_lite::Future<uint32_t> ReadVUInt32Async(size_t offset, ReadOption option) noexcept(false) override;
    future_lite::coro::Lazy<std::vector<FSResult<size_t>>> BatchReadOrdered(const BatchIO& batchIO,
                                                                            ReadOption option) noexcept override;
    void* GetBaseAddress() const noexcept override { return NULL; }
    size_t GetLength() const noexcept override
    {
        assert(_dataFileReader);
        return _dataFileReader->GetLength();
    }
    const std::string& GetLogicalPath() const noexcept override
    {
        assert(_dataFileReader);
        return _dataFileReader->GetLogicalPath();
    }
    const std::string& GetPhysicalPath() const noexcept override
    {
        assert(_dataFileReader);
        return _dataFileReader->GetPhysicalPath();
    }
    FSResult<void> Close() noexcept override
    {
        if (_dataFileReader) {
            auto ret = _dataFileReader->Close();
            _dataFileReader.reset();
            return ret;
        }
        return FSEC_OK;
    }
    FSOpenType GetOpenType() const noexcept override
    {
        if (_dataFileReader) {
            return _dataFileReader->GetOpenType();
        }
        return FSOT_UNKNOWN;
    }
    FSFileType GetType() const noexcept override
    {
        if (_dataFileReader) {
            return _dataFileReader->GetType();
        }
        return FSFT_UNKNOWN;
    }
    std::shared_ptr<FileNode> GetFileNode() const noexcept override
    {
        assert(false);
        return std::shared_ptr<FileNode>();
    }

    bool CheckPrefetchHit(size_t offset, std::function<void(size_t, uint8_t*, uint32_t)> onHitCallback) noexcept(false);

public:
    FL_LAZY(FSResult<size_t>)
    ReadAsyncCoro(void* buffer, size_t length, size_t offset, ReadOption option) noexcept override;

public:
    size_t GetUncompressedFileLength() const noexcept { return _compressInfo->deCompressFileLen; }
    std::shared_ptr<FileReader> GetDataFileReader() const noexcept { return _dataFileReader; }
    const std::shared_ptr<CompressFileInfo>& GetCompressInfo() const noexcept { return _compressInfo; }
    size_t GetMaxUnCompressBlockSize() const noexcept { return _compressInfo->blockSize; }
    size_t GetMaxCompressedBlockSize() const noexcept { return _compressAddrMapper->GetMaxCompressBlockSize(); }

    static util::BufferCompressor* CreateCompressor(autil::mem_pool::Pool* pool,
                                                    const std::shared_ptr<CompressFileInfo>& compressInfo,
                                                    size_t maxCompressedBlockSize) noexcept;

    void InitDecompressMetricReporter(FileSystemMetricsReporter* reporter) noexcept;
    const std::shared_ptr<CompressFileAddressMapper>& GetCompressFileAddressMapper() const
    {
        return _compressAddrMapper;
    }

protected:
    bool DoInit(const std::shared_ptr<FileReader>& fileReader, const std::shared_ptr<FileReader>& metaReader,
                const std::shared_ptr<CompressFileAddressMapper>& compressAddressMapper,
                const std::shared_ptr<CompressFileInfo>& compressInfo) noexcept;

    bool needHintData(const CompressFileInfo& info) const noexcept(false);

    std::string GetCompressFileName(const std::shared_ptr<FileReader>& reader,
                                    const IDirectory* directory) noexcept(false);

    std::shared_ptr<CompressFileAddressMapper> LoadCompressAddressMapper(
        const std::shared_ptr<FileReader>& fileReader, const std::shared_ptr<FileReader>& metaReader,
        const std::shared_ptr<CompressFileInfo>& compressInfo, IDirectory* directory) noexcept(false);

    bool InCurrentBlock(size_t offset) const noexcept
    {
        assert(offset < GetUncompressedFileLength());
        assert(_curBlockIdxs.size() <= 1);
        return _curBlockIdxs.size() == 1 && _compressAddrMapper->OffsetToBlockIdx(offset) == _curBlockIdxs[0];
    }

    void ResetCompressorBuffer(size_t idx) noexcept;
    void AppendCompressorBuffer(size_t count) noexcept;

protected:
    virtual void LoadBuffer(size_t offset, ReadOption option) noexcept(false) = 0;
    virtual void LoadBufferFromMemory(size_t offset, uint8_t* buffer, uint32_t bufLen,
                                      bool enableTrace) noexcept(false) = 0;
    // blockInfo: pair<blockIdx, BufferCompressor*>
    virtual future_lite::coro::Lazy<std::vector<ErrorCode>>
    BatchLoadBuffer(const std::vector<std::pair<size_t, util::BufferCompressor*>>& blockInfo,
                    ReadOption option) noexcept(false) = 0;

    virtual FL_LAZY(ErrorCode) LoadBufferAsyncCoro(size_t offset, ReadOption option) noexcept
    {
        try {
            LoadBuffer(offset, option);
        } catch (const std::exception& e) {
            AUTIL_LOG(ERROR, "LoadBuffer exception[%s]", e.what());
            FL_CORETURN FSEC_ERROR;
        } catch (...) {
            AUTIL_LOG(ERROR, "LoadBuffer");
            FL_CORETURN FSEC_ERROR;
        }
        FL_CORETURN FSEC_OK;
    }

    size_t GetLogicLength() const noexcept override { return GetUncompressedFileLength(); }

    std::pair<size_t, size_t> GetBlockRange(size_t offset, size_t length) noexcept(false);

private:
    FSResult<void> PrefetchData(size_t length, size_t offset, ReadOption option) noexcept;
    future_lite::Future<future_lite::Unit> PrefetchDataAsync(size_t length, size_t offset,
                                                             ReadOption option) noexcept(false);
    FL_LAZY(FSResult<void>)
    PrefetchDataAsyncCoro(size_t length, size_t offset, ReadOption option) noexcept;

    // unused function
    // FL_LAZY(std::pair<void*, size_t>)
    // ReadAsyncValue(size_t valueLen, size_t curInBlockOffset, size_t valueOffset, ReadOption& option,
    //                autil::mem_pool::Pool* pool) noexcept(false);
    size_t FillFromOneCompressor(size_t inBlockOffset, size_t length, util::BufferCompressor* compressor,
                                 void* buffer) const noexcept;

protected:
    static size_t DEFAULT_BATCH_SIZE;
    std::shared_ptr<CompressFileAddressMapper> _compressAddrMapper;
    std::vector<util::BufferCompressor*> _compressors;
    std::vector<size_t> _curBlockIdxs;
    std::unordered_map<size_t, size_t> _blockIdxToIndex;
    std::shared_ptr<CompressFileInfo> _compressInfo;
    std::shared_ptr<FileReader> _dataFileReader;
    std::shared_ptr<FileReader> _metaFileReader;
    // _pool may be a recyclePool
    autil::mem_pool::Pool* _pool;
    DecompressMetricsReporter _decompressMetricReporter;
    BlockFileAccessor* _blockAccessor;
    size_t _invalidBlockId;
    size_t _batchSize;
    uint8_t* _prefetchBuf;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<CompressFileReader> CompressFileReaderPtr;

class CompressFileReaderGuard
{
public:
    CompressFileReaderGuard(CompressFileReader* reader, autil::mem_pool::Pool* pool) noexcept
        : _reader(reader)
        , _pool(pool)
    {
        assert(_reader);
    }

    ~CompressFileReaderGuard() noexcept { IE_POOL_COMPATIBLE_DELETE_CLASS(_pool, _reader); }

private:
    CompressFileReader* _reader;
    autil::mem_pool::Pool* _pool;
};

class TemporarySessionCompressFileReader
{
public:
    TemporarySessionCompressFileReader(CompressFileReader* reader) noexcept : _reader(nullptr), _pool(nullptr)
    {
        _pool = new autil::mem_pool::Pool(GetPoolChunkSize(reader));
        _reader = reader->CreateSessionReader(_pool);
    }

    ~TemporarySessionCompressFileReader() noexcept
    {
        IE_POOL_COMPATIBLE_DELETE_CLASS(_pool, _reader);
        DELETE_AND_SET_NULL(_pool);
    }

    TemporarySessionCompressFileReader(TemporarySessionCompressFileReader&& other) noexcept
        : _reader(other._reader)
        , _pool(other._pool)
    {
        other._reader = nullptr;
        other._pool = nullptr;
    }

    TemporarySessionCompressFileReader& operator=(TemporarySessionCompressFileReader&& other) noexcept
    {
        std::swap(_reader, other._reader);
        std::swap(_pool, other._pool);
        return *this;
    }

    CompressFileReader* Get() const noexcept { return _reader; }

protected:
    // default align to 4K
    inline size_t GetPoolChunkSize(CompressFileReader* reader) noexcept
    {
        assert(reader);
        size_t totalBufferSize = reader->GetMaxUnCompressBlockSize() + reader->GetMaxCompressedBlockSize();
        size_t blockCount = (totalBufferSize * 2 + 4095) / 4096 + 1;
        return blockCount * 4096;
    }

private:
    CompressFileReader* _reader;
    autil::mem_pool::Pool* _pool;

private:
    friend class CompressBlockDataRetrieverTest;
};
}} // namespace indexlib::file_system
