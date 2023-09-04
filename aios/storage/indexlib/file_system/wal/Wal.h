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

#include <map>
#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/File.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/wal/Wal.pb.h"
#include "indexlib/util/buffer_compressor/BufferCompressor.h"

namespace indexlib { namespace file_system {

class WAL
{
public:
    enum class CompressType {
        CompressionKind_NONE,
        CompressionKind_ZLIB,
        CompressionKind_ZLIB_DEFAULT,
        CompressionKind_SNAPPY,
        CompressionKind_LZ4,
        CompressionKind_LZ4_HC,
        CompressionKind_ZSTD,
    };

    struct WALOption {
        std::string workDir;
        int64_t recoverPos = DEFAULT_RECOVER_POSITION;
        CompressType compressType = CompressType::CompressionKind_NONE;
        bool isReadDirectIO = false;
        bool isWriteDirectIO = false;
        bool isCheckSum = true;

        WALOption(const std::string& workDir_ = "") : workDir(workDir_) {}
        WALOption& operator=(const WALOption& opt)
        {
            if (this != &opt) {
                workDir = opt.workDir;
                recoverPos = opt.recoverPos;
                compressType = opt.compressType;
                isReadDirectIO = opt.isReadDirectIO;
                isWriteDirectIO = opt.isWriteDirectIO;
                isCheckSum = opt.isCheckSum;
            }
            return *this;
        }
    };

    static bool IsValidWALDirName(const std::string& fileName);

    explicit WAL(const WALOption& opt) : _walOpt(opt), _continuousAppendFails(0) {}
    ~WAL() = default;

    WAL(const WAL&) = delete;
    WAL& operator=(const WAL&) = delete;

    bool Init();
    bool AppendRecord(const std::string& record);
    bool ReadRecord(std::string& record);
    bool IsRecovered() const { return _isRecovered; };

    /*
        close and open new one
        if removeOldFile is true, will remove the old file
    */
    bool ReOpen(bool removeOldFile = false);
    bool SeekRecoverPosition(int64_t recoverPos);

    // WARN!!, will erase all wal data in workDir
    bool Recycle();

public:
    class WALOperator
    {
    public:
        WALOperator() = default;
        virtual ~WALOperator() = default;

        static bool Init();
        static const indexlib::util::BufferCompressorPtr GetCompressor(const WAL::CompressType& type);

        static bool ToPBType(const WAL::CompressType& src, ::indexlib::proto::CompressType& dst);
        static bool ToWALType(const ::indexlib::proto::CompressType& src, WAL::CompressType& type);

    private:
        static std::map<WAL::CompressType, indexlib::util::BufferCompressorPtr> _compressors;

    private:
        AUTIL_LOG_DECLARE();
    };

    class Writer : public WALOperator
    {
    public:
        explicit Writer(const std::shared_ptr<fslib::fs::File> file, uint64_t beginOffset,
                        const WAL::CompressType& type)
            : _file(file)
            , _beginOffset(beginOffset)
            , _compressType(type)
        {
            _buffer.reset(new std::vector<char>(DEFAULT_DATA_BUF_SIZE));
        }
        ~Writer();

        Writer(const Writer&) = delete;
        Writer& operator=(const Writer&) = delete;

        bool AppendRecord(const std::string& record);

        size_t LastOffset() const { return _offset; }

        size_t LastNextFileOffset() const { return _offset + _beginOffset; }

        const char* GetWriterFileName() const { return _file->getFileName(); }

    private:
        uint32_t GetDataCRC(const std::string& record);
        bool Compress(const std::string& record, std::string& compressData);

    private:
        static constexpr uint32_t DEFAULT_DATA_BUF_SIZE = 1024 * 1024; // 1M
        std::unique_ptr<std::vector<char>> _buffer;
        std::shared_ptr<fslib::fs::File> _file;
        const uint64_t _beginOffset;
        const WAL::CompressType _compressType;
        size_t _offset = 0;

    private:
        AUTIL_LOG_DECLARE();
    };

    class Reader : public WALOperator
    {
    public:
        explicit Reader(const std::shared_ptr<fslib::fs::File> file, size_t fileLength, bool checksum,
                        size_t skipOffset, uint32_t beginOffset)
            : _file(file)
            , _fileLength(fileLength)
            , _skipOffset(skipOffset)
            , _headerLen(sizeof(uint32_t))
            , _beginOffset(beginOffset)
            , _isCheckSum(checksum)
        {
            _buffer.reset(new std::vector<char>(DEFAULT_DATA_BUF_SIZE));
        }

        ~Reader() { _file->close(); }

        Reader(const Reader&) = delete;
        Reader& operator=(const Reader&) = delete;

        bool ReadRecord(std::string& record);
        size_t BeginOffset() const { return _beginOffset; }
        size_t LastRecordOffset() const { return _offsetInFile + _beginOffset; }

        bool IsEof() const { return _eof; }

    private:
        bool DeCompress(const WAL::CompressType& type, const char* rawData, uint32_t len, std::string& record) const;

        bool RotateNextBlock(size_t& leftLen, int64_t requiredBytes);

    private:
        static constexpr uint32_t DEFAULT_DATA_BUF_SIZE = 1024 * 1024; // 1M
        std::shared_ptr<fslib::fs::File> _file;
        size_t _fileLength = 0;
        std::unique_ptr<std::vector<char>> _buffer;
        bool _eof = false;
        size_t _endOffsetInBlock = 0;
        size_t _offsetInBlock = 0;
        size_t _offsetInFile = 0;
        // Offset at which to start looking for the first record to return
        size_t _skipOffset;
        const uint32_t _headerLen;
        const uint64_t _beginOffset;
        const bool _isCheckSum;

    private:
        AUTIL_LOG_DECLARE();
    };

public:
    // prefix/suffix for wal, like log_$versionId.__wal__ for dir; and log_$offset.__wal__ for file
    static constexpr char wal_prefix[] = "log_";
    static constexpr char wal_suffix[] = ".__wal__";

private:
    static constexpr uint32_t MAX_WAL_FILE_SIZE = 1 * 1024 * 1024 * 1024; // 1G
    // recover from the beginning of the first existed file
    static constexpr int64_t DEFAULT_RECOVER_POSITION = -1;
    static constexpr size_t CONTINUOUS_APPEND_FAIL_LIMITS = 50;

    std::pair<std::shared_ptr<fslib::fs::File>, size_t> OpenFile(size_t offset, bool useDirectIO, fslib::Flag mode);
    bool OpenWriter(size_t offset);
    bool OpenReader(size_t begin_pos, size_t offset);
    bool LoadWALFiles();

private:
    const WALOption _walOpt;
    std::map<uint64_t, fslib::RichFileMeta> _walFiles; // map<offset, meta>
    bool _isRecovered = false;
    std::unique_ptr<Writer> _writer;
    std::unique_ptr<Reader> _reader;
    size_t _continuousAppendFails;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<WAL> WALPtr;
}} // namespace indexlib::file_system
