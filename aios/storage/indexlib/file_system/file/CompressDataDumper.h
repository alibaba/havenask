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

#include "autil/NoCopyable.h"
#include "autil/legacy/md5.h"
#include "indexlib/file_system/FileSystemMetricsReporter.h"
#include "indexlib/file_system/file/FileWriterImpl.h"
#include "indexlib/util/KeyValueMap.h"

namespace indexlib::util {
class BufferCompressor;
}

namespace indexlib { namespace file_system {
class CompressFileAddressMapper;

class CompressDataDumper : public autil::NoCopyable
{
public:
    CompressDataDumper(const std::shared_ptr<FileWriter>& fileWriter, const std::shared_ptr<FileWriter>& infoWriter,
                       const std::shared_ptr<FileWriter>& metaWriter, FileSystemMetricsReporter* reporter) noexcept;
    virtual ~CompressDataDumper() noexcept;

public:
    virtual FSResult<void> Init(const std::string& compressorName, size_t bufferSize,
                                const util::KeyValueMap& compressorParam) noexcept;

    virtual FSResult<void> Close() noexcept;

    FSResult<size_t> Write(const char* buffer, size_t length) noexcept;
    size_t GetWriteLength() const noexcept { return _writeLength; }
    std::shared_ptr<FileWriter> GetDataFileWriter() const noexcept { return _dataWriter; }
    std::shared_ptr<FileWriter> GetMetaFileWriter() const noexcept { return _metaWriter; }

public:
    static size_t EstimateCompressBufferSize(const std::string& compressorName, size_t bufferSize,
                                             const util::KeyValueMap& compressorParam) noexcept;

protected:
    virtual void FlushCompressorData() noexcept(false);
    void WriteCompressorData(const std::shared_ptr<util::BufferCompressor>& compressor, bool useHint) noexcept(false);
    void FlushInfoFile(const util::KeyValueMap& additionalInfo, bool enableCompress) noexcept(false);

    std::shared_ptr<util::BufferCompressor> CreateCompressor(const std::string& compressorName, size_t bufferSize,
                                                             const util::KeyValueMap& compressorParam) noexcept(false);

protected:
    size_t _bufferSize;
    size_t _writeLength;
    std::shared_ptr<CompressFileAddressMapper> _compFileAddrMapper;
    std::shared_ptr<util::BufferCompressor> _compressor;
    std::shared_ptr<FileWriter> _dataWriter;
    std::shared_ptr<FileWriter> _infoWriter;
    std::shared_ptr<FileWriter> _metaWriter;
    util::KeyValueMap _compressParam;
    FileSystemMetricsReporter* _reporter;
    kmonitor::MetricsTags _kmonTags;
    size_t _expandBlockCount;
    size_t _expandWasteSize;
    bool _encodeCompressAddressMapper;

    typedef autil::legacy::Md5Stream Md5Stream;
    typedef std::shared_ptr<Md5Stream> Md5StreamPtr;
    Md5StreamPtr _md5Stream;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<CompressDataDumper> CompressDataDumperPtr;

}} // namespace indexlib::file_system
