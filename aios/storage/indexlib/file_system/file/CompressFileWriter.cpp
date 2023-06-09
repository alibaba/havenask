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
#include "indexlib/file_system/file/CompressFileWriter.h"

#include "indexlib/file_system/file/CompressDataDumper.h"
#include "indexlib/file_system/file/HintCompressDataDumper.h"

using namespace std;
using namespace indexlib::util;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, CompressFileWriter);

CompressFileWriter::CompressFileWriter(FileSystemMetricsReporter* reporter) noexcept
    : FileWriterImpl([](int64_t size) {})
    , _reporter(reporter)
{
}

CompressFileWriter::~CompressFileWriter() noexcept {}

FSResult<void> CompressFileWriter::Init(const std::shared_ptr<FileWriter>& fileWriter,
                                        const std::shared_ptr<FileWriter>& infoWriter,
                                        const std::shared_ptr<FileWriter>& metaWriter, const string& compressorName,
                                        size_t bufferSize, const KeyValueMap& compressorParam) noexcept
{
    assert(fileWriter);
    assert(infoWriter);

    _compressorName = compressorName;
    _logicalPath = fileWriter->GetLogicalPath();

    bool needHintData = GetTypeValueFromKeyValueMap(compressorParam, COMPRESS_ENABLE_HINT_PARAM_NAME, (bool)false);
    if (needHintData) {
        _dumper.reset(new HintCompressDataDumper(fileWriter, infoWriter, metaWriter, _reporter));
    } else {
        _dumper.reset(new CompressDataDumper(fileWriter, infoWriter, metaWriter, _reporter));
    }
    RETURN_IF_FS_ERROR(_dumper->Init(compressorName, bufferSize, compressorParam), "Init dumper failed");
    return FSEC_OK;
}

FSResult<size_t> CompressFileWriter::Write(const void* buffer, size_t length) noexcept
{
    assert(_dumper);
    return _dumper->Write((const char*)buffer, length);
}

ErrorCode CompressFileWriter::DoClose() noexcept
{
    assert(_dumper);
    return _dumper->Close().Code();
}

size_t CompressFileWriter::GetLength() const noexcept
{
    std::shared_ptr<FileWriter> dataWriter = GetDataFileWriter();
    return dataWriter != nullptr ? dataWriter->GetLength() : 0;
}

FSResult<void> CompressFileWriter::ReserveFile(size_t reserveSize) noexcept
{
    std::shared_ptr<FileWriter> dataWriter = GetDataFileWriter();
    if (dataWriter) {
        return dataWriter->ReserveFile(reserveSize);
    }
    return FSEC_OK;
}

size_t CompressFileWriter::GetUncompressLength() const noexcept
{
    return _dumper != nullptr ? _dumper->GetWriteLength() : 0;
}

std::shared_ptr<FileWriter> CompressFileWriter::GetDataFileWriter() const noexcept
{
    std::shared_ptr<FileWriter> dataWriter;
    if (_dumper) {
        dataWriter = _dumper->GetDataFileWriter();
    }
    return dataWriter;
}
}} // namespace indexlib::file_system
