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
#include "build_service/io/FileOutput.h"

#include "autil/ConstString.h"
#include "autil/StringUtil.h"
#include "indexlib/file_system/file/BufferedFileReader.h"
#include "indexlib/file_system/file/BufferedFileWriter.h"

using namespace std;
using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace build_service::util;
using namespace build_service::config;

namespace build_service { namespace io {
BS_LOG_SETUP(io, FileOutput);

FileOutput::FileOutput(const TaskOutputConfig& outputConfig) : Output(outputConfig) {}

FileOutput::~FileOutput() { close(); }

bool FileOutput::init(const KeyValueMap& initParams)
{
    mergeParams(initParams);
    const auto& params = _outputConfig.getParameters();
    auto filePath = getValueFromKeyValueMap(params, "file_path");
    auto bufferSizeStr = getValueFromKeyValueMap(params, "buffer_size");
    uint32_t bufferSize = DEFAULT_BUFFER_SIZE;
    if (!bufferSizeStr.empty()) {
        if (!StringUtil::fromString(bufferSizeStr, bufferSize)) {
            BS_LOG(ERROR, "buffer_size [%s] invalid", bufferSizeStr.c_str());
            return false;
        }
    }
    auto asyncStr = getValueFromKeyValueMap(params, "async");
    bool async = false;
    if (!asyncStr.empty()) {
        if (!StringUtil::parseTrueFalse(asyncStr, async)) {
            BS_LOG(ERROR, "async [%s] invalid", asyncStr.c_str());
            return false;
        }
    }

    indexlib::file_system::WriterOption writerOption;
    writerOption.bufferSize = bufferSize;
    writerOption.asyncDump = async;
    _file.reset(new indexlib::file_system::BufferedFileWriter(writerOption));
    if (auto ec = _file->Open(filePath, filePath).Code(); ec != indexlib::file_system::FSEC_OK) {
        BS_LOG(ERROR, "create file writer [%s] failed, ec [%d]", filePath.c_str(), ec);
        return false;
    }
    return true;
}

size_t FileOutput::getLength() const { return _file->GetLength(); }

bool FileOutput::write(autil::legacy::Any& any)
{
    try {
        const auto& data = AnyCast<StringView>(any);
        auto [ec, _] = _file->Write(data.data(), data.size());
        if (ec != indexlib::file_system::FSEC_OK) {
            BS_LOG(ERROR, "write failed, ec[%d]", ec);
            return false;
        }
        return true;
    } catch (const BadAnyCast& e) {
        BS_LOG(ERROR, "BadAnyCast: %s", e.what());
        return false;
    }
    return true;
}

void FileOutput::write(const void* buffer, size_t length) { _file->Write(buffer, length).GetOrThrow(); }

bool FileOutput::commit()
{
    auto ec = _file->Flush().Code();
    if (ec != indexlib::file_system::FSEC_OK) {
        BS_LOG(ERROR, "flush failed, ec[%d]", ec);
        return false;
    }
    return true;
}

bool FileOutput::cloneFrom(const std::string& srcFileName, size_t fileLength)
{
    indexlib::file_system::BufferedFileReader reader(DEFAULT_BUFFER_SIZE);
    auto status = reader.Open(srcFileName).Status();
    if (!status.IsOK()) {
        BS_LOG(ERROR, "open src file [%s] failed", srcFileName.c_str());
        return false;
    }

    indexlib::file_system::ReadOption readOption;
    char buffer[DEFAULT_BUFFER_SIZE];

    size_t restLength = fileLength;
    while (restLength > 0) {
        auto [status, readLen] =
            reader.Read(buffer, std::min((size_t)DEFAULT_BUFFER_SIZE, restLength), readOption).StatusWith();
        if (!status.IsOK()) {
            BS_LOG(ERROR, "read src file [%s] failed", srcFileName.c_str());
            return false;
        }

        assert(readLen <= restLength);
        restLength -= readLen;
        auto [writeStatus, writeLen] = _file->Write(buffer, readLen).StatusWith();
        if (!writeStatus.IsOK() || writeLen != readLen) {
            BS_LOG(ERROR, "write file failed, status [%s], write len [%ld] and read len [%ld]",
                   writeStatus.ToString().c_str(), writeLen, readLen);
            return false;
        }
    }
    return commit();
}

void FileOutput::close()
{
    if (_file) {
        _file->Close().GetOrThrow();
    }
}

}} // namespace build_service::io
