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
#include "build_service_tasks/io/MultiFileOutput.h"

#include "autil/StringUtil.h"
#include "build_service/document/DocumentDefine.h"
#include "indexlib/file_system/file/BufferedFileWriter.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "swift/client/MessageInfo.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace build_service;
using namespace build_service::config;
using namespace build_service::io;
using namespace build_service::task_base;
using namespace build_service::document;
using namespace indexlib::file_system;

namespace build_service_tasks {

BS_LOG_SETUP(build_service_tasks, MultiFileOutput);

const std::string MultiFileOutput::TEMP_MULTI_FILE_SUFFIX = ".__temp__";

MultiFileOutput::MultiFileOutput(const TaskOutputConfig& outputConfig) : Output(outputConfig), _currentFileId(-1) {}

MultiFileOutput::~MultiFileOutput() { close(); }

void MultiFileOutput::close() { switchFile(); }

bool MultiFileOutput::init(const KeyValueMap& initParams)
{
    mergeParams(initParams);
    const auto& params = _outputConfig.getParameters();
    _destDirectory = getValueFromKeyValueMap(params, "dest_directory");
    if (_destDirectory.empty()) {
        BS_LOG(ERROR, "dest_directory is empty");
        return false;
    }
    _filePrefix = getValueFromKeyValueMap(params, "file_prefix");
    if (_filePrefix.empty()) {
        BS_LOG(ERROR, "file_prefix is empty");
        return false;
    }
    auto rangeStr = getValueFromKeyValueMap(params, "range_str");
    _filePrefix += rangeStr;
    fslib::FileList fileList;
    FslibWrapper::ListDirE(_destDirectory, fileList);
    size_t maxFileId = 0;
    for (const auto& fileName : fileList) {
        if (StringUtil::startsWith(fileName, _filePrefix)) {
            if (StringUtil::endsWith(fileName, TEMP_MULTI_FILE_SUFFIX)) {
                FslibWrapper::DeleteFileE(FslibWrapper::JoinPath(_destDirectory, fileName),
                                          DeleteOption::NoFence(false));
                continue;
            }
            size_t fileId = 0;
            if (!StringUtil::fromString(fileName.substr(_filePrefix.size() + 1), fileId)) {
                BS_LOG(ERROR, "invalid file name [%s]", fileName.c_str());
                return false;
            }
            maxFileId = std::max(maxFileId, fileId);
        }
    }
    _currentFileId = maxFileId;
    return switchFile();
}

bool MultiFileOutput::switchFile()
{
    if (_file) {
        assert(!_currentTempFileName.empty());
        assert(!_currentTargetFileName.empty());
        _file->Close().GetOrThrow();
        _file.reset();
        FslibWrapper::RenameE(_currentTempFileName, _currentTargetFileName);
        _currentTempFileName.clear();
        _currentTargetFileName.clear();
    }
    ++_currentFileId;
    _currentTargetFileName = FslibWrapper::JoinPath(_destDirectory, _filePrefix + "_" + std::to_string(_currentFileId));
    _currentTempFileName = _currentTargetFileName + TEMP_MULTI_FILE_SUFFIX;
    return true;
}

BufferedFileWriter* MultiFileOutput::createFile(const KeyValueMap& params)
{
    auto filePath = getValueFromKeyValueMap(params, "file_path");
    auto bufferSizeStr = getValueFromKeyValueMap(params, "buffer_size");
    uint32_t bufferSize = DEFAULT_BUFFER_SIZE;
    if (!bufferSizeStr.empty()) {
        if (!StringUtil::fromString(bufferSizeStr, bufferSize)) {
            BS_LOG(ERROR, "buffer_size [%s] invalid", bufferSizeStr.c_str());
            return nullptr;
        }
    }
    auto asyncStr = getValueFromKeyValueMap(params, "async");
    bool async = false;
    if (!asyncStr.empty()) {
        if (!StringUtil::parseTrueFalse(asyncStr, async)) {
            BS_LOG(ERROR, "async [%s] invalid", asyncStr.c_str());
            return nullptr;
        }
    }

    indexlib::file_system::WriterOption writerOption;
    writerOption.bufferSize = bufferSize;
    writerOption.asyncDump = async;
    auto file = new indexlib::file_system::BufferedFileWriter(writerOption);
    if (auto ec = file->Open(filePath, filePath).Code(); ec != FSEC_OK) {
        BS_LOG(ERROR, "create file writer [%s] failed, ec [%d]", filePath.c_str(), ec);
        return nullptr;
    }
    return file;
}

bool MultiFileOutput::write(autil::legacy::Any& any)
{
    if (!_file) {
        auto outputConfig = _outputConfig;
        outputConfig.addParameters("file_path", _currentTempFileName);
        _file.reset(createFile(outputConfig.getParameters()));
    }
    try {
        const auto& data = AnyCast<StringView>(any);
        auto [ec, _] = _file->Write(data.data(), data.size());
        if (ec != FSEC_OK) {
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

bool MultiFileOutput::commit()
{
    if (!_file) {
        return true;
    }
    return switchFile();
}

} // namespace build_service_tasks
