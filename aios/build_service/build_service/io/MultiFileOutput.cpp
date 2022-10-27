#include "build_service/io/MultiFileOutput.h"
#include "build_service/document/DocumentDefine.h"
#include <indexlib/storage/file_system_wrapper.h>
#include <swift/client/MessageInfo.h>
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace build_service;
using namespace build_service::config;
using namespace build_service::io;
using namespace build_service::task_base;
using namespace build_service::document;
IE_NAMESPACE_USE(storage);

namespace build_service {
namespace io {

BS_LOG_SETUP(build_service_tasks, MultiFileOutput);

const std::string MultiFileOutput::TEMP_MULTI_FILE_SUFFIX = ".__temp__";

MultiFileOutput::MultiFileOutput(const TaskOutputConfig& outputConfig)
    : Output(outputConfig), _currentFileId(-1) {}

MultiFileOutput::~MultiFileOutput() { close(); }

void MultiFileOutput::close() { switchFile(); }

bool MultiFileOutput::init(const KeyValueMap& initParams) {
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
    FileSystemWrapper::ListDir(_destDirectory, fileList);
    size_t maxFileId = 0;
    for (const auto& fileName : fileList) {
        if (StringUtil::endsWith(fileName, TEMP_MULTI_FILE_SUFFIX)) {
            FileSystemWrapper::DeleteFile(
                FileSystemWrapper::JoinPath(_destDirectory, fileName));
            continue;
        }
        if (StringUtil::startsWith(fileName, _filePrefix)) {
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

bool MultiFileOutput::switchFile() {
    if (_file) {
        assert(!_currentTempFileName.empty());
        assert(!_currentTargetFileName.empty());
        _file->Close();
        _file.reset();
        FileSystemWrapper::Rename(_currentTempFileName, _currentTargetFileName);
        _currentTempFileName.clear();
        _currentTargetFileName.clear();
    }
    ++_currentFileId;
    _currentTargetFileName = FileSystemWrapper::JoinPath(
        _destDirectory, _filePrefix + "_" + std::to_string(_currentFileId));
    _currentTempFileName = _currentTargetFileName + TEMP_MULTI_FILE_SUFFIX;
    return true;
}

BufferedFileWrapper* MultiFileOutput::createFile(const KeyValueMap& params) {
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
    try {
        return FileSystemWrapper::OpenBufferedFile(filePath, fslib::WRITE, bufferSize, async);
    } catch (const IE_NAMESPACE(misc)::FileIOException& e) {
        BS_LOG(ERROR, "create file writer [%s] failed, error [%s]", filePath.c_str(),
               e.what());
        return nullptr;
    }
    return nullptr;
}

bool MultiFileOutput::write(autil::legacy::Any& any) {
    if (!_file) {
        auto outputConfig = _outputConfig;
        outputConfig.addParameters("file_path", _currentTempFileName);
        _file.reset(createFile(outputConfig.getParameters()));
    }
    try {
        const auto& data = AnyCast<ConstString>(any);
        _file->Write(data.data(), data.size());
        return true;
    } catch (const BadAnyCast& e) {
        BS_LOG(ERROR, "BadAnyCast: %s", e.what());
        return false;
    } catch (const IE_NAMESPACE(misc)::FileIOException& e) {
        BS_LOG(ERROR, "write FileIO Exception: %s", e.what());
        return false;
    }
    return true;
}

bool MultiFileOutput::commit() {
    if (!_file) {
        return true;
    }
    return switchFile();
}

}  // namespace io
}  // namespace build_service
