#include "build_service/io/FileOutput.h"
#include <autil/StringUtil.h>
#include <autil/ConstString.h>

using namespace std;
using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace build_service::util;
using namespace build_service::config;
using namespace fslib;
using namespace fslib::fs;

namespace build_service {
namespace io {
BS_LOG_SETUP(io, FileOutput);

FileOutput::FileOutput(const TaskOutputConfig& outputConfig) : Output(outputConfig) {}

FileOutput::~FileOutput() { close(); }

bool FileOutput::init(const KeyValueMap& initParams) {
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
    try {
        _file.reset(IE_NAMESPACE(storage)::FileSystemWrapper::OpenBufferedFile(
            filePath, fslib::WRITE, bufferSize, async));
        if (!_file) {
            BS_LOG(ERROR, "create file writer [%s] failed", filePath.c_str());
            return false;
        }
    } catch (const IE_NAMESPACE(misc)::FileIOException& e) {
        BS_LOG(ERROR, "create file writer [%s] failed, error [%s]", filePath.c_str(), e.what());
        return false;
    }

    return true;    
}
bool FileOutput::write(autil::legacy::Any& any) {
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

void FileOutput::write(const void* buffer, size_t length) { _file->Write(buffer, length); }

bool FileOutput::commit() {
    try {
        _file->Flush();
    } catch (const IE_NAMESPACE(misc)::FileIOException& e) {
        BS_LOG(ERROR, "flush FileIO Exception: %s", e.what());
        return false;
    }
    return true;
}

void FileOutput::close() {
    if (_file) {
        _file->Close();
    }
}

}  // namespace io
}  // namespace build_service
