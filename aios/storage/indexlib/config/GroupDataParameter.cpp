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
#include "indexlib/config/GroupDataParameter.h"

#include <assert.h>

#include "indexlib/config/ConfigDefine.h"
#include "indexlib/config/FileCompressConfig.h"
#include "indexlib/config/FileCompressSchema.h"
#include "indexlib/util/Exception.h"

using std::string;

namespace indexlib::config {
AUTIL_LOG_SETUP(indexlib.config, GroupDataParameter);

GroupDataParameter::GroupDataParameter() {}

GroupDataParameter::~GroupDataParameter() {}

const CompressTypeOption& GroupDataParameter::GetCompressTypeOption() const { return _compressType; }
const std::string& GroupDataParameter::GetDocCompressor() const { return _docCompressor; }
void GroupDataParameter::SetDocCompressor(const std::string& compressor) { _docCompressor = compressor; }

const std::string& GroupDataParameter::GetFileCompressor() const
{
    if (_fileCompressConfig) {
        return _fileCompressConfig->GetCompressType();
    }
    return _fileCompressor;
}
size_t GroupDataParameter::GetFileCompressBufferSize() const
{
    if (_fileCompressConfig) {
        return _fileCompressConfig->GetCompressBufferSize();
    }
    return _fileCompressBufferSize;
}

bool GroupDataParameter::IsCompressOffsetFileEnabled() const { return _enableOffsetFileCompress; }

void GroupDataParameter::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    if (json.GetMode() == TO_JSON) {
        if (_compressType.HasCompressOption()) {
            string compressStr = _compressType.GetCompressStr();
            assert(!compressStr.empty());
            json.Jsonize("compress_type", compressStr);
        }
        if (!_docCompressor.empty()) {
            json.Jsonize("doc_compressor", _docCompressor);
        }
        if (!_fileCompressor.empty()) {
            json.Jsonize("file_compressor", _fileCompressor);
        }
        if (_fileCompressBufferSize != DEFAULT_FILE_COMPRESSOR_BUFF_SIZE) {
            json.Jsonize("file_compress_buffer_size", _fileCompressBufferSize);
        }
        if (_enableOffsetFileCompress) {
            json.Jsonize("enable_compress_offset", _enableOffsetFileCompress);
        }
        if (!_fileCompressName.empty()) {
            json.Jsonize("file_compress", _fileCompressName);
        }
    } else {
        string compressType;
        json.Jsonize("compress_type", compressType, compressType);
        auto status = _compressType.Init(compressType);
        if (!status.IsOK()) {
            INDEXLIB_FATAL_ERROR(Schema, "invalid compressType [%s]", compressType.c_str());
        }
        json.Jsonize("doc_compressor", _docCompressor, _docCompressor);
        json.Jsonize("file_compressor", _fileCompressor, _fileCompressor);
        json.Jsonize("file_compress_buffer_size", _fileCompressBufferSize, _fileCompressBufferSize);
        json.Jsonize("enable_compress_offset", _enableOffsetFileCompress, _enableOffsetFileCompress);
        json.Jsonize("file_compress", _fileCompressName, _fileCompressName);
    }
}
Status GroupDataParameter::CheckEqual(const GroupDataParameter& other) const
{
    CHECK_CONFIG_EQUAL(_compressType, other._compressType, "compress_type not equal");
    CHECK_CONFIG_EQUAL(_docCompressor, other._docCompressor, "doc_compressor not equal");
    CHECK_CONFIG_EQUAL(_fileCompressor, other._fileCompressor, "file_compressor not equal");
    CHECK_CONFIG_EQUAL(_fileCompressBufferSize, other._fileCompressBufferSize, "file_compress_buffer_size not equal");
    CHECK_CONFIG_EQUAL(_enableOffsetFileCompress, other._enableOffsetFileCompress, "enable_compress_offset not equal");
    CHECK_CONFIG_EQUAL(_fileCompressName, other._fileCompressName, "file_compress not equal");
    return Status::OK();
}

bool GroupDataParameter::NeedSyncFileCompressConfig() const
{
    return !_fileCompressName.empty() && _fileCompressConfig == nullptr;
}

const std::shared_ptr<indexlib::config::FileCompressConfig>& GroupDataParameter::GetFileCompressConfig() const
{
    return _fileCompressConfig;
}

void GroupDataParameter::SetFileCompressConfig(const std::shared_ptr<indexlib::config::FileCompressConfig>& config)
{
    _fileCompressConfig = config;
    _fileCompressName = (config != nullptr) ? config->GetCompressName() : "";
}
Status GroupDataParameter::SyncFileCompressConfig(const std::shared_ptr<FileCompressSchema>& fileCompressSchema)
{
    if (_fileCompressName.empty()) {
        return Status::OK();
    }
    std::shared_ptr<FileCompressConfig> fileCompressConfig;
    if (fileCompressSchema) {
        fileCompressConfig = fileCompressSchema->GetFileCompressConfig(_fileCompressName);
    }
    if (!fileCompressConfig) {
        // TODO(xioahao.yxh) remove this when new schema ios ready
        fileCompressConfig.reset(new FileCompressConfig(_fileCompressName));
    }
    SetFileCompressConfig(fileCompressConfig);
    return Status::OK();
}

void GroupDataParameter::SetFileCompressConfigV2(
    const std::shared_ptr<indexlibv2::config::FileCompressConfigV2>& fileCompressConfigV2)
{
    _fileCompressConfigV2 = fileCompressConfigV2;
}

const std::shared_ptr<indexlibv2::config::FileCompressConfigV2>& GroupDataParameter::GetFileCompressConfigV2() const
{
    return _fileCompressConfigV2;
}

} // namespace indexlib::config
