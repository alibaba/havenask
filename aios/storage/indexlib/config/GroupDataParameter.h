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

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/base/Status.h"
#include "indexlib/config/CompressTypeOption.h"
namespace indexlibv2::config {
class FileCompressConfigV2;
}
namespace indexlib::config {
class FileCompressConfig;
class FileCompressSchema;

class GroupDataParameter : public autil::legacy::Jsonizable
{
public:
    GroupDataParameter();
    ~GroupDataParameter();

public:
    const CompressTypeOption& GetCompressTypeOption() const;
    const std::string& GetDocCompressor() const;

    const std::string& GetFileCompressor() const;
    size_t GetFileCompressBufferSize() const;

    bool IsCompressOffsetFileEnabled() const;

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    Status CheckEqual(const GroupDataParameter& other) const;

    bool NeedSyncFileCompressConfig() const;
    const std::string& GetFileCompressName() const { return _fileCompressName; }
    const std::shared_ptr<indexlibv2::config::FileCompressConfigV2>& GetFileCompressConfigV2() const;
    // TODO(xiaohao.yxh) remove this function
    const std::shared_ptr<indexlib::config::FileCompressConfig>& GetFileCompressConfig() const;

public:
    // TODO(xiaohao,yxh) remove this function
    void SetFileCompressConfig(const std::shared_ptr<indexlib::config::FileCompressConfig>& config);
    Status SyncFileCompressConfig(const std::shared_ptr<FileCompressSchema>& fileCompressSchema);

    void SetFileCompressConfigV2(const std::shared_ptr<indexlibv2::config::FileCompressConfigV2>& fileCompressConfigV2);
    void SetDocCompressor(const std::string& compressor);

private:
    CompressTypeOption _compressType;
    std::string _docCompressor;

    /* BEGIN: to remove parameters, use file_compress instead */
    std::string _fileCompressor;
    size_t _fileCompressBufferSize = DEFAULT_FILE_COMPRESSOR_BUFF_SIZE;
    bool _enableOffsetFileCompress = false;
    /* END: to remove parameters */

    std::string _fileCompressName;
    std::shared_ptr<indexlib::config::FileCompressConfig> _fileCompressConfig;
    std::shared_ptr<indexlibv2::config::FileCompressConfigV2> _fileCompressConfigV2;

    static constexpr size_t DEFAULT_FILE_COMPRESSOR_BUFF_SIZE = 4 * 1024;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::config
