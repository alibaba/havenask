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
#ifndef ISEARCH_BS_CONFIGDOWNLOADER_H
#define ISEARCH_BS_CONFIGDOWNLOADER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "fslib/fslib.h"

namespace build_service { namespace common {

class ConfigDownloader
{
public:
    enum DownloadErrorCode { DEC_NONE = 0, DEC_NORMAL_ERROR = 1, DEC_DEST_ERROR = 2 };

public:
    ConfigDownloader();
    ~ConfigDownloader();

private:
    ConfigDownloader(const ConfigDownloader&);
    ConfigDownloader& operator=(const ConfigDownloader&);

public:
    static DownloadErrorCode downloadConfig(const std::string& remotePath, std::string& localPath);
    static std::string getLocalConfigPath(const std::string& remotePath, const std::string& localPath);

private:
    static bool isLocalFile(const std::string& path);
    static DownloadErrorCode doDownloadConfig(const std::string& remotePath, const std::string& localPath);

private:
    static DownloadErrorCode removeIfExist(const std::string& localPath);
    static bool isLocalDiskError(fslib::ErrorCode ec);

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ConfigDownloader);

}} // namespace build_service::common

#endif // ISEARCH_BS_CONFIGDOWNLOADER_H
