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

#include "fslib/fslib.h"
#include "worker_framework/DataClient.h"

namespace worker_framework {

class DataClientWrapper {
public:
    enum WFErrorCode
    {
        ERROR_NONE = 0,
        ERROR_DOWNLOAD = 1,
        ERROR_FILEIO = 2,
        ERROR_DEST = 3,
        ERROR_UNKNOWN = 4
    };
    enum DownloadMode
    {
        MODE_DP2 = 0,
        MODE_FS_UTIL = 1,
        MODE_FSLIB = 2,
        MODE_NONE = 3
    };

public:
    DataClientWrapper();
    virtual ~DataClientWrapper();

private:
    DataClientWrapper(const DataClientWrapper &);
    DataClientWrapper &operator=(const DataClientWrapper &);

public:
    virtual WFErrorCode
    downloadConfig(const std::string &srcBaseUri, const std::string &dstDir, const std::string &configDownloadMode);

private:
    DownloadMode initConfigDownloadMode(const std::string &configDownloadMode);
    DownloadMode strToDownloadMode(const std::string &downloadMode);

private:
    virtual WFErrorCode downloadWithDp2(const std::string &srcBaseUri, const std::string &dstDir);
    virtual WFErrorCode
    downloadWithFsutil(const std::string &fsutil, const std::string &remotePath, const std::string &localPath);
    virtual WFErrorCode downloadWithFslib(const std::string &remotePath, const std::string &localPath);

private:
    bool isLocalFile(const std::string &path);
    std::vector<DownloadMode> makeTryVector(const DownloadMode &configDownloadMode);
    virtual std::string getFsUtilPath();

private:
    bool getEnv(const std::string &name, std::string *value);

private:
    bool isLocalDiskError(fslib::ErrorCode ec) const;

private:
    DataClient *_dataClient;

private:
    static const std::string FSUTIL_BIN;
};

typedef std::shared_ptr<DataClientWrapper> DataClientWrapperPtr;

}; // namespace worker_framework
