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
#include "build_service/common/ConfigDownloader.h"

#include <ostream>
#include <signal.h>
#include <stdint.h>

#include "alog/Logger.h"
#include "autil/Span.h"
#include "autil/TimeUtility.h"
#include "build_service/common/PathDefine.h"
#include "build_service/util/ErrorLogCollector.h"
#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"
#include "fslib/util/FileUtil.h"
#include "worker_framework/DataClientWrapper.h"
#include "worker_framework/DataOption.h"

using namespace std;
using namespace autil;
using namespace fslib;
using namespace fslib::fs;
using namespace build_service::util;

using namespace worker_framework;

namespace build_service { namespace common {
BS_LOG_SETUP(common, ConfigDownloader);

string ConfigDownloader::getLocalConfigPath(const string& remotePath, const string& localPath)
{
    if (localPath.empty() || *(localPath.begin()) != '/' || remotePath.empty() || remotePath == "/") {
        string errorMsg = "get config path failed! Invalid parameter:"
                          "remotePath[" +
                          remotePath + "] , localPath[" + localPath + "]";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return string();
    }
    size_t pos = remotePath.rfind('/', remotePath.length() - 2);
    if (pos == string::npos) {
        return string();
    }
    return fslib::util::FileUtil::joinFilePath(localPath, remotePath.substr(pos + 1));
}

ConfigDownloader::DownloadErrorCode ConfigDownloader::downloadConfig(const string& remotePath, string& localPath)
{
    if (localPath.empty()) {
        localPath = PathDefine::getLocalConfigPath();
    }
    localPath = getLocalConfigPath(remotePath, localPath);
    if (localPath.empty()) {
        return DEC_NORMAL_ERROR;
    }
    ErrorCode ec = FileSystem::isExist(localPath);
    if (isLocalDiskError(ec)) {
        string errorMsg = "failed to check whether " + localPath + " exist, local disk has some problem";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return DEC_DEST_ERROR;
    } else if (ec != EC_TRUE && ec != EC_FALSE) {
        string errorMsg = "failed to check whether " + localPath + " exist";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return DEC_NORMAL_ERROR;
    } else if (ec == EC_TRUE) {
        BS_INTERVAL_LOG(1800, INFO, "[%s] exist, download config done", localPath.c_str());
        return DEC_NONE;
    } else {
        return doDownloadConfig(remotePath, localPath);
    }
}

bool ConfigDownloader::isLocalFile(const string& path)
{
    string fsType = FileSystem::getFsType(path);
    return ("local" == fsType || "LOCAL" == fsType);
}

ConfigDownloader::DownloadErrorCode ConfigDownloader::doDownloadConfig(const string& remotePath,
                                                                       const string& localPath)
{
    string tmpPath = localPath;
    int i = tmpPath.size() - 1;
    for (; i >= 0 && tmpPath[i] == '/'; i--)
        ;
    tmpPath = tmpPath.substr(0, i + 1);
    tmpPath += "_tmp";

    DownloadErrorCode errorCode = removeIfExist(tmpPath);
    if (errorCode != DEC_NONE) {
        return errorCode;
    }

    int64_t timeBeforeDownload = TimeUtility::currentTimeInSeconds();
    DataClientWrapper dataClientWrapper;
    DataClientWrapper::WFErrorCode wfCode = dataClientWrapper.downloadConfig(remotePath, tmpPath, "");
    int64_t timeAfterDownload = TimeUtility::currentTimeInSeconds();
    DownloadErrorCode ret;
    if (wfCode == DataClientWrapper::ERROR_NONE) {
        ErrorCode ec = FileSystem::rename(tmpPath, localPath);
        if (ec != EC_OK) {
            stringstream ss;
            ss << "Failed to rename [" << tmpPath << "] to [" << localPath << "], errroCode[" << ec << "]";
            string errorMsg = ss.str();
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            ret = DEC_NORMAL_ERROR;
        } else {
            BS_LOG(INFO, "rename [%s] to [%s]", tmpPath.c_str(), localPath.c_str());
            BS_LOG(INFO, "download config from [%s] use [%ld] seconds", remotePath.c_str(),
                   timeAfterDownload - timeBeforeDownload);
            ret = DEC_NONE;
        }
    } else if (wfCode == DataClientWrapper::ERROR_DEST || wfCode == DataClientWrapper::ERROR_FILEIO) {
        string errorMsg =
            "download config from [" + remotePath + "] to [" + localPath + "] failed, maybe disk has problem";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        ret = DEC_DEST_ERROR;
    } else {
        string errorMsg = "download config from [" + remotePath + "] to [" + localPath + "] failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        ret = DEC_NORMAL_ERROR;
    }
    return ret;
}

ConfigDownloader::DownloadErrorCode ConfigDownloader::removeIfExist(const string& localPath)
{
    ErrorCode ec = FileSystem::isExist(localPath);
    if (isLocalDiskError(ec)) {
        string errorMsg = "failed to check whether " + localPath + " exist, local disk has some problem";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return DEC_DEST_ERROR;
    } else if (ec != EC_TRUE && ec != EC_FALSE) {
        string errorMsg = "failed to check whether " + localPath + " exist";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return DEC_NORMAL_ERROR;
    } else if (ec == EC_FALSE) {
        return DEC_NONE;
    }
    ec = FileSystem::remove(localPath);
    BS_LOG(INFO, "[%s] removed", localPath.c_str());
    if (isLocalDiskError(ec)) {
        string errorMsg = "failed to check whether " + localPath + " exist, local disk has some problem";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return DEC_DEST_ERROR;
    } else if (ec != EC_OK) {
        BS_LOG(DEBUG, "remove [%s] failed.", localPath.c_str());
        return DEC_NORMAL_ERROR;
    }
    return DEC_NONE;
}

bool ConfigDownloader::isLocalDiskError(ErrorCode ec)
{
    return ec == fslib::EC_PERMISSION || ec == fslib::EC_LOCAL_DISK_NO_SPACE || ec == fslib::ErrorCode(25);
}

}} // namespace build_service::common
