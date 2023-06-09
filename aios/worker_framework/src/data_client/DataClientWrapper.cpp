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
#include "fslib/util/FileUtil.h"
#include "worker_framework/DataClientWrapper.h"

#include <cstdlib>
#include <ctime>
#include <errno.h>
#include <sys/wait.h>
#include <unistd.h>

#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "worker_framework/DataItem.h"

using namespace std;
using namespace fslib;
using namespace fslib::fs;
using namespace fslib::util;



using namespace worker_framework;
namespace worker_framework {

AUTIL_DECLARE_AND_SETUP_LOGGER(worker_framework.data_client, DataClientWrapper);

const string DataClientWrapper::FSUTIL_BIN = string("fs_util_bin");
DataClientWrapper::DataClientWrapper() { _dataClient = new DataClient(); }

DataClientWrapper::~DataClientWrapper() { DELETE_AND_SET_NULL(_dataClient); }

vector<DataClientWrapper::DownloadMode>
DataClientWrapper::makeTryVector(const DataClientWrapper::DownloadMode &configDownloadMode) {
    vector<DataClientWrapper::DownloadMode> tryVector;
    tryVector.push_back(DataClientWrapper::MODE_DP2);
    tryVector.push_back(DataClientWrapper::MODE_FS_UTIL);
    tryVector.push_back(DataClientWrapper::MODE_FSLIB);

    if (configDownloadMode == DataClientWrapper::MODE_FS_UTIL) {
        iter_swap(tryVector.begin(), tryVector.begin() + 1);
    } else if (configDownloadMode == DataClientWrapper::MODE_FSLIB) {
        iter_swap(tryVector.begin(), tryVector.begin() + 2);
    }
    return tryVector;
}

DataClientWrapper::WFErrorCode
DataClientWrapper::downloadConfig(const string &srcUri, const string &dstDir, const string &configDownloadMode) {
    string fsutil = getFsUtilPath();
    DataClientWrapper::WFErrorCode errorCode = DataClientWrapper::ERROR_UNKNOWN;
    DataClientWrapper::DownloadMode mode = initConfigDownloadMode(configDownloadMode);
    vector<DataClientWrapper::DownloadMode> tryVector = makeTryVector(mode);
    for (size_t i = 0; i < tryVector.size(); i++) {
        if (FileSystem::isExist(dstDir) == EC_TRUE) {
            if (FileSystem::remove(dstDir) != EC_OK) {
                AUTIL_LOG(ERROR, "remove %s failed", dstDir.c_str());
                continue;
            }
        }
        if (tryVector[i] == DataClientWrapper::MODE_DP2 && !isLocalFile(srcUri) && _dataClient->init()) {
            AUTIL_LOG(INFO, "download config using dp2");
            errorCode = downloadWithDp2(srcUri, dstDir);
        } else if (tryVector[i] == DataClientWrapper::MODE_FS_UTIL && fsutil != "" &&
                   FileSystem::isExist(fsutil) == EC_TRUE) {
            AUTIL_LOG(INFO, "download config using fs_util");
            errorCode = downloadWithFsutil(fsutil, srcUri, dstDir);
        } else if (tryVector[i] == DataClientWrapper::MODE_FSLIB) {
            AUTIL_LOG(INFO, "download config using fslib");
            errorCode = downloadWithFslib(srcUri, dstDir);
        }

        if (errorCode == DataClientWrapper::ERROR_NONE) {
            break;
        }
    }
    return errorCode;
}

DataClientWrapper::DownloadMode DataClientWrapper::initConfigDownloadMode(const string &downloadModeStr) {
    DownloadMode downloadMode = strToDownloadMode(downloadModeStr);
    if (downloadMode == MODE_NONE) {
        string envDownloadMode;
        if (!getEnv("DEFAULT_DOWNLOAD_MODE", &envDownloadMode)) {
            AUTIL_LOG(WARN, "env[DEFAULT_DOWNLOAD_MODE] not declare");
        }
        downloadMode = strToDownloadMode(envDownloadMode);
        if (downloadMode == MODE_NONE) {
            downloadMode = DataClientWrapper::MODE_FS_UTIL;
        }
    }
    return downloadMode;
}

DataClientWrapper::WFErrorCode DataClientWrapper::downloadWithDp2(const string &srcUri, const string &dstDir) {
    string uriSuffixSpaceLimitStr;
    int uriSpaceLimit = 0;
    if (getEnv("DP2_URI_SUFFIX_SPACE_LIMIT", &uriSuffixSpaceLimitStr)) {
        if (!autil::StringUtil::fromString(uriSuffixSpaceLimitStr, uriSpaceLimit)) {
            AUTIL_LOG(WARN, "env[DP2_URI_SUFFIX_SPACE_LIMIT] is not a valid number");
            uriSpaceLimit = 0;
        }
    }
    string modifiedSrcUri = srcUri;
    if (uriSpaceLimit > 0) {
        srand((time(NULL) + dstDir.size()));
        int suffixSpaceCount = rand() % uriSpaceLimit;
        modifiedSrcUri += string(suffixSpaceCount, ' ');
        if (suffixSpaceCount > 0) {
            AUTIL_LOG(WARN, "add %d space in tail in order to break deploy chain", suffixSpaceCount);
        }
    }
    DataOption option;
    DataItemPtr dataItemPtr = _dataClient->getData(modifiedSrcUri, dstDir, option);
    while (true) {
        DataStatus status = dataItemPtr->getStatus();
        if (status == DS_FAILED || status == DS_STOPPED) {
            AUTIL_LOG(ERROR, "download use dp2 from [%s] to [%s] failed", modifiedSrcUri.c_str(), dstDir.c_str());
            return ERROR_DOWNLOAD;
        } else if (status == DS_FINISHED) {
            AUTIL_LOG(INFO, "download use dp2 from [%s] to [%s] success", modifiedSrcUri.c_str(), dstDir.c_str());
            return ERROR_NONE;
        } else if (status == DS_DEST) {
            AUTIL_LOG(INFO,
                      "download use dp2 from [%s] to [%s] failed, "
                      "status_code[%d], maybe dst disk has some problem",
                      modifiedSrcUri.c_str(),
                      dstDir.c_str(),
                      status);
            return ERROR_DEST;
        } else {
            AUTIL_INTERVAL_LOG(30,
                               INFO,
                               "wait download use dp2 from"
                               " [%s] to [%s], status_code[%d]",
                               modifiedSrcUri.c_str(),
                               dstDir.c_str(),
                               status);
            usleep(1000 * 1000);
        }
    }
    return ERROR_DOWNLOAD;
}

DataClientWrapper::WFErrorCode
DataClientWrapper::downloadWithFsutil(const string &fsutil, const string &remotePath, const string &localPath) {
    pid_t pid;
    if ((pid = vfork()) < 0) {
        AUTIL_LOG(ERROR, "vfork failed, errmsg:%s", strerror(errno));
        return ERROR_DOWNLOAD;
    } else if (pid == 0) {
        // child process
        // close stdout and stderr for child process
        close(1);
        close(2);
        if (execl(fsutil.c_str(),
                  fsutil.c_str(),
                  "cp",
                  "-r",
                  remotePath.c_str(),
                  localPath.c_str(),
                  "--defaultLog",
                  "INFO",
                  NULL) < 0) {
            AUTIL_LOG(ERROR, "execl process failed, errmsg:%s", strerror(errno));
            _exit(1);
        }
        _exit(0);
    } else {
        // parent process
        int childRet = 1;
        pid_t waitPid = waitpid(pid, &childRet, 0);
        if (waitPid < 0) {
            AUTIL_LOG(ERROR,
                      "wait child process exit failed, "
                      "pid:%d, errmsg:%s",
                      pid,
                      strerror(errno));
            return ERROR_DOWNLOAD;
        }
        if (childRet != 0) {
            AUTIL_LOG(ERROR,
                      "child process exit with error, "
                      "pid:%d, ret:%d",
                      pid,
                      childRet);
            return ERROR_DOWNLOAD;
        }
        return ERROR_NONE;
    }
}

DataClientWrapper::WFErrorCode DataClientWrapper::downloadWithFslib(const string &remotePath, const string &localPath) {
    ErrorCode ec = FileSystem::copy(remotePath, localPath);
    if (ec != fslib::EC_OK) {
        AUTIL_LOG(ERROR, "Failed to copy dir[%s] to [%s], errorCode[%d]", remotePath.c_str(), localPath.c_str(), ec);
        FileSystem::remove(localPath);
        if (isLocalDiskError(ec)) {
            return ERROR_FILEIO;
        } else {
            return ERROR_DOWNLOAD;
        }
    }
    return ERROR_NONE;
}

string DataClientWrapper::getFsUtilPath() {
    char buf[2048];
    memset(buf, 0, 2048);
    int ret = readlink("/proc/self/exe", buf, 2047);
    if (ret < 0 || ret >= 2047) {
        AUTIL_LOG(ERROR, "failed to get current binary path");
        return "";
    }
    string binaryPath(buf);
    size_t lastSlashPos = binaryPath.rfind('/');
    if (lastSlashPos == string::npos) {
        AUTIL_LOG(ERROR, "Invalid binary path: %s", binaryPath.c_str());
        return "";
    }
    string fsutil = binaryPath.substr(0, lastSlashPos + 1) + FSUTIL_BIN;
    return fsutil;
}

bool DataClientWrapper::isLocalFile(const string &path) {
    string fsType = FileSystem::getFsType(path);
    return ("local" == fsType || "LOCAL" == fsType);
}

bool DataClientWrapper::isLocalDiskError(fslib::ErrorCode ec) const {
    // TODO: fslib::ErrorCode(25) = EC_LOCAL_DISK_IO_ERROR when fslib >= 0.6.1
    return ec == fslib::EC_PERMISSION || ec == fslib::EC_LOCAL_DISK_NO_SPACE || ec == fslib::ErrorCode(25);
}

bool DataClientWrapper::getEnv(const string &name, string *value) {
    char *v = getenv(name.c_str());
    if (v == NULL) {
        return false;
    }
    *value = string(v);
    return true;
}

DataClientWrapper::DownloadMode DataClientWrapper::strToDownloadMode(const string &downloadMode) {
    if (downloadMode == "dp2") {
        return DataClientWrapper::MODE_DP2;
    } else if (downloadMode == "fs_util") {
        return DataClientWrapper::MODE_FS_UTIL;
    } else if (downloadMode == "fslib") {
        return DataClientWrapper::MODE_FSLIB;
    } else {
        return DataClientWrapper::MODE_NONE;
    }
}

}; // namespace worker_framework
