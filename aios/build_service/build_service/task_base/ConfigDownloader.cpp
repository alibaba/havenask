#include "build_service/task_base/ConfigDownloader.h"
#include "build_service/common/PathDefine.h"
#include "build_service/util/FileUtil.h"
#include <autil/TimeUtility.h>
#include <worker_framework/DataClientWrapper.h>
#include <fslib/fslib.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>
#include <errno.h>
#include <cstring>

using namespace std;
using namespace autil;
using namespace fslib;
using namespace fslib::fs;
using namespace build_service::util;
using namespace build_service::common;

USE_WORKER_FRAMEWORK_NAMESPACE(data_client);

namespace build_service {
namespace task_base {
BS_LOG_SETUP(task_base, ConfigDownloader);

string ConfigDownloader::getLocalConfigPath(
        const string &remotePath, const string &localPath)
{
    if (localPath.empty() || *(localPath.begin()) != '/'
        || remotePath.empty() || remotePath == "/")
    {
        string errorMsg = "get config path failed! Invalid parameter:"
                          "remotePath[" + remotePath + "] , localPath["
                          + localPath + "]";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return string();
    }
    size_t pos = remotePath.rfind('/', remotePath.length() - 2);
    if (pos == string::npos) {
        return string();
    }
    return FileUtil::joinFilePath(localPath, remotePath.substr(pos + 1));
}

ConfigDownloader::DownloadErrorCode ConfigDownloader::downloadConfig(
        const string &remotePath, string &localPath)
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
        string errorMsg = "failed to check whether " + localPath
                          + " exist, local disk has some problem";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return DEC_DEST_ERROR;
    } else if (ec != EC_TRUE && ec != EC_FALSE) {
        string errorMsg = "failed to check whether " + localPath + " exist";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return DEC_NORMAL_ERROR;
    } else if (ec == EC_TRUE) {
        BS_LOG(INFO, "[%s] exist, download config done", localPath.c_str());
        return DEC_NONE;
    } else {
        return doDownloadConfig(remotePath, localPath);
    }

}

bool ConfigDownloader::isLocalFile(const string &path) {
    string fsType = FileSystem::getFsType(path);
    return ("local" == fsType || "LOCAL" == fsType);
}

ConfigDownloader::DownloadErrorCode ConfigDownloader::doDownloadConfig(
        const string &remotePath, const string &localPath)
{
    string tmpPath = localPath;
    int i = tmpPath.size() - 1;
    for (; i >= 0 && tmpPath[i] == '/'; i--);
    tmpPath = tmpPath.substr(0, i+1);
    tmpPath += "_tmp";

    DownloadErrorCode errorCode = removeIfExist(tmpPath);
    if (errorCode != DEC_NONE) {
        return errorCode;
    }
    
    int64_t timeBeforeDownload = TimeUtility::currentTimeInSeconds();
    DataClientWrapper dataClientWrapper;
    DataClientWrapper::WFErrorCode wfCode = 
        dataClientWrapper.downloadConfig(remotePath, tmpPath, "");
    int64_t timeAfterDownload = TimeUtility::currentTimeInSeconds();
    DownloadErrorCode ret;
    if (wfCode == DataClientWrapper::ERROR_NONE) {
        ErrorCode ec = FileSystem::rename(tmpPath, localPath);
        if (ec != EC_OK) {
            stringstream ss;
            ss << "Failed to rename [" << tmpPath
               << "] to [" <<  localPath
               << "], errroCode[" << ec << "]";
            string errorMsg = ss.str();
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            ret = DEC_NORMAL_ERROR;
        } else {
            BS_LOG(INFO, "rename [%s] to [%s]", tmpPath.c_str(), localPath.c_str());
            BS_LOG(INFO, "download config use [%ld] seconds", timeAfterDownload - timeBeforeDownload);
            ret = DEC_NONE;
        }
    } else if (wfCode == DataClientWrapper::ERROR_DEST || wfCode == DataClientWrapper::ERROR_FILEIO) 
    {
        string errorMsg = "download config from [" + remotePath + "] to ["
                          + localPath + "] failed, maybe disk has problem";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        ret = DEC_DEST_ERROR;
    } else {
        string errorMsg = "download config from [" + remotePath + "] to ["
                          + localPath + "] failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        ret = DEC_NORMAL_ERROR;
    }
    return ret;
}

ConfigDownloader::DownloadErrorCode ConfigDownloader::removeIfExist(const string &localPath) {
    ErrorCode ec = FileSystem::isExist(localPath);
    if (isLocalDiskError(ec)) {
        string errorMsg = "failed to check whether " + localPath
                          + " exist, local disk has some problem";
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
        string errorMsg = "failed to check whether " + localPath
                          + " exist, local disk has some problem";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return DEC_DEST_ERROR;
    } else if (ec != EC_OK) {
        BS_LOG(DEBUG, "remove [%s] failed.", localPath.c_str());
        return DEC_NORMAL_ERROR;
    }
    return DEC_NONE;
}

bool ConfigDownloader::isLocalDiskError(ErrorCode ec) {
    return ec == fslib::EC_PERMISSION || ec == fslib::EC_LOCAL_DISK_NO_SPACE
        || ec == fslib::ErrorCode(25);
}

}
}
