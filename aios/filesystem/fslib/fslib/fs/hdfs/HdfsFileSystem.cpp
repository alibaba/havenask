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
#include "fslib/fs/hdfs/HdfsFileSystem.h"

#include <errno.h>
#include <glob.h>
#include <stdlib.h>

#include "autil/EnvUtil.h"
#include "autil/StringUtil.h"
#include "fslib/fs/hdfs/HdfsFile.h"
#include "kmonitor_adapter/MonitorFactory.h"

using namespace std;
using namespace autil;

FSLIB_PLUGIN_BEGIN_NAMESPACE(hdfs);
AUTIL_DECLARE_AND_SETUP_LOGGER(hdfs, HdfsFileSystem);

const size_t HdfsFileSystem::MAX_CONNECTION = 5;
const string HdfsFileSystem::HADOOP_CLASSPATH_ENV("FSLIB_HADOOP_CLASSPATH");
const string HdfsFileSystem::CLASSPATH_ENV("CLASSPATH");
const string HdfsFileSystem::HADOOP_HOME_ENV("HADOOP_HOME");
const string HdfsFileSystem::HDFS_PREFIX("hdfs://");
const string HdfsFileSystem::DFS_PREFIX("dfs://");
const string HdfsFileSystem::JFS_PREFIX("jfs://");

const string HdfsFileSystem::KV_SEPARATOR("=");
const string HdfsFileSystem::KV_PAIR_SEPARATOR("&");
const string HdfsFileSystem::CONFIG_SEPARATOR("?");
const string HdfsFileSystem::HDFS_BUFFER_SIZE("buf");
const string HdfsFileSystem::HDFS_REPLICA("rep");
const string HdfsFileSystem::HDFS_BLOCK_SIZE("blk");
const string HdfsFileSystem::HDFS_USER("user");
const string HdfsFileSystem::HDFS_GROUPS("groups");
const string HdfsFileSystem::HDFS_GROUP_SEPARATOR(",");

HdfsFileSystem::HdfsFileSystem()
    : _panguMonitor(kmonitor_adapter::MonitorFactory::getInstance()->createMonitor("fslib")) {

    if (!autil::EnvUtil::hasEnv("LIBHDFS_OPTS")) {
        autil::EnvUtil::setEnv("LIBHDFS_OPTS", "-Xrs", false);
    }
    if (!autil::EnvUtil::hasEnv("JAVA_TOOL_OPTIONS")) {
        autil::EnvUtil::setEnv("JAVA_TOOL_OPTIONS", "-Djdk.lang.processReaperUseDefaultStackSize=true", false);
    }
    initClassPath();
}

HdfsFileSystem::~HdfsFileSystem() {}

bool HdfsFileSystem::eliminateSeqSlash(const char *src, size_t len, string &dst, bool keepLastSlash) {
    int slashNum = 0;
    bool flag = false;
    for (size_t i = 0; i < len; i++) {
        if (src[i] == '/') {
            if (flag == false) {
                flag = true;
                dst.append(1, src[i]);
                slashNum++;
            } else {
                continue;
            }
        } else {
            flag = false;
            dst.append(1, src[i]);
        }
    }

    if (*dst.rbegin() == '/' && slashNum > 1 && !keepLastSlash) {
        dst.resize(dst.size() - 1);
    }

    return slashNum > 0;
}

bool HdfsFileSystem::parsePath(const string &fullPath, string &filePath, HdfsFsConfig &config) {
    // string path = HDFS_PREFIX + "localhost:10"
    //   + CONFIG_SEPARATOR + "blk=1&rep=2&buf=5/home/test.txt";
    string dfsPrefix = HdfsFileSystem::HDFS_PREFIX;
    string subString = fullPath.substr(0, HdfsFileSystem::HDFS_PREFIX.size());
    if (subString != dfsPrefix) {
        string subJfsString = fullPath.substr(0, HdfsFileSystem::JFS_PREFIX.size());
        if (HdfsFileSystem::JFS_PREFIX == subJfsString) {
            dfsPrefix = HdfsFileSystem::JFS_PREFIX;
        } else {
            dfsPrefix = HdfsFileSystem::DFS_PREFIX;
        }
    }
    config._prefix = dfsPrefix;
    filePath.reserve(fullPath.size());
    filePath.resize(0);
    size_t prefixLen = dfsPrefix.size();
    size_t beginPosForFileName = fullPath.find('/', prefixLen);
    if (beginPosForFileName == string::npos) {
        AUTIL_LOG(ERROR,
                  "parse hdfs config fail for path %s which "
                  "is an invalid path",
                  fullPath.c_str());
        return false;
    }

    string configStr;
    configStr.reserve(beginPosForFileName);
    configStr.append(fullPath.c_str(), beginPosForFileName);
    size_t beginPosForConfig = configStr.find(HdfsFileSystem::CONFIG_SEPARATOR.c_str(), prefixLen);
    configStr.resize(0);
    if (beginPosForConfig != string::npos) {
        configStr.append(fullPath.c_str() + beginPosForConfig + 1, beginPosForFileName - beginPosForConfig - 1);
        filePath.append(fullPath.c_str() + prefixLen, beginPosForConfig - prefixLen);
    } else {
        filePath.append(fullPath.c_str() + prefixLen, beginPosForFileName - prefixLen);
    }

    if (!eliminateSeqSlash(
            fullPath.c_str() + beginPosForFileName, fullPath.size() - beginPosForFileName, filePath, true)) {
        AUTIL_LOG(ERROR,
                  "parse hdfs config fail for path %s which "
                  "is an invalid path",
                  fullPath.c_str());
        return false;
    }
    // parse config
    config._bufferSize = 0;
    config._blockSize = 0;
    config._replica = 0;
    config._user = NULL;
    config._groups = NULL;
    config._groupNum = 0;

    return parseConfig(configStr, config);
}

bool HdfsFileSystem::parseConfig(const string &configStr, HdfsFsConfig &config) {
    vector<string> confVec = StringUtil::split(configStr, HdfsFileSystem::KV_PAIR_SEPARATOR);
    for (uint32_t i = 0; i < confVec.size(); i++) {
        vector<string> subConfVec = StringUtil::split(confVec[i], HdfsFileSystem::KV_SEPARATOR);
        if (subConfVec.size() != 2) {
            AUTIL_LOG(ERROR,
                      "parse HdfsFsConfig string <%s> fail! "
                      "parse key value pair error",
                      configStr.c_str());
            return false;
        }
        switch (subConfVec[0][0]) {
        case 'b': {
            if (strcmp(subConfVec[0].c_str(), HdfsFileSystem::HDFS_BUFFER_SIZE.c_str()) == 0) {
                if (!StringUtil::strToInt32(subConfVec[1].c_str(), config._bufferSize)) {
                    AUTIL_LOG(ERROR,
                              "parse HdfsFsConfig string <%s> fail "
                              "while parsing buffer size, convert string to "
                              "int32 error",
                              configStr.c_str());
                    return false;
                } else {
                    continue;
                }
            } else if (strcmp(subConfVec[0].c_str(), HdfsFileSystem::HDFS_BLOCK_SIZE.c_str()) == 0) {
                if (!StringUtil::strToInt32(subConfVec[1].c_str(), config._blockSize)) {
                    AUTIL_LOG(ERROR,
                              "parse HdfsFsConfig string <%s> fail "
                              "while parsing block size, convert string to int32 "
                              "error",
                              configStr.c_str());
                    return false;
                } else {
                    continue;
                }
            }
            break;
        }
        case 'r': {
            if (strcmp(subConfVec[0].c_str(), HdfsFileSystem::HDFS_REPLICA.c_str()) == 0) {
                if (!StringUtil::strToInt16(subConfVec[1].c_str(), config._replica)) {
                    AUTIL_LOG(ERROR,
                              "parse HdfsFsConfig string <%s> fail "
                              "while parsing replica, convert string to int16"
                              " error",
                              configStr.c_str());
                    return false;
                } else {
                    continue;
                }
            }
            break;
        }
        case 'u': {
            if (strcmp(subConfVec[0].c_str(), HdfsFileSystem::HDFS_USER.c_str()) == 0) {
                config._user = new char[subConfVec[1].size() + 1];
                strcpy(config._user, subConfVec[1].c_str());
                continue;
            }
            break;
        }
        case 'g': {
            if (strcmp(subConfVec[0].c_str(), HdfsFileSystem::HDFS_GROUPS.c_str()) == 0) {
                vector<string> groupVec = StringUtil::split(subConfVec[1], HdfsFileSystem::HDFS_GROUP_SEPARATOR);
                config._groups = new const char *[groupVec.size()];
                config._groupNum = groupVec.size();
                for (size_t i = 0; i < groupVec.size(); i++) {
                    char *group = new char[groupVec[i].size() + 1];
                    strcpy(group, groupVec[i].c_str());
                    config._groups[i] = group;
                }
                continue;
            }
            break;
        }
        case 'f': {
            if (subConfVec[0] == "file_storage_type" || subConfVec[0] == "fst") {
                continue;
            }
            break;
        }
        default:
            break;
        } // switch

        AUTIL_LOG(WARN,
                  "parse HadoopFsConfig string <%s> fail! "
                  "not support config: %s",
                  configStr.c_str(),
                  subConfVec[0].c_str());
    } // for

#if defined HADOOP_CDH
    return true;
#else
    if ((config._user && config_groups && config._groupNum > 0) ||
        (config._user == NULL && config._groups == NULL && config._groupNum == 0)) {
        return true;
    }

    AUTIL_LOG(ERROR,
              "parse HdfsFsConfig string <%s> fail! user and groups "
              "should either both be configured or both not be configured",
              configStr.c_str());
    return false;
#endif
}

File *HdfsFileSystem::openFile(const string &fileName, Flag flag) {
    HdfsConnectionPtr hdfsConnection;
    string dstPath;
    HdfsFsConfig fsConfig;
    ErrorCode ec = internalGetConnectionAndPath(fileName, fsConfig, hdfsConnection, dstPath);
    if (ec != EC_OK) {
        return new HdfsFile(fileName, dstPath, hdfsConnection, NULL, NULL, 0, ec);
    }

    int flags;
    int64_t curPos = 0;

    flag = (Flag)(flag & FLAG_CMD_MASK);

    switch (flag) {
    case READ: {
        flags = O_RDONLY;
        break;
    }
    case WRITE: {
        flags = O_WRONLY;
        break;
    }
    case APPEND: {
#if defined HADOOP_CDH
        AUTIL_LOG(TRACE1, "defined HADOOP_CDH");
        ErrorCode ec = internalIsExist(hdfsConnection, dstPath);

        if (ec == EC_TRUE) {
            flags = O_WRONLY | O_APPEND;
        } else if (ec == EC_FALSE) {
            flags = O_WRONLY;
        } else {
            AUTIL_LOG(ERROR, "check file exist fail, file: %s", fileName.c_str());
            return new HdfsFile(fileName, dstPath, hdfsConnection, NULL, NULL, 0, ec);
        }
#else
        AUTIL_LOG(TRACE1, "not defined HADOOP_CDH");
        AUTIL_LOG(ERROR,
                  "openFile <%s> fail, hdfs currently does not "
                  "support append",
                  fileName.c_str());
        return new HdfsFile(fileName, dstPath, hdfsConnection, NULL, NULL, 0, EC_NOTSUP);
#endif
        break;
    }
    default:
        AUTIL_LOG(ERROR, "openFile <%s> fail, flag %d not support", fileName.c_str(), flag);
        return new HdfsFile(fileName, dstPath, hdfsConnection, NULL, NULL, 0, EC_NOTSUP);
    }

    hdfsFile file = hdfsOpenFile(hdfsConnection->getHdfsFs(),
                                 dstPath.c_str(),
                                 flags,
                                 fsConfig._bufferSize,
                                 fsConfig._replica,
                                 fsConfig._blockSize);
    if (flag == APPEND || !file) {
        bool isDir = false;
        bool fileInfoExist = false;

        hdfsFileInfo *fileInfo = hdfsGetPathInfo(hdfsConnection->getHdfsFs(), dstPath.c_str());
        if (fileInfo) {
            isDir = (fileInfo->mKind == kObjectKindDirectory);
            curPos = fileInfo->mSize;
            hdfsFreeFileInfo(fileInfo, 1);
            fileInfoExist = true;
        }

        if (!fileInfoExist) {
            AUTIL_LOG(ERROR, "get info of file <%s> fail, %s, %d", fileName.c_str(), strerror(errno), errno);
            return new HdfsFile(fileName, dstPath, hdfsConnection, NULL, NULL, 0, convertErrno(errno));
        }

        if (isDir) {
            AUTIL_LOG(ERROR, "openFile <%s> fail, cannot open directory", fileName.c_str());
            return new HdfsFile(fileName, dstPath, hdfsConnection, NULL, NULL, 0, EC_ISDIR);
        }
    }

    if (!file) {
        AUTIL_LOG(ERROR, "hdfsOpenFile <%s> fail, %s, %d", fileName.c_str(), strerror(errno), errno);
        if (errno == EINTERNAL) {
            hdfsConnection->setError(true);
        }
        return new HdfsFile(fileName, dstPath, hdfsConnection, NULL, NULL, 0, convertErrno(errno));
    }
    return new HdfsFile(fileName, dstPath, hdfsConnection, file, &_panguMonitor, curPos);
}

MMapFile *HdfsFileSystem::mmapFile(
    const string &fileName, Flag openMode, char *start, size_t length, int prot, int mapFlag, off_t offset) {
    return new MMapFile(fileName, -1, NULL, -1, -1, EC_NOTSUP);
}

ErrorCode HdfsFileSystem::rename(const string &oldPath, const string &newPath) {
    string oldServer;
    string oldDstPath;
    string oldPathWithoutConfig;
    HdfsFsConfig oldFsConfig;
    string newServer;
    string newDstPath;
    string newPathWithoutConfig;
    HdfsFsConfig newFsConfig;

    if (!parsePath(oldPath, oldPathWithoutConfig, oldFsConfig)) {
        return EC_PARSEFAIL;
    }
    ErrorCode ec = splitServerAndPath(oldPathWithoutConfig, oldFsConfig._prefix, oldServer, oldDstPath);
    if (ec != EC_OK) {
        return ec;
    }

    if (!parsePath(newPath, newPathWithoutConfig, newFsConfig)) {
        return EC_PARSEFAIL;
    }
    ec = splitServerAndPath(newPathWithoutConfig, newFsConfig._prefix, newServer, newDstPath);
    if (ec != EC_OK) {
        return ec;
    }

    HdfsConnectionPtr oldConnection;
    HdfsConnectionPtr newConnection;
    string oldUser;
    string newUser;
    bool isSameHdfs = true;
    getConnectUser(oldFsConfig, oldUser);
    getConnectUser(newFsConfig, newUser);
    if (oldServer == newServer && oldUser == newUser) {
        ec = connectServer(oldServer, oldFsConfig, oldConnection);
        if (ec != EC_OK) {
            return ec;
        }

        newConnection = oldConnection;
    } else {
        isSameHdfs = false;
        ec = connectServer(oldServer, oldFsConfig, oldConnection);
        if (ec != EC_OK) {
            return ec;
        }

        ec = connectServer(newServer, newFsConfig, newConnection);
        if (ec != EC_OK) {
            return ec;
        }
    }

    hdfsFileInfo *fileInfo = hdfsGetPathInfo(oldConnection->getHdfsFs(), oldDstPath.c_str());
    if (!fileInfo) {
        AUTIL_LOG(ERROR,
                  "hdfs rename path <%s> to path <%s> fail, fail to get"
                  "src path info, %s",
                  oldPath.c_str(),
                  newPath.c_str(),
                  strerror(errno));
        if (errno == EINTERNAL) {
            oldConnection->setError(true);
        }
        return convertErrno(errno);
    }

    bool isFile = (fileInfo->mKind == kObjectKindFile);
    hdfsFreeFileInfo(fileInfo, 1);

    if (hdfsExists(newConnection->getHdfsFs(), newDstPath.c_str()) == 0) {
        AUTIL_LOG(ERROR,
                  "hdfs rename path <%s> to path <%s> fail, src path"
                  "already exist",
                  oldPath.c_str(),
                  newPath.c_str());
        return EC_EXIST;
    }
    if (errno == EINTERNAL) {
        newConnection->setError(true);
    }

    if (newDstPath[newDstPath.size() - 1] == '/' && isFile == true) {
        AUTIL_LOG(ERROR,
                  "hdfs rename path <%s> to path <%s> fail, src path is"
                  "not a directory",
                  oldPath.c_str(),
                  newPath.c_str());
        return EC_NOTDIR;
    }

    size_t pos = newDstPath.rfind('/');
    if (pos != string::npos) {
        if (pos == newDstPath.size() - 1) {
            if (pos == 0) {
                AUTIL_LOG(ERROR,
                          "hdfs rename path <%s> to path <%s> fail,"
                          "dst path should not be /",
                          oldPath.c_str(),
                          newPath.c_str());
                return EC_NOTSUP;
            }
            pos = newDstPath.rfind('/', pos - 1);
        }

        if (pos != 0) {
            string parentDir = newDstPath.substr(0, pos);
            if (hdfsExists(newConnection->getHdfsFs(), parentDir.c_str()) != 0 &&
                hdfsCreateDirectory(newConnection->getHdfsFs(), parentDir.c_str()) < 0) {
                AUTIL_LOG(ERROR,
                          "hdfs rename path <%s> to path <%s> fail, create"
                          " parent dir of src path fail, %s.",
                          oldPath.c_str(),
                          newPath.c_str(),
                          strerror(errno));
                return convertErrno(errno);
            }
        }
    } else {
        AUTIL_LOG(ERROR, "hdfs rename path <%s> to path <%s> fail, invalid src path", oldPath.c_str(), newPath.c_str());
        return EC_NOENT;
    }

    if (isSameHdfs) {
        if (hdfsRename(oldConnection->getHdfsFs(), oldDstPath.c_str(), newDstPath.c_str()) < 0) {
            AUTIL_LOG(ERROR,
                      "hdfs rename path <%s> to path <%s> fail, %s",
                      oldPath.c_str(),
                      newPath.c_str(),
                      strerror(errno));
            ec = convertErrno(errno);
        }

        return ec;
    } else {
        if (hdfsMove(oldConnection->getHdfsFs(), oldDstPath.c_str(), newConnection->getHdfsFs(), newDstPath.c_str()) <
            0) {
            AUTIL_LOG(
                ERROR, "hdfsMove path <%s> to path <%s> fail, %s", oldPath.c_str(), newPath.c_str(), strerror(errno));
            ec = convertErrno(errno);
        }

        return ec;
    }
}

ErrorCode HdfsFileSystem::link(const string &oldPath, const string &newPath) { return EC_NOTSUP; }

ErrorCode HdfsFileSystem::getFileMeta(const string &fileName, FileMeta &fileMeta) {
    PathMeta pathMeta;
    ErrorCode ret = getPathMeta(fileName, pathMeta);
    if (ret == EC_OK) {
        fileMeta.fileLength = pathMeta.length;
        fileMeta.lastModifyTime = pathMeta.lastModifyTime;
        fileMeta.createTime = pathMeta.createTime;
    }
    return ret;
}

ErrorCode HdfsFileSystem::getPathMeta(const std::string &path, PathMeta &pathMeta) {
    HdfsConnectionPtr hdfsConnection;
    string dstPath;
    HdfsFsConfig fsConfig;
    ErrorCode ec = internalGetConnectionAndPath(path, fsConfig, hdfsConnection, dstPath);
    if (ec != EC_OK) {
        return ec;
    }

    hdfsFileInfo *fileInfo = hdfsGetPathInfo(hdfsConnection->getHdfsFs(), dstPath.c_str());
    if (!fileInfo) {
        AUTIL_LOG(ERROR, "hdfsGetPathInfo of <%s> fail, %s", path.c_str(), strerror(errno));
        if (errno == EINTERNAL) {
            hdfsConnection->setError(true);
        }
        return convertErrno(errno);
    }

    pathMeta.isFile = (fileInfo->mKind == kObjectKindFile);
    pathMeta.length = (int64_t)fileInfo->mSize;
    pathMeta.createTime = (DateTime)fileInfo->mLastAccess;
    pathMeta.lastModifyTime = (DateTime)fileInfo->mLastMod;

    hdfsFreeFileInfo(fileInfo, 1);
    return EC_OK;
}

ErrorCode HdfsFileSystem::isFile(const string &path) {
    HdfsConnectionPtr hdfsConnection;
    string dstPath;
    HdfsFsConfig fsConfig;
    ErrorCode ec = internalGetConnectionAndPath(path, fsConfig, hdfsConnection, dstPath);
    if (ec != EC_OK) {
        return ec;
    }

    hdfsFileInfo *fileInfo = hdfsGetPathInfo(hdfsConnection->getHdfsFs(), dstPath.c_str());
    if (!fileInfo) {
        AUTIL_LOG(DEBUG, "hdfsGetPathInfo of file <%s> fail, %s", path.c_str(), strerror(errno));
        if (errno == EINTERNAL) {
            hdfsConnection->setError(true);
        }
        return convertErrno(errno);
    }

    bool ret = fileInfo->mKind == kObjectKindFile;
    hdfsFreeFileInfo(fileInfo, 1);

    if (ret) {
        return EC_TRUE;
    } else {
        return EC_FALSE;
    }
}

FileChecksum HdfsFileSystem::getFileChecksum(const string &fileName) { return 0; }

ErrorCode HdfsFileSystem::mkDir(const string &dirName, bool recursive) {
    HdfsConnectionPtr hdfsConnection;
    string dstDir;
    HdfsFsConfig fsConfig;
    ErrorCode ec = internalGetConnectionAndPath(dirName, fsConfig, hdfsConnection, dstDir);
    if (ec != EC_OK) {
        return ec;
    }

    if (hdfsExists(hdfsConnection->getHdfsFs(), dstDir.c_str()) == 0) {
        AUTIL_LOG(ERROR,
                  "hdfsCreateDirectory of dir <%s> fail, path with the "
                  "same name already exist. ",
                  dstDir.c_str());
        return EC_EXIST;
    }

    // hdfsExist return -1, check connection loss or file not exist
    if (errno == EINTERNAL) {
        hdfsConnection->setError(true);
        return convertErrno(errno);
    }

    int ret = hdfsCreateDirectory(hdfsConnection->getHdfsFs(), dstDir.c_str());
    if (ret < 0) {
        AUTIL_LOG(ERROR,
                  "hdfsCreateDirectory of dir <%s> fail, %s, address %p",
                  dirName.c_str(),
                  strerror(errno),
                  hdfsConnection->getHdfsFs());
        ec = convertErrno(errno);
    }

    return ec;
}

ErrorCode HdfsFileSystem::listDir(const string &dirName, FileList &fileList) {
    EntryList entryList;
    ErrorCode ec = listDir(dirName, entryList);
    if (ec != EC_OK) {
        return ec;
    }
    fileList.resize(entryList.size());
    for (size_t i = 0; i < entryList.size(); ++i) {
        fileList[i] = entryList[i].entryName;
    }
    return EC_OK;
}

ErrorCode HdfsFileSystem::listDir(const string &dirName, EntryList &entryList) {
    HdfsConnectionPtr hdfsConnection;
    string dstDir;
    HdfsFsConfig fsConfig;
    ErrorCode ec = internalGetConnectionAndPath(dirName, fsConfig, hdfsConnection, dstDir);
    if (ec != EC_OK) {
        return ec;
    }

    hdfsFileInfo *fileInfo = hdfsGetPathInfo(hdfsConnection->getHdfsFs(), dstDir.c_str());
    if (!fileInfo) {
        AUTIL_LOG(ERROR, "hdfsListDirectory of dir <%s> fail, %s", dirName.c_str(), strerror(errno));
        if (errno == EINTERNAL) {
            hdfsConnection->setError(true);
        }
        return convertErrno(errno);
    }

    bool isDir = (fileInfo->mKind == kObjectKindDirectory);
    hdfsFreeFileInfo(fileInfo, 1);
    if (!isDir) {
        AUTIL_LOG(ERROR,
                  "hdfsListDirectory of path <%s> fail, path is not"
                  " a directory",
                  dirName.c_str());
        return EC_NOTDIR;
    }

    int fileNum = -1;
    hdfsFileInfo *fileInfos = hdfsListDirectory(hdfsConnection->getHdfsFs(), dstDir.c_str(), &fileNum);

    entryList.clear();
    // hdfs bug: if dir is empty, then fileInfos == NULL, but fileNum set to 0
    if (!fileInfos && fileNum != 0) {
        AUTIL_LOG(ERROR, "hdfsListDirectory of dir <%s> fail, %s", dirName.c_str(), strerror(errno));
        return convertErrno(errno);
    }

    entryList.reserve(fileNum);
    for (int i = 0; i < fileNum; i++) {
        string fullName(fileInfos[i].mName);
        EntryMeta entry;
        entry.isDir = fileInfos[i].mKind == kObjectKindDirectory;
        entry.entryName = fullName.substr(fullName.rfind('/') + 1);
        entryList.push_back(entry);
    }

    hdfsFreeFileInfo(fileInfos, fileNum);
    return EC_OK;
}

ErrorCode HdfsFileSystem::isDirectory(const string &path) {
    HdfsConnectionPtr hdfsConnection;
    string dstPath;
    HdfsFsConfig fsConfig;
    ErrorCode ec = internalGetConnectionAndPath(path, fsConfig, hdfsConnection, dstPath);
    if (ec != EC_OK) {
        return ec;
    }

    hdfsFileInfo *fileInfo = hdfsGetPathInfo(hdfsConnection->getHdfsFs(), dstPath.c_str());
    if (!fileInfo) {
        AUTIL_LOG(DEBUG, "hdfsGetPathInfo of file <%s> fail, %s", path.c_str(), strerror(errno));
        if (errno == EINTERNAL) {
            hdfsConnection->setError(true);
        }
        return convertErrno(errno);
    }

    bool ret = (fileInfo->mKind == kObjectKindDirectory);
    hdfsFreeFileInfo(fileInfo, 1);

    if (ret) {
        return EC_TRUE;
    } else {
        return EC_FALSE;
    }
}

ErrorCode HdfsFileSystem::listDir(const string &dirName, RichFileList &fileList) {
    HdfsConnectionPtr hdfsConnection;
    string dstDir;
    HdfsFsConfig fsConfig;
    ErrorCode ec = internalGetConnectionAndPath(dirName, fsConfig, hdfsConnection, dstDir);
    if (ec != EC_OK) {
        return ec;
    }

    hdfsFileInfo *fileInfo = hdfsGetPathInfo(hdfsConnection->getHdfsFs(), dstDir.c_str());
    if (!fileInfo) {
        AUTIL_LOG(ERROR, "hdfsListDirectory of dir <%s> fail, %s", dirName.c_str(), strerror(errno));
        if (errno == EINTERNAL) {
            hdfsConnection->setError(true);
        }
        return convertErrno(errno);
    }

    bool isDir = (fileInfo->mKind == kObjectKindDirectory);
    hdfsFreeFileInfo(fileInfo, 1);
    if (!isDir) {
        AUTIL_LOG(ERROR,
                  "hdfsListDirectory of path <%s> fail, path is not"
                  " a directory",
                  dirName.c_str());
        return EC_NOTDIR;
    }

    int fileNum = -1;
    hdfsFileInfo *fileInfos = hdfsListDirectory(hdfsConnection->getHdfsFs(), dstDir.c_str(), &fileNum);

    fileList.clear();
    // hdfs bug: if dir is empty, then fileInfos == NULL, but fileNum set to 0
    if (!fileInfos && fileNum != 0) {
        AUTIL_LOG(ERROR, "hdfsListDirectory of dir <%s> fail, %s", dirName.c_str(), strerror(errno));
        return convertErrno(errno);
    }

    fileList.reserve(fileNum);
    for (int i = 0; i < fileNum; i++) {
        string fullName(fileInfos[i].mName);
        RichFileMeta meta;
        meta.path = fullName.substr(fullName.rfind('/') + 1);
        meta.size = fileInfos[i].mSize;
        meta.physicalSize = fileInfos[i].mSize * fileInfos[i].mReplication;
        meta.createTime = (DateTime)fileInfos[i].mLastAccess;
        meta.lastModifyTime = (DateTime)fileInfos[i].mLastMod;
        meta.isDir = fileInfos[i].mKind == kObjectKindDirectory;
        fileList.push_back(meta);
    }

    hdfsFreeFileInfo(fileInfos, fileNum);
    return EC_OK;
}

ErrorCode HdfsFileSystem::remove(const string &path) {
    HdfsConnectionPtr hdfsConnection;
    string dstPath;
    HdfsFsConfig fsConfig;
    ErrorCode ec = internalGetConnectionAndPath(path, fsConfig, hdfsConnection, dstPath);
    if (ec != EC_OK) {
        return ec;
    }
    ec = internalIsExist(hdfsConnection, dstPath);
    if (ec == EC_FALSE) {
        ec = EC_NOENT;
    }
    if (ec != EC_TRUE) {
        return ec;
    }
    int ret = hdfsDelete(hdfsConnection->getHdfsFs(), dstPath.c_str(), 1);
    if (ret == 0) {
        ec = EC_OK;
    } else if (ret == 1) {
        AUTIL_LOG(ERROR, "hdfsDelete path <%s> fail, path does not exist", path.c_str());
        ec = EC_NOENT;
    } else {
        AUTIL_LOG(ERROR, "hdfsDelete path <%s> fail, %s", path.c_str(), strerror(errno));
        ec = convertErrno(errno);
    }

    return ec;
}

ErrorCode HdfsFileSystem::isExist(const string &path) {
    HdfsConnectionPtr hdfsConnection;
    string dstPath;
    HdfsFsConfig fsConfig;
    ErrorCode ec = internalGetConnectionAndPath(path, fsConfig, hdfsConnection, dstPath);
    if (ec != EC_OK) {
        return ec;
    }

    return internalIsExist(hdfsConnection, dstPath);
}

ErrorCode HdfsFileSystem::internalIsExist(HdfsConnectionPtr hdfsConnection, const string &dstPath) {
    int ret = hdfsExists(hdfsConnection->getHdfsFs(), dstPath.c_str());
    if (ret == 0) {
        return EC_TRUE;
    } else if (convertErrno(errno) == EC_NOENT || ret == 1) {
        return EC_FALSE;
    } else {
        AUTIL_LOG(ERROR, "path exist <%s> fail, %s", dstPath.c_str(), strerror(errno));
        if (errno == EINTERNAL) {
            hdfsConnection->setError(true);
        }
        return convertErrno(errno);
    }
}

ErrorCode
HdfsFileSystem::splitServerAndPath(const string &srcPath, const string &prefix, string &server, string &dstPath) {
    size_t pos = srcPath.find('/', 0);
    if (pos == string::npos) {
        AUTIL_LOG(ERROR, "cannot find / between servername and pathname for %s", srcPath.c_str());
        return EC_PARSEFAIL;
    }

    server = prefix + srcPath.substr(0, pos);
    dstPath = srcPath.substr(pos);

    return EC_OK;
}

HdfsConnectionPtr HdfsFileSystem::getHdfsConnection(const std::string &username) {
    map<string, HdfsConnectionPtr>::iterator it;
    ScopedReadWriteLock l(_rwLock, 'r');
    it = _connectionMap.find(username);
    if (it != _connectionMap.end()) {
        return it->second;
    }
    return HdfsConnectionPtr();
}

ErrorCode
HdfsFileSystem::connectServer(const string &server, const HdfsFsConfig &fsConfig, HdfsConnectionPtr &hdfsConnection) {
    string username;
    getConnectUser(fsConfig, username);
    username.append(1, '@');
    username.append(server);

    hdfsConnection = getHdfsConnection(username);

    if (hdfsConnection != NULL && !hdfsConnection->hasError()) {
        return EC_OK;
    } else if (hdfsConnection != NULL) {
        ScopedReadWriteLock l(_rwLock, 'w');
        AUTIL_LOG(ERROR, "reset hdfs connection [%s] for error!", username.c_str());
        _connectionMap.erase(username);
    }

    size_t pos = server.find(":", fsConfig._prefix.size());
    uint16_t port = 0;
    // do not parse port for HA proxy
    if (pos != string::npos) {
        if (!StringUtil::strToUInt16(server.substr(pos + 1).c_str(), port)) {
            AUTIL_LOG(ERROR, "cannot convert port %s to int16_t value fail", server.substr(pos + 1).c_str());
            return EC_PARSEFAIL;
        }
    }
    hdfsConnection = getHdfsConnection(username);
    if (hdfsConnection != NULL && !hdfsConnection->hasError()) {
        return EC_OK;
    }
    ScopedReadWriteLock l(_rwLock, 'w');
    if (hdfsConnection != NULL && hdfsConnection->hasError()) {
        AUTIL_LOG(ERROR, "reset hdfs connection [%s] for error!", username.c_str());
        _connectionMap.erase(username);
    }

    hdfsFS hdfsFs;
#if defined HADOOP_CDH
    if (fsConfig._user) {
        hdfsFs = hdfsConnectAsUserNewInstance(server.substr(0, pos).c_str(), (tPort)port, fsConfig._user);
#else
    const char **groups = fsConfig._groups;
    if (fsConfig._user && fsConfig._groups && fsConfig._groupNum > 0) {
        hdfsFs = hdfsConnectAsUserNewInstance(
            server.substr(0, pos).c_str(), (tPort)port, fsConfig._user, groups, fsConfig._groupNum);
#endif
    } else {
        hdfsFs = hdfsConnectNewInstance(server.substr(0, pos).c_str(), (tPort)port);
    }

    if (!hdfsFs) {
        AUTIL_LOG(ERROR, "fail to connect to hdfs, %s", strerror(errno));
        return convertErrno(errno);
    }

    hdfsConnection.reset(new HdfsConnection(hdfsFs));
    if (_connectionMap.size() >= MAX_CONNECTION) {
        map<string, HdfsConnectionPtr>::iterator itToDel;
        map<string, HdfsConnectionPtr>::iterator it;
        it = _connectionMap.begin();
        while (it != _connectionMap.end()) {
            if (it->second.use_count() == 1) {
                itToDel = it;
                it++;
                AUTIL_LOG(DEBUG, "erase connection with key %s from pool, address %p", itToDel->first.c_str(), this);
                _connectionMap.erase(itToDel);
            } else {
                it++;
            }
        }
    }
    _connectionMap.insert(pair<string, HdfsConnectionPtr>(username, hdfsConnection));
    AUTIL_LOG(DEBUG,
              "Add connection %p with key %s into pool with address %p",
              hdfsConnection->getHdfsFs(),
              username.c_str(),
              this);
    return EC_OK;
}

ErrorCode HdfsFileSystem::internalGetConnectionAndPath(const string &srcPath,
                                                       HdfsFsConfig &config,
                                                       HdfsConnectionPtr &hdfsConnection,
                                                       string &dstPath) {
    string server;
    string pathWithoutConfig;
    if (!parsePath(srcPath, pathWithoutConfig, config)) {
        return EC_PARSEFAIL;
    }
    ErrorCode ret = splitServerAndPath(pathWithoutConfig, config._prefix, server, dstPath);
    if (ret != EC_OK) {
        return ret;
    }

    return connectServer(server, config, hdfsConnection);
}

void HdfsFileSystem::getConnectUser(const HdfsFsConfig &fsConfig, string &username) {
    if (fsConfig._user) {
        username.append(fsConfig._user);
    } else {
        username.append("");
    }
}

ErrorCode HdfsFileSystem::convertErrno(int errNum) {
    switch (errNum) {
    case ENOENT:
        return EC_NOENT;
    case EBADF:
    case EINVAL:
        return EC_BADF;
    case 255:
        return EC_UNKNOWN;
    default:
        AUTIL_LOG(ERROR, "unknown errNum: %d", errNum);
        return EC_UNKNOWN;
    }
}

void HdfsFileSystem::initClassPath() {
    string hadoopClassPath;
    if (getHadoopClassPath(hadoopClassPath)) {
        setHadoopClassPath(hadoopClassPath);
        return;
    }

    string hadoopHome;
    getHadoopHome(hadoopHome);
    if (hadoopHome.empty()) {
        AUTIL_LOG(WARN, "HADOOP_HOME is not set or empty!");
        return;
    }

    genHadoopClassPath(hadoopHome, hadoopClassPath);
    setHadoopClassPath(hadoopClassPath);
}

bool HdfsFileSystem::getHadoopClassPath(string &hadoopClassPath) {
    return autil::EnvUtil::getEnvWithoutDefault(HADOOP_CLASSPATH_ENV, hadoopClassPath);
}

void HdfsFileSystem::getHadoopHome(string &hadoopHome) {
    if (!autil::EnvUtil::getEnvWithoutDefault(HADOOP_HOME_ENV, hadoopHome)) {
        hadoopHome = "";
    }
}

void HdfsFileSystem::genHadoopClassPath(const string &hadoopHome, string &hadoopClassPath) {
    string pattern = getHadoopClassPathPattern(hadoopHome);
    if (pattern.empty()) {
        AUTIL_LOG(ERROR, "get class path failed!");
        return;
    }
    extendHadoopClassPathPattern(pattern, hadoopClassPath);
}

void HdfsFileSystem::setHadoopClassPath(const string &hadoopClassPath) {
    if (hadoopClassPath.empty()) {
        return;
    }
    string classPath = autil::EnvUtil::getEnv(CLASSPATH_ENV);
    if (classPath.empty()) {
        autil::EnvUtil::setEnv(CLASSPATH_ENV, hadoopClassPath);
    } else if (classPath.find(hadoopClassPath) == string::npos) {
        classPath = hadoopClassPath + ":" + classPath;
        autil::EnvUtil::setEnv(CLASSPATH_ENV, classPath);
    }
}

string HdfsFileSystem::getHadoopClassPathPattern(const string &hadoopHome) {
    string hadoopBin = hadoopHome + "/bin/hadoop";
    if (access(hadoopBin.c_str(), F_OK) != 0) {
        AUTIL_LOG(ERROR, "hadoop bin not exist. path:[%s]", hadoopBin.c_str());
        return "";
    }

    string hadoopCmd = "LD_PRELOAD= " + hadoopBin + " classpath";
    FILE *stream = popen(hadoopCmd.c_str(), "r");
    if (stream == NULL) {
        AUTIL_LOG(ERROR,
                  "execute hadoop cmd [%s] failed. "
                  "errorno:[%d], errormsg:[%s].",
                  hadoopCmd.c_str(),
                  errno,
                  strerror(errno));
        return "";
    }

    string pattern;
    readStream(stream, &pattern);
    if (pattern.empty()) {
        AUTIL_LOG(ERROR, "empty hadoop classpath, hadoop cmd:[%s]", hadoopCmd.c_str());
    }
    pclose(stream);

    if (pattern.length() > 0 && *pattern.rbegin() == '\n') {
        return pattern.substr(0, pattern.length() - 1);
    }
    return pattern;
}

void HdfsFileSystem::readStream(FILE *stream, string *pattern) {
    char buf[256];
    while (true) {
        size_t len = fread(buf, sizeof(char), 256, stream);
        if (ferror(stream)) {
            AUTIL_LOG(ERROR, "read hadoop cmd stream failed. error:[%s]", strerror(errno));
            *pattern = "";
            return;
        }

        *pattern += string(buf, len);

        if (feof(stream)) {
            break;
        }
    }
}

void HdfsFileSystem::extendHadoopClassPathPattern(const string &pattern, string &hadoopClassPath) {
    size_t len = pattern.length();
    char *dataBuf = new char[len + 1];
    memcpy(dataBuf, pattern.c_str(), len);
    dataBuf[len] = '\0';
    vector<string> paths;
    size_t startPos = 0;
    for (size_t pos = 0; pos < len; pos++) {
        if (dataBuf[pos] == ':' || pos == len - 1) {
            if (dataBuf[pos] == ':') {
                dataBuf[pos] = '\0';
            }
            globPath(&dataBuf[startPos], &paths);
            startPos = pos + 1;
        }
    }

    for (size_t i = 0; i < paths.size(); i++) {
        hadoopClassPath += paths[i];
        if (i < paths.size() - 1) {
            hadoopClassPath += ":";
        }
    }
    delete[] dataBuf;
}

void HdfsFileSystem::globPath(const char *pathPattern, vector<string> *paths) {
    glob_t pglob;
    if (glob(pathPattern, GLOB_MARK, NULL, &pglob) == 0) {
        for (size_t i = 0; i < pglob.gl_pathc; i++) {
            int pathLen = strlen(pglob.gl_pathv[i]);
            if (pathLen > 0 && pglob.gl_pathv[i][pathLen - 1] != '/') {
                paths->push_back(string(pglob.gl_pathv[i]));
            }
        }
    }
    // reserve origin path
    paths->push_back(string(pathPattern));
    globfree(&pglob);
}

FSLIB_PLUGIN_END_NAMESPACE(fs);
