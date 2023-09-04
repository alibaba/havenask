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
#include <ctime>
#include <dirent.h>
#include <fnmatch.h>
#include <stdlib.h>
#include <unistd.h>

#include "autil/EnvUtil.h"
#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#ifdef ENABLE_BEEPER
#include "beeper/beeper.h"
#endif
#include "fslib/common/common_define.h"
#include "fslib/config.h"
#include "fslib/fs/FileSystemFactory.h"
#include "fslib/fs/ProxyFileSystem.h"
#include "fslib/fs/local/LocalFileSystem.h"

// #ifdef ZOOKEEPER_FILE_SYSTEM
// #include "fslib/fs/zookeeper/ZookeeperFileSystem.h"
// #endif
// #ifdef HTTP_FILE_SYSTEM
// #include "fslib/fs/http/HttpFileSystem.h"
// #endif

using namespace std;
using namespace autil;
FSLIB_BEGIN_NAMESPACE(fs);

AUTIL_DECLARE_AND_SETUP_LOGGER(fs, FileSystemFactory);

FileSystemFactory::FileSystemFactory() { init(); }

FileSystemFactory::~FileSystemFactory() { close(); }

void FileSystemFactory::close() {
    map<FsType, AbstractFileSystem *>::iterator it = _fsMap.begin();
    for (; it != _fsMap.end(); ++it) {
        destroyFs(it->first, it->second);
        it->second = NULL;
    }
    _fsMap.clear();

    map<FsType, ModuleInfo>::iterator mit = _fsModuleInfo.begin();
    for (; mit != _fsModuleInfo.end(); ++mit) {
        if (mit->second.dllWrapper != NULL) {
            mit->second.dllWrapper->dlclose();
            delete mit->second.dllWrapper;
            mit->second.dllWrapper = NULL;
        }
    }
    _fsModuleInfo.clear();
}

void FileSystemFactory::destroyFs(const FsType &fsType, AbstractFileSystem *fs) {
    if (!fs) {
        return;
    }
    ProxyFileSystem *proxyFs = dynamic_cast<ProxyFileSystem *>(fs);
    if (proxyFs) {
        fs = proxyFs->getFs();
        delete proxyFs;
    }
    if (isLocalFileSystem(fsType)) {
        delete fs;
        return;
    } else if (_fsModuleInfo.find(fsType) == _fsModuleInfo.end() || _fsModuleInfo[fsType].dllWrapper == NULL) {
        return;
    }
    DestroyFileSystemFun destroyFsFunction =
        (DestroyFileSystemFun)_fsModuleInfo[fsType].dllWrapper->dlsym(FSLIB_FS_DLL_DESTROY_FUNCTION_NAME);
    if (destroyFsFunction == NULL) {
        delete fs;
    } else {
        destroyFsFunction(fs);
    }
}

void FileSystemFactory::destroyFs(const FsType &fsType) {
    AbstractFileSystem *fs = NULL;
    {
        ScopedLock lock(_lock);
        fs = _fsMap[fsType];
        _fsMap.erase(fsType);
        _failTimestampMap.erase(fsType);
    }
    destroyFs(fsType, fs);
}

bool FileSystemFactory::isLocalFileSystem(const FsType &type) { return FSLIB_FS_LOCAL_FILESYSTEM_NAME == type; }

bool FileSystemFactory::isZookeeperFileSystem(const FsType &type) { return FSLIB_FS_ZFS_FILESYSTEM_NAME == type; }

bool FileSystemFactory::isHttpFileSystem(const FsType &type) { return FSLIB_FS_HTTP_FILESYSTEM_NAME == type; }

bool FileSystemFactory::isDfsFileSystem(const FsType &type) { return FSLIB_FS_DFS_FILESYSTEM_NAME == type; }

bool FileSystemFactory::isFslibPlugin(const string &fileName) {

    return StringUtil::startsWith(fileName, FSLIB_FS_LIBSO_NAME_PREFIX) &&
           (StringUtil::endsWith(fileName, ".so") || fileName.find(".so.") != string::npos);
}

bool FileSystemFactory::parsePlugin(const string &fileName, FsType &fileType, uint32_t &majorVersion) {
    if (!StringUtil::startsWith(fileName, FSLIB_FS_LIBSO_NAME_PREFIX)) {
        return false;
    }

    size_t found = fileName.find(".so");
    if (found == string::npos) {
        return false;
    }

    fileType = fileName.substr(strlen(FSLIB_FS_LIBSO_NAME_PREFIX), found - strlen(FSLIB_FS_LIBSO_NAME_PREFIX));
    if (fileType.length() == 0) {
        return false;
    }

    string version = fileName.substr(found + strlen(".so"));
    if (version == "") {
        majorVersion = FSLIB_FS_LIBSO_MAX_VERSION;
        return true;
    } else {
        if (!StringUtil::startsWith(version, ".")) {
            return false;
        } else {
            // +1 to ignore "."
            return StringUtil::strToUInt32(version.c_str() + 1, majorVersion);
        }
    }
}

bool FileSystemFactory::parseSoUnderOnePath(const string &path) {
    struct dirent **nameList;
    int n = 0;
    n = scandir(path.c_str(), &nameList, 0, alphasort);
    AUTIL_LOG(DEBUG, "scans fslib plugin under path: [%s]", path.c_str());
    if (n < 0) {
        AUTIL_LOG(DEBUG, "scan out none so under [%s]", path.c_str());
    } else {
        while (n--) {
            AUTIL_LOG(TRACE1, "find name :[%s]", nameList[n]->d_name);
            string soFileName = nameList[n]->d_name;
            free(nameList[n]);
            AUTIL_LOG(DEBUG, "[%s] find file[%s]", path.c_str(), soFileName.c_str());
            FsType type("");
            uint32_t version = 0;
            if (parsePlugin(soFileName, type, version)) {
                string fullPath = string(path) + '/' + soFileName;
                if (_fsModuleInfo.find(type) != _fsModuleInfo.end()) {
                    continue;
                }
                _fsModuleInfo[type].majorVersion = version;
                _fsModuleInfo[type].path = fullPath;
                _fsModuleInfo[type].dllWrapper = NULL;
                AUTIL_LOG(
                    INFO, "find fslib plugin[%s]:[%s], %u", type.c_str(), _fsModuleInfo[type].path.c_str(), version);
            }
        }
        free(nameList);
    }
    return true;
}

bool FileSystemFactory::init() {
    // add "./"
    string pathes = "./:./aios/filesystem/fslib/";
    string env = autil::EnvUtil::getEnv(FSLIB_FS_LIBSO_PATH_ENV_NAME);
    if (!env.empty()) {
        pathes += ":" + env;
    }

    char cwd[1024];
    if (getcwd(cwd, 1024)) {
        AUTIL_LOG(INFO, "cwd: [%s]", cwd);
    }
    AUTIL_LOG(INFO, "scan path: [%s]", pathes.c_str());
    StringUtil::trim(pathes);
    StringTokenizer tokens(pathes, ":", StringTokenizer::TOKEN_TRIM);
    StringTokenizer::Iterator it = tokens.begin();
    while (it != tokens.end()) {
        if (!parseSoUnderOnePath(*it)) {
            AUTIL_LOG(ERROR, "fail to scan out so under [%s]", (*it).c_str());
        }
        it++;
    }
    _retryInterval = autil::EnvUtil::getEnv(FSLIB_CREATE_FS_RETRY_INTERVAL, 10);
    _retryInterval = (_retryInterval == 0 ? 10 : _retryInterval);
    AUTIL_LOG(INFO, "retry interval:[%lf]", _retryInterval);
    return true;
}

AbstractFileSystem *FileSystemFactory::createFileSystem(const FsType &type) {
    if (_fsModuleInfo.find(type) == _fsModuleInfo.end()) {
        return NULL;
    }

    if (_fsModuleInfo[type].dllWrapper == NULL) {
        _fsModuleInfo[type].dllWrapper = new DllWrapper(_fsModuleInfo[type].path);
    }

    if (!_fsModuleInfo[type].dllWrapper->dlopen()) {
        AUTIL_LOG(ERROR,
                  "dlopen [%s] fail, error :[%s]",
                  _fsModuleInfo[type].path.c_str(),
                  _fsModuleInfo[type].dllWrapper->dlerror().c_str());
#ifdef ENABLE_BEEPER
        BEEPER_FORMAT_REPORT_WITHOUT_TAGS(FSLIB_ERROR_COLLECTOR_NAME,
                                          "dlopen [%s] fail, error :[%s]",
                                          _fsModuleInfo[type].path.c_str(),
                                          _fsModuleInfo[type].dllWrapper->dlerror().c_str());
#endif
        delete _fsModuleInfo[type].dllWrapper;
        _fsModuleInfo[type].dllWrapper = NULL;
        return NULL;
    }

    CreateFileSystemFun createFsFunction =
        (CreateFileSystemFun)_fsModuleInfo[type].dllWrapper->dlsym(FSLIB_FS_DLL_CREATE_FUNCTION_NAME);
    if (createFsFunction == NULL) {
        AUTIL_LOG(ERROR,
                  "dlsym 'create' from [%s] fail, error :[%s]",
                  _fsModuleInfo[type].path.c_str(),
                  _fsModuleInfo[type].dllWrapper->dlerror().c_str());
#ifdef ENABLE_BEEPER
        BEEPER_FORMAT_REPORT_WITHOUT_TAGS(FSLIB_ERROR_COLLECTOR_NAME,
                                          "dlsym 'create' from [%s] fail, error :[%s]",
                                          _fsModuleInfo[type].path.c_str(),
                                          _fsModuleInfo[type].dllWrapper->dlerror().c_str());
#endif
        delete _fsModuleInfo[type].dllWrapper;
        _fsModuleInfo[type].dllWrapper = NULL;
        return NULL;
    }
    return createFsFunction();
}

AbstractFileSystem *FileSystemFactory::createFs(const FsType &type) {
    if (_fsModuleInfo.find(type) == _fsModuleInfo.end()) {
        if (isLocalFileSystem(type)) {
            return new LocalFileSystem();
        } else if (isDfsFileSystem(type)) {
            return createDfsFileSystem();
            // #ifdef ZOOKEEPER_FILE_SYSTEM
            //         } else if (isZookeeperFileSystem(type)) {
            //             return new zookeeper::ZookeeperFileSystem();
            // #endif
            // #ifdef HTTP_FILE_SYSTEM
            //         } else if (isHttpFileSystem(type)) {
            //             return new http::HttpFileSystem();
            // #endif
        }
        AUTIL_LOG(WARN, "create fs with type:[%s] fail", type.c_str());
        return NULL;
    }

    AbstractFileSystem *fs = NULL;
    auto timestampIter = _failTimestampMap.find(type);
    if (timestampIter == _failTimestampMap.end()) {
        fs = createFileSystem(type);
        if (!fs) {
            AUTIL_LOG(WARN, "try create fs with type:[%s] fail", type.c_str());
            _failTimestampMap[type] = time(nullptr);
        }
    } else if (difftime(time(nullptr), timestampIter->second) > _retryInterval) {
        fs = createFileSystem(type);
        if (!fs) {
            AUTIL_LOG(WARN, "retry create fs with type:[%s] fail", type.c_str());
            _failTimestampMap[type] = time(nullptr);
        }
    }
    return fs;
}

AbstractFileSystem *FileSystemFactory::doGetFs(const FsType &type) {
    const auto &it = _fsMap.find(type);
    if (unlikely(it == _fsMap.end())) {
        AbstractFileSystem *fs = createFs(type);
        assert(dynamic_cast<ProxyFileSystem *>(fs) == NULL);
        if (fs) {
            AbstractFileSystem *proxyFs = fs;
            if (!autil::EnvUtil::getEnv("FSLIB_DISABLE_PROXY_FILE_SYSTEM", false)) {
                proxyFs = new ProxyFileSystem(fs);
            }
            _fsMap[type] = proxyFs;
            return proxyFs;
        }
        return NULL;
    }
    return it->second;
}

AbstractFileSystem *FileSystemFactory::getFs(const FsType &type) {
    AbstractFileSystem *fs = NULL;
    AUTIL_LOG(TRACE1, "try to get file system of type [%s]", type.c_str());
    {
        ScopedLock lock(_lock);
        fs = doGetFs(type);
    }
    if (unlikely(!fs)) {
        AUTIL_LOG(ERROR, "not support file system of type [%s]", type.c_str());
#ifdef ENABLE_BEEPER
        BEEPER_FORMAT_REPORT_WITHOUT_TAGS(
            FSLIB_ERROR_COLLECTOR_NAME, "not support file system of type [%s]", type.c_str());
#endif
    } else {
        AUTIL_LOG(DEBUG, "return file system[%p] for type [%s]", fs, type.c_str());
    }
    return fs;
}

AbstractFileSystem *FileSystemFactory::createDfsFileSystem() {
    // https://yuque.antfin-inc.com/docs/share/f3ecef36-31c1-49c9-9957-e63c9a8c85a8
    string mode = autil::EnvUtil::getEnv("FSLIB_DFS_STORAGE_MODE");
    if (!mode.empty()) {
        bool useHdfs = (mode == "hdfs");
        AUTIL_LOG(INFO, "env FSLIB_DFS_STORAGE_MODE set to [%s], useHdfs[%d]", mode.c_str(), useHdfs);
        return doGetFs(useHdfs ? FSLIB_FS_HDFS_FILESYSTEM_NAME : FSLIB_FS_PANGU_FILESYSTEM_NAME);
    }

    bool alb = false;
    bool pov = false;
    bool dir = true;
    string linkEnv = autil::EnvUtil::getEnv("FSLIB_DFS_STORAGE_LINKS");
    if (linkEnv.empty()) {
        linkEnv = autil::EnvUtil::getEnv("HIPPO_ENV_STORAGE_LINKS");
    }
    if (!linkEnv.empty()) {
        string linkStr(linkEnv);
        StringUtil::toLowerCase(linkStr);
        alb = linkStr.find("alb") != string::npos;
        pov = linkStr.find("pov") != string::npos;
        dir = linkStr.find("dir") != string::npos;
    }
    bool useHdfs = alb && !pov;
    AUTIL_LOG(INFO, "ALB[%d], DIR[%d], POV[%d], useHdfs[%d]", alb, dir, pov, useHdfs);
    return doGetFs(useHdfs ? FSLIB_FS_HDFS_FILESYSTEM_NAME : FSLIB_FS_PANGU_FILESYSTEM_NAME);
}

AbstractFileSystem *FileSystemFactory::getFs(const FsType &type, bool safe) {
    if (safe) {
        ScopedLock lock(_lock);
        return doGetFs(type);
    }
    return doGetFs(type);
}

void FileSystemFactory::setFs(const FsType &type, AbstractFileSystem *fs, bool safe) {
    AbstractFileSystem *originalFs = NULL;
    if (safe) {
        ScopedLock lock(_lock);
        originalFs = _fsMap[type];
        _fsMap[type] = fs;
    } else {
        _fsMap[type] = fs;
    }
    if (originalFs) {
        destroyFs(type, originalFs);
    }
}

AbstractFileSystem *FileSystemFactory::getRawFs(const FsType &type, bool safe) {
    AbstractFileSystem *fs = getFs(type, safe);
    return getRawFs(fs);
}

void FileSystemFactory::setRawFs(const FsType &type, AbstractFileSystem *fs, bool safe) {
    assert(dynamic_cast<ProxyFileSystem *>(fs) == NULL);
    AbstractFileSystem *proxyFs = fs;
    if (!autil::EnvUtil::getEnv("FSLIB_DISABLE_PROXY_FILE_SYSTEM", false)) {
        proxyFs = new ProxyFileSystem(fs);
    }
    setFs(type, proxyFs, safe);
}

AbstractFileSystem *FileSystemFactory::getRawFs(AbstractFileSystem *fs) {
    ProxyFileSystem *proxyFs = dynamic_cast<ProxyFileSystem *>(fs);
    if (proxyFs) {
        return proxyFs->getFs();
    }
    return fs;
}

FSLIB_END_NAMESPACE(fs);
