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
#ifndef FSLIB_FILESYSTEMFACTORY_H
#define FSLIB_FILESYSTEMFACTORY_H

#include <ctime>
#include <map>
#include <string>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "fslib/common/common_define.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/AbstractFileSystem.h"
#include "fslib/fs/DllWrapper.h"
#include "fslib/util/Singleton.h"

FSLIB_BEGIN_NAMESPACE(fs);

typedef struct _moduleInfo {
    std::string path;
    uint32_t majorVersion;
    DllWrapper *dllWrapper;
} ModuleInfo;

typedef AbstractFileSystem *(*CreateFileSystemFun)();
typedef void (*DestroyFileSystemFun)(AbstractFileSystem *);

class FileSystemFactory : public util::Singleton<FileSystemFactory> {
public:
    friend class util::LazyInstantiation;

public:
    ~FileSystemFactory();

public:
    bool init();
    AbstractFileSystem *getFs(const FsType &type);
    void close();

public:
    // for test
    void setFileSystem(const FsType &type, AbstractFileSystem *fs) { _fsMap[type] = fs; }
    // for test
    ModuleInfo getFsModuleInfo(const FsType &type) {
        auto iter = _fsModuleInfo.find(type);
        if (iter != _fsModuleInfo.end()) {
            return iter->second;
        } else {
            return {};
        }
    }

private:
    FileSystemFactory();
    FileSystemFactory(const FileSystemFactory &);
    FileSystemFactory &operator=(const FileSystemFactory &);

private:
    // for plugin, such as DFS
    friend class FileSystemManager;
    AbstractFileSystem *getRawFs(const FsType &type, bool safe);
    void setRawFs(const FsType &type, AbstractFileSystem *fs, bool safe);
    AbstractFileSystem *getFs(const FsType &type, bool safe);
    void setFs(const FsType &type, AbstractFileSystem *fs, bool safe);
    static AbstractFileSystem *getRawFs(AbstractFileSystem *fs);

private:
    bool isFslibPlugin(const std::string &fileName);
    bool parseSoUnderOnePath(const std::string &path);
    bool parsePlugin(const std::string &fileName, FsType &fileType, uint32_t &majorVersion);
    bool isLocalFileSystem(const FsType &type);
    bool isZookeeperFileSystem(const FsType &type);
    bool isHttpFileSystem(const FsType &type);
    bool isDfsFileSystem(const FsType &type);
    AbstractFileSystem *createFileSystem(const FsType &type);
    AbstractFileSystem *createDfsFileSystem();
    AbstractFileSystem *createFs(const FsType &type);
    AbstractFileSystem *doGetFs(const FsType &type);
    void destroyFs(const FsType &fsType, AbstractFileSystem *fs);
    void destroyFs(const FsType &fsType);

private:
    std::map<FsType, AbstractFileSystem *> _fsMap;
    std::map<FsType, time_t> _failTimestampMap;
    double _retryInterval;
    std::map<FsType, ModuleInfo> _fsModuleInfo;
    autil::ThreadMutex _lock;
    friend class FileSystemFactoryTest;
};

FSLIB_END_NAMESPACE(fs);

#endif // FSLIB_FILESYSTEMFACTORY_H
