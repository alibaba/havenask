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
#ifndef FSLIB_PROXYFILESYSTEM_H
#define FSLIB_PROXYFILESYSTEM_H

#include "autil/Log.h"
#include "fslib/fs/AbstractFileSystem.h"

FSLIB_BEGIN_NAMESPACE(fs);

class ProxyFileSystem : public AbstractFileSystem
{
public:
    ProxyFileSystem(AbstractFileSystem* fs);
    virtual ~ProxyFileSystem();

public:
    virtual File* openFile(const std::string& fileName, Flag flag) override;
    virtual File* openFile(const std::string& fileName, Flag flag, ssize_t fileLength) override;

    virtual MMapFile* mmapFile(const std::string& fileName, Flag openMode,
                               char* start, size_t length, int prot,
                               int mapFlag, off_t offset) override;
    virtual MMapFile* mmapFile(const std::string& fileName, Flag openMode,
                               char* start, size_t length, int prot,
                               int mapFlag, off_t offset, ssize_t fileLength) override;

    virtual ErrorCode rename(const std::string& oldFileName,
                             const std::string& newFileName) override;

    virtual ErrorCode getFileMeta(const std::string& fileName,
                                  FileMeta& fileMeta) override;

    virtual ErrorCode isFile(const std::string& path) override;

    virtual FileChecksum getFileChecksum(const std::string& fileName) override;

    virtual ErrorCode mkDir(const std::string& dirName,
                            bool recursive = false) override;

    virtual ErrorCode listDir(const std::string& dirName,
                              FileList& fileList) override;

    virtual ErrorCode isDirectory(const std::string& path) override;

    virtual ErrorCode remove(const std::string& path) override;
    virtual ErrorCode removeFile(const std::string& path) override;
    virtual ErrorCode removeDir(const std::string& path) override;

    virtual ErrorCode isExist(const std::string& path) override;

    virtual ErrorCode listDir(const std::string& dirName,
                              EntryList& entryList) override;

    virtual ErrorCode listDir(const std::string& dirName, RichFileList& fileList) override;

    virtual FileReadWriteLock* createFileReadWriteLock(
            const std::string& fileName) override;

    virtual FileLock* createFileLock(const std::string& fileName) override;

    virtual FileSystemCapability getCapability() const override;

    virtual ErrorCode getPathMeta(const std::string &path, PathMeta &pathMeta) override;

    // forward command to real file system
    virtual ErrorCode forward(const std::string &command, const std::string &path,
                              const std::string &args, std::string &output) override;

public:
    AbstractFileSystem* getFs() const;
private:
    AbstractFileSystem* _fs;
};

FSLIB_TYPEDEF_AUTO_PTR(ProxyFileSystem);

FSLIB_END_NAMESPACE(fs);

#endif //FSLIB_PROXYFILESYSTEM_H
