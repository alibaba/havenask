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

#include <memory>
#include <unordered_map>

#include "autil/legacy/any.h"
#include "fslib/fs/AbstractFileSystem.h"
#include "fslib/fs/mock/MockFS.h"
#include "fslib/fs/mock/common.h"

FSLIB_PLUGIN_BEGIN_NAMESPACE(mock);

class MockFileSystem : public AbstractFileSystem {
public:
    MockFileSystem();
    ~MockFileSystem();
    bool init();

public:
    File *openFile(const std::string &fileName, Flag flag) override;
    MMapFile *mmapFile(const std::string &fileName,
                       Flag openMode,
                       char *start,
                       size_t length,
                       int prot,
                       int mapFlag,
                       off_t offset) override;
    ErrorCode rename(const std::string &oldFileName, const std::string &newFileName) override;
    ErrorCode link(const std::string &srcPath, const std::string &dstPath) override;
    ErrorCode getFileMeta(const std::string &fileName, FileMeta &fileMeta) override;

    ErrorCode isExist(const std::string &path) override;
    ErrorCode isFile(const std::string &path) override;
    ErrorCode isDirectory(const std::string &path) override;

    ErrorCode mkDir(const std::string &dirName, bool recursive = false) override;

    ErrorCode listDir(const std::string &dirName, FileList &fileList) override;
    ErrorCode listDir(const std::string &dirName, EntryList &entryList) override;

    ErrorCode remove(const std::string &path) override;
    ErrorCode removeFile(const std::string &path) override;
    ErrorCode removeDir(const std::string &path) override;

    FileReadWriteLock *createFileReadWriteLock(const std::string &fileName) override;

    FileLock *createFileLock(const std::string &fileName) override;

    FileChecksum getFileChecksum(const std::string &fileName) override;

    // FileSystemCapability getCapability() const override;
    ErrorCode getPathMeta(const std::string &path, PathMeta &pathMeta) override;
    ErrorCode
    forward(const std::string &command, const std::string &path, const std::string &args, std::string &output) override;

public:
    void setLocalRoot(const std::string &root);
    std::string getLocalRoot();
    void mockForward(const std::string &command, const MockFS::ForwardFunc &func);

public:
    static AbstractFileSystem *getFs(const std::string &path);
    static std::string parsePath(const std::string &path);

private:
    std::map<std::string, MockFS::ForwardFunc> _forwardFuncMap;
    std::string _localRoot;
};

typedef std::unique_ptr<MockFileSystem> MockFileSystemPtr;

FSLIB_PLUGIN_END_NAMESPACE(mock);
