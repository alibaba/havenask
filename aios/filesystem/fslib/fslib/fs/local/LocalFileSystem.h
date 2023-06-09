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
#ifndef FSLIB_LOCALFILESYSTEM_H
#define FSLIB_LOCALFILESYSTEM_H

#include "autil/Log.h"
#include "fslib/common/common_define.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/AbstractFileSystem.h"

FSLIB_BEGIN_NAMESPACE(fs);

class LocalFileSystem : public AbstractFileSystem
{
public:
    LocalFileSystem();
    ~LocalFileSystem();

public:
    File* openFile(const std::string& rawFileName, Flag mode) override;

    MMapFile* mmapFile(const std::string& rawFileName, Flag openMode,
                                    char* start, size_t length, int prot,
                                    int mapFlag, off_t offset) override;

    ErrorCode rename(const std::string& oldRawFileName,
                                  const std::string& newRawFileName) override;

    ErrorCode getFileMeta(const std::string& rawFileName,
            FileMeta& fileMeta) override;

    ErrorCode isFile(const std::string& path) override;

    FileChecksum getFileChecksum(const std::string& rawFileName) override;

    ErrorCode mkDir(const std::string& dirName,
                                 bool recursive = false) override;

    ErrorCode isDirectory(const std::string& path) override;

    ErrorCode remove(const std::string& path) override;
    ErrorCode removeFile(const std::string& path) override;
    ErrorCode removeDir(const std::string& path) override;

    ErrorCode isExist(const std::string& path) override;

    ErrorCode listDir(const std::string& dirName, FileList& fileList) override;
    ErrorCode listDir(const std::string& dirName, EntryList& entryList) override;
    ErrorCode listDir(const std::string& dirName, RichFileList& fileList) override;

    FileReadWriteLock* createFileReadWriteLock(
            const std::string& fileName) override;

    FileLock* createFileLock(const std::string& fileName) override;

    FileSystemCapability getCapability() const override {
        return FSC_ALL_CAPABILITY;
    }

    ErrorCode isLink(const std::string &path);

    static ErrorCode convertErrno(int errNum);

private:
    static bool normalizeFilePath(const std::string& filePath,
                                  std::string& normalizedFilePath);
    static std::string getParentDir(const std::string& dirName);
    ErrorCode mkDirInternal(const std::string& dirName,
                            bool recursive = false);
    ErrorCode mkNestDir(const std::string& dirName);
    ErrorCode removeInternal(const std::string& path);
    ErrorCode isDirectoryInternal(const std::string& path);
    ErrorCode isFileInternal(const std::string& path);
    ErrorCode isLinkInternal(const std::string& path);
    ErrorCode isExistInternal(const std::string& path);
    ErrorCode removeDirInternal(const std::string& path);
    ErrorCode removeTypeInternal(const std::string& path, const char* type);

    static ErrorCode doLockMmapFile(const char *base, int64_t fileLength);
private:
    friend class MMapFileTest;
};

FSLIB_TYPEDEF_AUTO_PTR(LocalFileSystem);

FSLIB_END_NAMESPACE(local);

#endif //FSLIB_LOCALFILESYSTEM_H
