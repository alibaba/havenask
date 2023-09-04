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
#ifndef FSLIB_PLUGIN_HTTPFILESYSTEM_H
#define FSLIB_PLUGIN_HTTPFILESYSTEM_H

#include <curl/curl.h>
#include <fslib/common.h>

#include "fslib/fs/AbstractFileSystem.h"

FSLIB_PLUGIN_BEGIN_NAMESPACE(http);
using namespace fslib;
using namespace fslib::fs;

class HttpFileSystem : public AbstractFileSystem {
public:
    HttpFileSystem() { curl_global_init(CURL_GLOBAL_DEFAULT); }
    ~HttpFileSystem() { curl_global_cleanup(); }

public:
    /*override*/ File *openFile(const std::string &fileName, Flag flag);

    /*override*/ MMapFile *mmapFile(
        const std::string &fileName, Flag openMode, char *start, size_t length, int prot, int mapFlag, off_t offset);

    /*override*/ ErrorCode rename(const std::string &oldPath, const std::string &newPath);

    /*override*/ ErrorCode link(const std::string &oldPath, const std::string &newPath);

    /*override*/ ErrorCode getFileMeta(const std::string &fileName, FileMeta &fileMeta);

    /*override*/ ErrorCode isFile(const std::string &path);

    /*override*/ FileChecksum getFileChecksum(const std::string &fileName);

    /*override*/ ErrorCode mkDir(const std::string &dirName, bool recursive = false);

    /*override*/ ErrorCode listDir(const std::string &dirName, FileList &fileList);

    /*ovverride*/ ErrorCode isDirectory(const std::string &path);

    /*override*/ ErrorCode remove(const std::string &pathName);

    /*override*/ ErrorCode isExist(const std::string &pathName);

    /*override*/ ErrorCode listDir(const std::string &dirName, EntryList &entryList);

    /*override*/ FileReadWriteLock *createFileReadWriteLock(const std::string &fileName) { return NULL; }

    /*override*/ FileLock *createFileLock(const std::string &fileName) { return NULL; }

    /*override*/ FileSystemCapability getCapability() const { return FSC_ALL_CAPABILITY; }
};

typedef std::shared_ptr<HttpFileSystem> HttpFileSystemPtr;

FSLIB_PLUGIN_END_NAMESPACE(http);

#endif // FSLIB_PLUGIN_HTTPFILESYSTEM_H
