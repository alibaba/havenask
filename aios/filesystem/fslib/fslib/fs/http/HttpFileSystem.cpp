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
#include "fslib/fs/http/HttpFileSystem.h"

#include "fslib/fs/http/HttpFile.h"

using namespace std;
using namespace autil;

FSLIB_PLUGIN_BEGIN_NAMESPACE(http);
AUTIL_DECLARE_AND_SETUP_LOGGER(http, HttpFileSystem);

File *HttpFileSystem::openFile(const string &fileName, Flag flag) {
    flag = (Flag)(flag & FLAG_CMD_MASK);
    if (flag != READ) {
        return new HttpFile(fileName, EC_NOTSUP);
    }
    HttpFile *httpFile = new HttpFile(fileName);
    if (!httpFile->initFileInfo()) {
        httpFile->close();
    }
    return httpFile;
}

MMapFile *HttpFileSystem::mmapFile(
    const string &fileName, Flag openMode, char *start, size_t length, int prot, int mapFlag, off_t offset) {
    return new MMapFile(fileName, -1, NULL, -1, -1, EC_NOTSUP);
}

ErrorCode HttpFileSystem::rename(const string &oldPath, const string &newPath) { return EC_NOTSUP; }

ErrorCode HttpFileSystem::link(const string &oldPath, const string &newPath) { return EC_NOTSUP; }

ErrorCode HttpFileSystem::getFileMeta(const string &fileName, FileMeta &fileMeta) {
    HttpFile file(fileName);
    if (file.initFileInfo()) {
        fileMeta = file.getFileMeta();
        return EC_OK;
    } else {
        return EC_BADF;
    }
}

ErrorCode HttpFileSystem::isFile(const string &path) {
    HttpFile file(path);
    if (file.initFileInfo()) {
        return EC_TRUE;
    } else {
        return EC_FALSE;
    }
}

FileChecksum HttpFileSystem::getFileChecksum(const string &fileName) { return EC_NOTSUP; }

ErrorCode HttpFileSystem::mkDir(const string &dirName, bool recursive) { return EC_NOTSUP; }

ErrorCode HttpFileSystem::listDir(const string &dirName, FileList &fileList) { return EC_NOTSUP; }

ErrorCode HttpFileSystem::isDirectory(const string &path) { return EC_FALSE; }

ErrorCode HttpFileSystem::remove(const string &pathName) { return EC_NOTSUP; }

ErrorCode HttpFileSystem::isExist(const string &pathName) { return isFile(pathName); }

ErrorCode HttpFileSystem::listDir(const string &dirName, EntryList &entryList) { return EC_NOTSUP; }

FSLIB_PLUGIN_END_NAMESPACE(http);
