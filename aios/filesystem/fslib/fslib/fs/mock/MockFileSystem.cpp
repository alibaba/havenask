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
#include "fslib/fs/mock/MockFileSystem.h"

#include <assert.h>

#include "autil/StringUtil.h"
#include "fslib/fs/FileSystem.h"
#include "fslib/fs/FileSystemManager.h"
#include "fslib/fs/mock/MockFS.h"
#include "fslib/fs/mock/MockFile.h"

using namespace std;
using namespace autil;
using namespace fslib::fs;

FSLIB_PLUGIN_BEGIN_NAMESPACE(mock);
AUTIL_DECLARE_AND_SETUP_LOGGER(mock, MockFileSystem);

MockFileSystem::MockFileSystem() {}

MockFileSystem::~MockFileSystem() {}

bool MockFileSystem::init() { return true; }

File *MockFileSystem::openFile(const string &fileName, Flag flag) {
    return new MockFile(getFs(fileName)->openFile(parsePath(fileName), flag));
}

MMapFile *MockFileSystem::mmapFile(
    const string &fileName, Flag openMode, char *start, size_t length, int prot, int mapFlag, off_t offset) {
    return getFs(fileName)->mmapFile(parsePath(fileName), openMode, start, length, prot, mapFlag, offset);
}

ErrorCode MockFileSystem::rename(const string &oldFileName, const string &newFileName) {
    return getFs(oldFileName)->rename(parsePath(oldFileName), parsePath(newFileName));
}

ErrorCode MockFileSystem::link(const string &oldFileName, const string &newFileName) {
    return getFs(oldFileName)->link(parsePath(oldFileName), parsePath(newFileName));
}

ErrorCode MockFileSystem::getFileMeta(const string &fileName, FileMeta &fileMeta) {
    return getFs(fileName)->getFileMeta(parsePath(fileName), fileMeta);
}

ErrorCode MockFileSystem::isExist(const string &path) { return getFs(path)->isExist(parsePath(path)); }

ErrorCode MockFileSystem::isFile(const string &path) { return getFs(path)->isFile(parsePath(path)); }

ErrorCode MockFileSystem::isDirectory(const string &path) { return getFs(path)->isDirectory(parsePath(path)); }

ErrorCode MockFileSystem::mkDir(const string &dirName, bool recursive) {
    return getFs(dirName)->mkDir(parsePath(dirName), recursive);
}

ErrorCode MockFileSystem::listDir(const string &dirName, FileList &fileList) {
    return getFs(dirName)->listDir(parsePath(dirName), fileList);
}

ErrorCode MockFileSystem::listDir(const string &dirName, EntryList &entryList) {
    return getFs(dirName)->listDir(parsePath(dirName), entryList);
}

ErrorCode MockFileSystem::remove(const string &path) { return getFs(path)->remove(parsePath(path)); }

ErrorCode MockFileSystem::removeFile(const string &path) { return getFs(path)->removeFile(parsePath(path)); }

ErrorCode MockFileSystem::removeDir(const string &path) { return getFs(path)->removeDir(parsePath(path)); }

FileReadWriteLock *MockFileSystem::createFileReadWriteLock(const string &fileName) {
    return getFs(fileName)->createFileReadWriteLock(parsePath(fileName));
}

FileLock *MockFileSystem::createFileLock(const string &fileName) {
    return getFs(fileName)->createFileLock(parsePath(fileName));
}

FileChecksum MockFileSystem::getFileChecksum(const string &fileName) {
    return getFs(fileName)->getFileChecksum(parsePath(fileName));
}

ErrorCode MockFileSystem::getPathMeta(const string &path, PathMeta &pathMeta) {
    return getFs(path)->getPathMeta(parsePath(path), pathMeta);
}

ErrorCode apiPanguStat(const string &path, const string &args, string &output) { return EC_NOTSUP; }

ErrorCode apiPanguStatFileSystem(const string &path, const string &args, string &output) { return EC_NOTSUP; }

ErrorCode apiPanguLinkFile(const string &path, const string &args, string &output) { return EC_NOTSUP; }

ErrorCode apiPanguCreateInlinefile(const string &path, const string &args, string &output) {
    FilePtr file(MockFileSystem::getFs(path)->openFile(MockFileSystem::parsePath(path), WRITE));
    if (!file) {
        return EC_UNKNOWN;
    }
    output = "";
    return file->close();
}

ErrorCode apiPanguStatInlinefile(const string &path, const string &args, string &output) { return EC_NOTSUP; }

ErrorCode apiPanguUpdateInlinefile(const string &path, const string &args, string &output) {
    FilePtr file(MockFileSystem::getFs(path)->openFile(MockFileSystem::parsePath(path), WRITE));
    if (!file) {
        return EC_UNKNOWN;
    }
    ssize_t size = file->write(args.c_str(), args.length());
    if (size < 0) {
        return file->getLastError();
    }
    ErrorCode ec = file->close();
    if (ec != EC_OK) {
        return ec;
    }
    output = "";
    return EC_OK;
}

ErrorCode apiPanguReleaseFilelock(const string &path, const string &args, string &output) { return EC_NOTSUP; }

ErrorCode MockFileSystem::forward(const string &command, const string &path, const string &args, string &output) {
    static map<string, MockFS::ForwardFunc> funcMap{{"pangu_Stat", apiPanguStat},
                                                    {"pangu_StatFileSystem", apiPanguStatFileSystem},
                                                    {"pangu_LinkFile", apiPanguLinkFile},
                                                    {"pangu_CreateInlinefile", apiPanguCreateInlinefile},
                                                    {"pangu_StatInlinefile", apiPanguStatInlinefile},
                                                    {"pangu_UpdateInlinefile", apiPanguUpdateInlinefile},
                                                    {"pangu_ReleaseFilelock", apiPanguReleaseFilelock}};

    AUTIL_LOG(DEBUG, "command[%s], path[%s], args[%s]", command.c_str(), path.c_str(), args.c_str());

    const auto &it = _forwardFuncMap.find(command);
    if (it != _forwardFuncMap.end()) {
        return it->second(path, args, output);
    }

    const auto &it2 = funcMap.find(command);
    if (it2 != funcMap.end()) {
        return it2->second(path, args, output);
    }
    AUTIL_LOG(ERROR, "not support[%s], path[%s], args[%s]", command.c_str(), path.c_str(), args.c_str());
    return EC_NOTSUP;
}

AbstractFileSystem *MockFileSystem::getFs(const string &path) {
    assert(StringUtil::startsWith(path, "mock://"));
    return FileSystemManager::getRawFs(FileSystem::getFsType(parsePath(path)));
}

string MockFileSystem::parsePath(const string &path) {
    assert(StringUtil::startsWith(path, "mock://"));
    string mainPath = MockFS::getLocalRoot() + path.substr(strlen("mock://"));

    string parsedPath = mainPath;
    if (mainPath.find("://") == string::npos && mainPath[0] != '/') {
        parsedPath = "/" + mainPath;
    }
    if (parsedPath[0] == '/') {
        size_t configSeparatorPos = parsedPath.find("?");
        if (configSeparatorPos == string::npos) {
            return parsedPath;
        }
        size_t fsEnd = parsedPath.find("/", configSeparatorPos);
        if (fsEnd == string::npos) {
            fsEnd = parsedPath.length();
        }
        return parsedPath.substr(0, configSeparatorPos) + parsedPath.substr(fsEnd);
    }
    return parsedPath;
}

void MockFileSystem::setLocalRoot(const string &root) {
    if (root.length() > 0 && root[root.length() - 1] != '/') {
        _localRoot = root + "/";
    } else {
        _localRoot = root;
    }
}

string MockFileSystem::getLocalRoot() { return _localRoot; }

void MockFileSystem::mockForward(const string &command, const MockFS::ForwardFunc &func) {
    _forwardFuncMap[command] = func;
}

FSLIB_PLUGIN_END_NAMESPACE(mock);
