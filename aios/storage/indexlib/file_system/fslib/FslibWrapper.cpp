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
#include "indexlib/file_system/fslib/FslibWrapper.h"

#include <assert.h>
#include <errno.h>
#include <iosfwd>
#include <list>
#include <memory>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <vector>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/EnvUtil.h"
#include "autil/Lock.h"
#include "autil/ThreadPool.h"
#include "autil/TimeUtility.h"
#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"
#include "fslib/fs/MMapFile.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/fslib/FslibCommonFileWrapper.h"
#include "indexlib/file_system/fslib/FslibFileWrapper.h"
#include "indexlib/util/EpochIdUtil.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace autil;
using namespace indexlib::util;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, FslibWrapper);

IOConfig FslibWrapper::_mergeIOConfig = IOConfig();

ThreadPoolPtr FslibWrapper::_writePool;
ThreadPoolPtr FslibWrapper::_readPool;
autil::ThreadMutex FslibWrapper::_writePoolLock;
autil::ThreadMutex FslibWrapper::_readPoolLock;

const string FslibWrapper::EPOCHID = "epochId";
const string FslibWrapper::FENCE_INLINE_FILE = string("fence.inline") + TEMP_FILE_SUFFIX;
const char FslibWrapper::FENCE_ARGS_SEP = '\0';
static autil::ThreadMutex TEST_panguLock;

void FslibWrapper::Reset() noexcept
{
    FslibWrapper::_mergeIOConfig = IOConfig();
    FslibWrapper::_writePool.reset();
    FslibWrapper::_readPool.reset();
}

FSResult<void> FslibWrapper::DeleteFile(const string& path, const DeleteOption& deleteOption) noexcept
{
    return Delete(path, deleteOption);
}

FSResult<void> FslibWrapper::DeleteDir(const string& path, const DeleteOption& deleteOption) noexcept
{
    return Delete(path, deleteOption);
}

FSResult<void> FslibWrapper::Delete(const string& path, const DeleteOption& deleteOption) noexcept
{
    auto ec = Delete(path, deleteOption.fenceContext).Code();
    if (deleteOption.mayNonExist) {
        if (ec == FSEC_NOENT) {
            AUTIL_LOG(INFO, "target remove path [%s] not exist!", path.c_str());
            return FSEC_OK;
        } else if (ec != FSEC_OK) {
            return FSEC_ERROR;
        }
        AUTIL_LOG(INFO, "delete exist path: %s", path.c_str());
    }
    return ec;
}

FSResult<void> FslibWrapper::Delete(const string& path, FenceContext* fenceContext) noexcept
{
    if (fenceContext) {
        return DeleteFencing(path, fenceContext);
    }

    fslib::ErrorCode ec = fslib::fs::FileSystem::remove(path);
    if (ec == fslib::EC_OK) {
        return FSEC_OK;
    }
    if (ec == fslib::EC_NOENT) {
        AUTIL_LOG(DEBUG, "delete [%s] failed, no entry", path.c_str());
        return FSEC_NOENT;
    }
    AUTIL_LOG(ERROR, "delete [%s] failed, ec[%d]", path.c_str(), ec);
    return FSEC_ERROR;
}

FSResult<void> FslibWrapper::AtomicStore(const string& filePath, const char* data, size_t len, bool removeIfExist,
                                         FenceContext* fenceContext) noexcept
{
    // @uniqFlag is to make tmp file unique for each worker
    string uniqFlag = fenceContext ? ("e" + fenceContext->epochId)
                                   : ("t" + std::to_string(autil::TimeUtility::currentTimeInMicroSeconds()));
    const string tempFilePath = filePath + "." + uniqFlag + TEMP_FILE_SUFFIX;
    if (removeIfExist) {
        bool isExist = false;
        auto ret = IsExist(filePath, isExist);
        if (ret != FSEC_OK) {
            AUTIL_LOG(WARN, "isExist failed of file[%s]", filePath.c_str());
            return ret;
        }
        if (isExist) {
            ret = Delete(filePath, fenceContext).Code();
            if (ret != FSEC_OK) {
                AUTIL_LOG(WARN, "delete file failed when atomic store, file[%s]", filePath.c_str());
                return ret;
            }
        }

        ret = IsExist(tempFilePath, isExist);
        if (ret != FSEC_OK) {
            AUTIL_LOG(WARN, "isExist failed of file[%s]", tempFilePath.c_str());
            return ret;
        }
        if (isExist) {
            ret = Delete(tempFilePath, FenceContext::NoFence()).Code();
            if (ret != FSEC_OK) {
                AUTIL_LOG(WARN, "delete file failed when atomic store, file[%s]", tempFilePath.c_str());
                return ret;
            }
        }
    }
    auto ret = Store(tempFilePath, data, len);
    if (ret != FSEC_OK) {
        AUTIL_LOG(ERROR, "store file failed, file[%s]", tempFilePath.c_str());
        return ret;
    }
    auto ec = RenameWithFenceContext(tempFilePath, filePath, fenceContext).Code();
    if (ec != FSEC_OK) {
        AUTIL_LOG(ERROR, "rename file: [%s] to file: [%s] failed, error: %d", tempFilePath.c_str(), filePath.c_str(),
                  ec);
        return ec;
    }
    return FSEC_OK;
}

bool FslibWrapper::UpdateFenceInlineFile(FenceContext* fenceContext) noexcept
{
    const string& epochId = fenceContext->epochId;
    string inlineFilePath = JoinPath(fenceContext->fenceHintPath, FENCE_INLINE_FILE);
    string currentEpochId;
    auto ec = StatPanguInlineFile(inlineFilePath, currentEpochId).Code();
    if (ec == FSEC_NOENT) {
        bool fenceHintPathExist = false;
        auto ec1 = IsExist(fenceContext->fenceHintPath, fenceHintPathExist).Code();
        if (ec1 != FSEC_OK) {
            AUTIL_LOG(ERROR, "fence hint path [%s] is exsited failed, ec [%d]", fenceContext->fenceHintPath.c_str(),
                      ec1);
            return false;
        }
        if (!fenceHintPathExist) {
            ec1 = MkDirIfNotExist(fenceContext->fenceHintPath);
            if (ec1 != FSEC_OK) {
                AUTIL_LOG(ERROR, "fence hint path [%s] create failed, ec [%d]", fenceContext->fenceHintPath.c_str(),
                          ec1);
                return false;
            }
        }
        ec1 = CreatePanguInlineFile(inlineFilePath);
        if (ec1 != FSEC_OK && ec1 != FSEC_EXIST) {
            AUTIL_LOG(ERROR, " failed as file [%s] not exist, and create it failed, ec is [%d]", inlineFilePath.c_str(),
                      ec1);
            return false;
        }
        ec1 = UpdatePanguInlineFileCAS(inlineFilePath, "", epochId);
        if (ec1 != FSEC_OK) {
            AUTIL_LOG(ERROR, " failed as update file [%s] to epochId [%s], failed, ec is [%d]", inlineFilePath.c_str(),
                      epochId.c_str(), ec1);
            return false;
        }
        currentEpochId = epochId;
    } else if (ec != FSEC_OK) {
        AUTIL_LOG(ERROR, "failed as stat file [%s] failed, ec is [%d]", inlineFilePath.c_str(), ec);
        return false;
    }

    if (!EpochIdUtil::CompareGE(epochId, currentEpochId)) {
        AUTIL_LOG(ERROR, "epochId [%s] is smaller than current epochId [%s], cannot operate file", epochId.c_str(),
                  currentEpochId.c_str());
        return false;
    }

    if (epochId != currentEpochId) {
        auto ec = UpdatePanguInlineFileCAS(inlineFilePath, currentEpochId, epochId).Code();
        if (ec != FSEC_OK) {
            AUTIL_LOG(ERROR, " failed as update file [%s] to epochId [%s], failed, ec is [%d]", inlineFilePath.c_str(),
                      epochId.c_str(), ec);
            return false;
        }
        AUTIL_LOG(INFO, "update fence inline file [%s] content from [%s] to [%s]", inlineFilePath.c_str(),
                  currentEpochId.c_str(), epochId.c_str());
    }
    return true;
}

FSResult<void> FslibWrapper::DeleteFencing(const std::string& path, FenceContext* fenceContext) noexcept
{
    assert(fenceContext);
    if (fenceContext->usePangu == true) {
        // args is @inlineFilePath+'\0'+@epochId
        if (fenceContext->hasPrepareHintFile == false) {
            if (!UpdateFenceInlineFile(fenceContext)) {
                AUTIL_LOG(ERROR, "fencing delete path [%s] failed, can't operate", path.c_str());
                return FSEC_ERROR;
            }
            fenceContext->hasPrepareHintFile = true;
        }
        stringstream args;
        args << JoinPath(fenceContext->fenceHintPath, FENCE_INLINE_FILE) << FENCE_ARGS_SEP << fenceContext->epochId;
        return DeletePanguPathCAS(path, args.str());
    } else {
        assert(false);
        return FSEC_ERROR;
    }
}

FSResult<void> FslibWrapper::Store(const string& filePath, const char* data, size_t size) noexcept
{
    fslib::fs::FilePtr file(fslib::fs::FileSystem::openFile(filePath, fslib::WRITE, false, size));
    if (unlikely(!file)) {
        AUTIL_LOG(ERROR, "open file failed, file[%s]", filePath.c_str());
        return FSEC_ERROR;
    }
    if (!file->isOpened()) {
        fslib::ErrorCode ec = file->getLastError();
        if (ec == fslib::EC_EXIST) {
            AUTIL_LOG(DEBUG, "file already exist, file[%s]", filePath.c_str());
            return FSEC_EXIST;
        }
        AUTIL_LOG(ERROR, "open file failed, file[%s], ec[%d]", filePath.c_str(), ec);
        return FSEC_ERROR;
    }
    if (unlikely(fslib::fs::FileSystem::GENERATE_ERROR("write", file->getFileName()) != fslib::EC_OK)) {
        return FSEC_ERROR;
    }
    ssize_t writeBytes = file->write(data, size);
    if (writeBytes != static_cast<ssize_t>(size)) {
        AUTIL_LOG(ERROR, "write file: [%s] failed, error: %s", filePath.c_str(),
                  GetErrorString(file->getLastError()).c_str());
        return FSEC_ERROR;
    }

    fslib::ErrorCode ec = file->close();
    if (ec != fslib::EC_OK) {
        AUTIL_LOG(ERROR, "close file: [%s] failed, error: %s", filePath.c_str(), GetErrorString(ec).c_str());
        return FSEC_ERROR;
    }
    return FSEC_OK;
}

FSResult<void> FslibWrapper::AtomicLoad(const string& filePath, string& fileContent) noexcept
{
    return Load(filePath, fileContent);
}

FSResult<void> FslibWrapper::Load(const string& filePath, string& fileContent) noexcept
{
    fslib::fs::FilePtr file(fslib::fs::FileSystem::openFile(filePath, fslib::READ));
    if (unlikely(!file)) {
        AUTIL_LOG(ERROR, "open file failed, file[%s]", filePath.c_str());
        return FSEC_ERROR;
    }
    if (!file->isOpened()) {
        fslib::ErrorCode ec = file->getLastError();
        if (ec == fslib::EC_NOENT) {
            AUTIL_LOG(DEBUG, "file not exist [%s].", filePath.c_str());
            return FSEC_NOENT;
        } else {
            AUTIL_LOG(ERROR, "file not exist [%s]. error: %s", filePath.c_str(),
                      GetErrorString(file->getLastError()).c_str());
            return FSEC_ERROR;
        }
    }

    fileContent.clear();
    constexpr ssize_t bufSize = 128 * 1024;
    char buffer[bufSize];
    ssize_t readBytes = 0;
    do {
        readBytes = file->read(buffer, bufSize);
        if (readBytes > 0) {
            if (fileContent.empty()) {
                fileContent = string(buffer, readBytes);
            } else {
                fileContent.append(buffer, readBytes);
            }
        }
    } while (readBytes == bufSize);

    if (!file->isEof()) {
        AUTIL_LOG(ERROR, "Read file[%s] FAILED, error: [%s]", filePath.c_str(),
                  GetErrorString(file->getLastError()).c_str());
        return FSEC_ERROR;
    }

    fslib::ErrorCode ret = file->close();
    if (ret != fslib::EC_OK) {
        AUTIL_LOG(ERROR, "Close file[%s] FAILED, error: [%s]", filePath.c_str(), GetErrorString(ret).c_str());
        return ParseFromFslibEC(ret);
    }
    return FSEC_OK;
}

FSResult<void> FslibWrapper::AtomicLoad(FslibFileWrapper* file, string& fileContent) noexcept
{
    fileContent.clear();
    if (!file) {
        AUTIL_LOG(ERROR, "empty file point");
        return FSEC_ERROR;
    }
    size_t readLength = 0;
    int64_t offset = 0;
    constexpr ssize_t bufSize = 128 * 1024;
    char buffer[bufSize];

    while (true) {
        auto ret = file->PRead(buffer, bufSize, offset, readLength);
        if (ret != FSEC_OK) {
            AUTIL_LOG(ERROR, "pread failed, ret[%d]", static_cast<int>(ret));
            return FSEC_ERROR;
        }
        if (readLength > 0) {
            if (fileContent.empty()) {
                fileContent = string(buffer, readLength);
            } else {
                fileContent.append(buffer, readLength);
            }
            offset += readLength;
        } else {
            break;
        }
    }
    return FSEC_OK;
}

FSResult<void> FslibWrapper::OpenFile(const string& fileName, fslib::Flag mode, bool useDirectIO, ssize_t fileLength,
                                      FslibFileWrapper*& fileWrapper) noexcept
{
    AUTIL_LOG(DEBUG, "OpenFile [%s], mode[%d]", fileName.c_str(), mode);
    fslib::fs::File* file = fslib::fs::FileSystem::openFile(fileName, mode, useDirectIO, fileLength);
    if (unlikely(!file)) {
        AUTIL_LOG(ERROR, "Open file: [%s] FAILED", fileName.c_str());
        return FSEC_ERROR;
    }
    if (!file->isOpened()) {
        fslib::ErrorCode ec = file->getLastError();
        delete file;
        if (ec == fslib::EC_NOENT) {
            AUTIL_LOG(ERROR, "File [%s] not exist.", fileName.c_str());
            return FSEC_NOENT;
        }
        AUTIL_LOG(ERROR, "Open file: [%s] FAILED, ec[%d]", fileName.c_str(), ec);
        return ParseFromFslibEC(ec);
    }
    fileWrapper = new FslibCommonFileWrapper(file, useDirectIO);
    return FSEC_OK;
}

FSResult<void> FslibWrapper::MmapFile(const string& fileName, fslib::Flag openMode, char* start, int64_t length,
                                      int prot, int mapFlag, off_t offset, ssize_t fileLength,
                                      fslib::fs::MMapFile*& mmapFile) noexcept
{
    AUTIL_LOG(DEBUG, "MmapFile [%s], mode[%d]", fileName.c_str(), openMode);
    if (openMode == fslib::READ) {
        if (length == -1) {
            auto ret = GetFileLength(fileName, length);
            if (!ret.OK()) {
                return ret;
            }
            length = fileLength > 0 ? fileLength : length;
        }
        if (length == 0) {
            mmapFile = new fslib::fs::MMapFile(fileName, -1, NULL, 0, 0, fslib::EC_OK);
            return FSEC_OK;
        }
    }
    assert(length != (size_t)-1 && length != 0);
    fslib::fs::MMapFile* file =
        fslib::fs::FileSystem::mmapFile(fileName, openMode, start, length, prot, mapFlag, offset, fileLength);
    if (!file) {
        AUTIL_LOG(ERROR, "Mmap file: [%s] FAILED", fileName.c_str());
        return FSEC_ERROR;
    }
    if (!file->isOpened()) {
        fslib::ErrorCode ec = file->getLastError();
        assert(ec != fslib::EC_OK);
        delete file;
        if (ec == fslib::EC_NOENT) {
            AUTIL_LOG(ERROR, "Mmap File: [%s] not exist.", fileName.c_str());
            return FSEC_NOENT;
        } else if (ec != fslib::EC_OK) {
            AUTIL_LOG(ERROR, "Mmap file: [%s] FAILED, ec[%d]", fileName.c_str(), ec);
            return FSEC_ERROR;
        }
    }
    mmapFile = file;
    return FSEC_OK;
}

FSResult<void> FslibWrapper::IsExist(const string& filePath, bool& ret) noexcept
{
    AUTIL_LOG(TRACE1, "IsExist [%s]", filePath.c_str());
    fslib::ErrorCode ec = fslib::fs::FileSystem::isExist(filePath);
    if (ec == fslib::EC_TRUE) {
        ret = true;
        return FSEC_OK;
    } else if (ec == fslib::EC_FALSE) {
        ret = false;
        return FSEC_OK;
    }
    AUTIL_LOG(WARN, "Determine file existence failed, file [%s], error: %s", filePath.c_str(),
              GetErrorString(ec).c_str());
    return FSEC_ERROR;
}

FSResult<void> FslibWrapper::IsDir(const string& path, bool& ret) noexcept
{
    fslib::ErrorCode ec = fslib::fs::FileSystem::isDirectory(path);
    if (ec == fslib::EC_TRUE) {
        ret = true;
        return FSEC_OK;
    } else if (ec == fslib::EC_FALSE || ec == fslib::EC_NOENT) {
        ret = false;
        return FSEC_OK;
    }
    AUTIL_LOG(WARN, "Determine is directory of path [%s] FAILED: [%s]", path.c_str(), GetErrorString(ec).c_str());
    ret = false;
    return FSEC_ERROR;
}

FSResult<void> FslibWrapper::IsFile(const string& path, bool& ret) noexcept
{
    fslib::ErrorCode ec = fslib::fs::FileSystem::isFile(path);
    if (ec == fslib::EC_TRUE) {
        ret = true;
        return FSEC_OK;
    } else if (ec == fslib::EC_FALSE || ec == fslib::EC_NOENT) {
        ret = false;
        return FSEC_OK;
    }
    AUTIL_LOG(WARN, "Determine is file of path [%s] FAILED: [%s]", path.c_str(), GetErrorString(ec).c_str());
    ret = false;
    return FSEC_ERROR;
}

bool FslibWrapper::IsTempFile(const string& filePath) noexcept
{
    size_t pos = filePath.rfind('.');
    return (pos != string::npos && filePath.substr(pos) == string(TEMP_FILE_SUFFIX));
}

FSResult<void> FslibWrapper::ListDirRecursive(const string& path, FileList& fileList) noexcept
{
    list<string> pathList;
    string relativeDir;
    do {
        fslib::EntryList tmpList;
        string absoluteDir = PathUtil::JoinPath(path, relativeDir);
        fslib::ErrorCode ec = fslib::fs::FileSystem::listDir(absoluteDir, tmpList);

        if (ec != fslib::EC_OK) {
            if (ec == fslib::EC_NOENT) {
                AUTIL_LOG(WARN, "path not exist, path[%s]", absoluteDir.c_str());
                return FSEC_NOENT;
            }
            if (ec == fslib::EC_NOTDIR) {
                AUTIL_LOG(ERROR, "path not dir, path[%s]", absoluteDir.c_str());
                return FSEC_NOTDIR;
            }
            AUTIL_LOG(ERROR, "list dir failed, path[%s], ec[%d]", absoluteDir.c_str(), ec);
            return FSEC_ERROR;
        }
        fslib::EntryList::iterator it = tmpList.begin();
        for (; it != tmpList.end(); it++) {
            if (IsTempFile(it->entryName)) {
                continue;
            }
            string tmpName = PathUtil::JoinPath(relativeDir, it->entryName);
            if (it->isDir) {
                pathList.push_back(tmpName);
                fileList.push_back(PathUtil::NormalizeDir(tmpName));
            } else {
                fileList.push_back(tmpName);
            }
        }
        if (pathList.empty()) {
            break;
        }
        relativeDir = pathList.front();
        pathList.pop_front();
    } while (true);
    return FSEC_OK;
}

FSResult<void> FslibWrapper::ListDir(const string& dirPath, FileList& fileList) noexcept
{
    fslib::ErrorCode ec = fslib::fs::FileSystem::listDir(dirPath, fileList);
    if (ec == fslib::EC_NOENT) {
        AUTIL_LOG(WARN, "dir not exist, path[%s]", dirPath.c_str());
        return FSEC_NOENT;
    } else if (ec == fslib::EC_NOTDIR) {
        AUTIL_LOG(ERROR, "path is not a dir, path[%s]", dirPath.c_str());
        return FSEC_NOTDIR;
    } else if (ec != fslib::EC_OK) {
        AUTIL_LOG(ERROR, "list dir failed, path[%s], ec[%d]", dirPath.c_str(), ec);
        return FSEC_ERROR;
    }
    return FSEC_OK;
}

FSResult<void> FslibWrapper::ListDir(const string& dirPath, fslib::EntryList& entryList) noexcept
{
    fslib::ErrorCode ec = fslib::fs::FileSystem::listDir(dirPath, entryList);
    if (ec == fslib::EC_NOENT) {
        AUTIL_LOG(WARN, "dir not exist, path[%s]", dirPath.c_str());
        return FSEC_NOENT;
    } else if (ec == fslib::EC_NOTDIR) {
        AUTIL_LOG(ERROR, "path is not a dir, path[%s]", dirPath.c_str());
        return FSEC_NOTDIR;
    } else if (ec != fslib::EC_OK) {
        AUTIL_LOG(ERROR, "list dir failed, path[%s], ec[%d]", dirPath.c_str(), ec);
        return FSEC_ERROR;
    }
    return FSEC_OK;
}

FSResult<void> FslibWrapper::MkDir(const string& dirPath, bool recursive, bool mayExist) noexcept
{
    fslib::ErrorCode ec = fslib::fs::FileSystem::mkDir(dirPath, recursive);
    if (ec == fslib::EC_EXIST) {
        if (mayExist) {
            return FSEC_OK;
        }
        AUTIL_LOG(ERROR, "dir already exist, path[%s]", dirPath.c_str());
        return FSEC_EXIST;
    } else if (ec == fslib::EC_NOENT) {
        AUTIL_LOG(ERROR, "parrent dir not exist, path[%s]", dirPath.c_str());
        return FSEC_NOTDIR;
    } else if (ec != fslib::EC_OK) {
        AUTIL_LOG(ERROR, "make dir failed, path[%s], ec[%d]", dirPath.c_str(), ec);
        return FSEC_ERROR;
    }
    return FSEC_OK;
}

FSResult<void> FslibWrapper::MkDirIfNotExist(const string& dirPath) noexcept
{
    fslib::ErrorCode ec = fslib::fs::FileSystem::mkDir(dirPath, true);
    if (ec == fslib::EC_NOENT) {
        AUTIL_LOG(ERROR, "make dir failed, path[%s], ec[%d]", dirPath.c_str(), ec);
        return FSEC_NOENT;
    }
    if (ec != fslib::EC_OK && ec != fslib::EC_EXIST) {
        AUTIL_LOG(ERROR, "make dir failed, path[%s], ec[%d]", dirPath.c_str(), ec);
        return FSEC_ERROR;
    }
    return FSEC_OK;
}

FSResult<void> FslibWrapper::Rename(const string& srcName, const string& dstName) noexcept
{
    return RenameWithFenceContext(srcName, dstName, FenceContext::NoFence());
}

FSResult<void> FslibWrapper::RenameWithFenceContext(const string& srcName, const string& dstName,
                                                    FenceContext* fenceContext) noexcept
{
    if (fenceContext) {
        return RenameFencing(srcName, dstName, fenceContext);
    }

    fslib::ErrorCode ec = fslib::fs::FileSystem::rename(srcName, dstName);
    if (ec == fslib::EC_NOENT) {
        AUTIL_LOG(WARN, "rename [%s] to [%s] failed, src not exist", srcName.c_str(), dstName.c_str());
        return FSEC_NOENT;
    } else if (ec == fslib::EC_EXIST) {
        AUTIL_LOG(WARN, "rename [%s] to [%s] failed, dst exist", srcName.c_str(), dstName.c_str());
        return FSEC_EXIST;
    } else if (ec != fslib::EC_OK) {
        AUTIL_LOG(ERROR, "rename [%s] to [%s] failed, ec[%d]", srcName.c_str(), dstName.c_str(), ec);
        return FSEC_ERROR;
    }
    return FSEC_OK;
}

FSResult<void> FslibWrapper::RenameFencing(const string& srcName, const string& dstName,
                                           FenceContext* fenceContext) noexcept
{
    if (srcName.find(TEMP_FILE_SUFFIX) == string::npos) {
        AUTIL_LOG(ERROR, "rename fenceing only support rename temp src");
        return FSEC_ERROR;
    }
    if (fenceContext->usePangu == true) {
        // args is @dstName + '\0' + @inlineFilePath+'\0'+@epochId
        if (fenceContext->hasPrepareHintFile == false) {
            if (!UpdateFenceInlineFile(fenceContext)) {
                AUTIL_LOG(ERROR, "fencing rename srcName [%s] to dest [%s] failed, can't operate", srcName.c_str(),
                          dstName.c_str());
                return FSEC_ERROR;
            }
            fenceContext->hasPrepareHintFile = true;
        }
        stringstream args;
        args << dstName << FENCE_ARGS_SEP << JoinPath(fenceContext->fenceHintPath, FENCE_INLINE_FILE) << FENCE_ARGS_SEP
             << fenceContext->epochId;
        return RenamePanguPathCAS(srcName, args.str());
    } else {
        assert(false);
        return FSEC_ERROR;
    }
}

FSResult<void> FslibWrapper::Copy(const string& srcName, const string& dstName) noexcept
{
    fslib::ErrorCode ec = fslib::fs::FileSystem::copy(srcName, dstName);
    if (ec == fslib::EC_NOENT) {
        AUTIL_LOG(ERROR, "source file not existe, file[%s]", srcName.c_str());
        return FSEC_NOENT;
    } else if (ec == fslib::EC_EXIST) {
        AUTIL_LOG(INFO, "dest file already exist, file[%s]", dstName.c_str());
        return FSEC_EXIST;
    } else if (ec != fslib::EC_OK) {
        AUTIL_LOG(ERROR, "copy [%s] to [%s] failed, ec[%d]", srcName.c_str(), dstName.c_str(), ec);
        return FSEC_ERROR;
    }
    return FSEC_OK;
}

FSResult<void> FslibWrapper::GetFileMeta(const string& fileName, fslib::FileMeta& fileMeta) noexcept
{
    AUTIL_LOG(TRACE1, "GetFileMeta [%s]", fileName.c_str());
    fslib::ErrorCode ec = fslib::fs::FileSystem::getFileMeta(fileName, fileMeta);
    if (ec == fslib::EC_NOENT) {
        AUTIL_LOG(INFO, "file not exist, file[%s]", fileName.c_str());
        return FSEC_NOENT;
    } else if (ec != fslib::EC_OK) {
        AUTIL_LOG(ERROR, "get file meta failed, file[%s], ec[%d]", fileName.c_str(), ec);
        return FSEC_ERROR;
    }
    return FSEC_OK;
}

FSResult<void> FslibWrapper::GetPathMeta(const string& path, fslib::PathMeta& pathMeta) noexcept
{
    AUTIL_LOG(TRACE1, "GetPathMeta [%s]", path.c_str());
    fslib::ErrorCode ec = fslib::fs::FileSystem::getPathMeta(path, pathMeta);
    if (ec == fslib::EC_NOENT) {
        AUTIL_LOG(INFO, "path not exist, path[%s]", path.c_str());
        return FSEC_NOENT;
    } else if (ec != fslib::EC_OK) {
        AUTIL_LOG(ERROR, "get path meta failed, path[%s], ec[%d]", path.c_str(), ec);
        return FSEC_ERROR;
    }
    return FSEC_OK;
}

FSResult<void> FslibWrapper::GetFileLength(const string& fileName, int64_t& fileLength) noexcept
{
    fslib::FileMeta fileMeta;
    fileMeta.fileLength = 0;
    AUTIL_LOG(TRACE1, "GetFileMeta [%s]", fileName.c_str());
    fslib::ErrorCode ec = fslib::fs::FileSystem::getFileMeta(fileName, fileMeta);
    if (ec == fslib::EC_NOENT) {
        AUTIL_LOG(WARN, "get file len failed, file not exist, file[%s]", fileName.c_str());
        return FSEC_NOENT;
    } else if (ec != fslib::EC_OK) {
        AUTIL_LOG(ERROR, "get file meta failed, file[%s], ec[%d]", fileName.c_str(), ec);
        return FSEC_ERROR;
    }
    fileLength = fileMeta.fileLength;
    return FSEC_OK;
}

FSResult<void> FslibWrapper::GetDirSize(const string& path, size_t& dirSize) noexcept
{
    fslib::FileList fileList;
    auto ret = ListDirRecursive(path, fileList);
    if (!ret.OK()) {
        AUTIL_LOG(WARN, "list path[%s] failed", path.c_str());
        return FSEC_ERROR;
    }
    dirSize = 0;
    for (const auto& fileName : fileList) {
        bool isDir = *fileName.rbegin() == '/';
        string filePath = PathUtil::JoinPath(path, fileName);
        if (!isDir) {
            auto [ec, fileLength] = GetFileLength(filePath);
            if (ec != FSEC_OK) {
                return FSEC_ERROR;
            }
            dirSize += fileLength;
        }
    }
    return FSEC_OK;
}

bool FslibWrapper::NeedMkParentDirBeforeOpen(const string& path) noexcept
{
    return fslib::fs::FileSystem::getFsType(path) == FSLIB_FS_LOCAL_FILESYSTEM_NAME;
}

FSResult<void> FslibWrapper::SymLink(const string& srcPath, const string& dstPath) noexcept
{
    if (fslib::fs::FileSystem::getFsType(srcPath) != FSLIB_FS_LOCAL_FILESYSTEM_NAME) {
        AUTIL_LOG(DEBUG, "not support make symlink for dfs path[%s]", srcPath.c_str());
        return FSEC_NOTSUP;
    }

    int ret = symlink(srcPath.c_str(), dstPath.c_str());
    if (ret < 0) {
        AUTIL_LOG(WARN, "symlink src [%s] to dst [%s] fail, %s.", srcPath.c_str(), dstPath.c_str(), strerror(errno));
        return FSEC_ERROR;
    }
    return FSEC_OK;
}

const autil::ThreadPoolPtr& FslibWrapper::GetReadThreadPool() noexcept
{
    ScopedLock lock(_readPoolLock);
    if (_readPool) {
        return _readPool;
    }
    InitThreadPool(_readPool, _mergeIOConfig.readThreadNum, _mergeIOConfig.readQueueSize, "indexFSWRead");
    return _readPool;
}

const autil::ThreadPoolPtr& FslibWrapper::GetWriteThreadPool() noexcept
{
    ScopedLock lock(_writePoolLock);
    if (_writePool) {
        return _writePool;
    }
    InitThreadPool(_writePool, _mergeIOConfig.writeThreadNum, _mergeIOConfig.writeQueueSize, "indexFSWWrite");
    return _writePool;
}

void FslibWrapper::InitThreadPool(ThreadPoolPtr& threadPool, uint32_t threadNum, uint32_t queueSize,
                                  const string& name) noexcept
{
    assert(!threadPool);
    threadPool.reset(new ThreadPool(threadNum, queueSize, true));
    threadPool->start(name);
}

FSResult<void> FslibWrapper::UpdatePanguInlineFile(const string& path, const string& content) noexcept
{
    constexpr const char* UPDATE_INLINE_COMMAND = "pangu_UpdateInlinefile";
    static string out;

    if (getenv("INDEXLIB_TEST_MOCK_PANGU_INLINE_FILE")) {
        ScopedLock lock(TEST_panguLock);
        auto ec = DeleteFile(path, DeleteOption::NoFence(true)).Code();
        if (ec != FSEC_OK) {
            return ec;
        }
        return Store(path, content);
    }

    fslib::ErrorCode ec = fslib::fs::FileSystem::forward(UPDATE_INLINE_COMMAND, path, content, out);
    if (ec == fslib::EC_OK) {
        return FSEC_OK;
    } else if (ec == fslib::EC_NOTSUP) {
        AUTIL_LOG(WARN, "update pangu inline file [%s] with content [%s] failed, ec[%d]", path.c_str(), content.c_str(),
                  ec);
        return FSEC_NOTSUP;
    } else {
        AUTIL_LOG(WARN, "update pangu inline file [%s] with content [%s] failed, ec[%d]", path.c_str(), content.c_str(),
                  ec);
        return FSEC_ERROR;
    }
}

FSResult<void> FslibWrapper::UpdatePanguInlineFileCAS(const std::string& path, const std::string& oldContent,
                                                      const std::string& newContent) noexcept
{
    constexpr const char* UPDATE_INLINE_COMMAND = "pangu_UpdateInlinefileCAS";

    if (getenv("INDEXLIB_TEST_MOCK_PANGU_INLINE_FILE")) {
        ScopedLock lock(TEST_panguLock);
        auto ec = DeleteFile(path, DeleteOption::NoFence(true)).Code();
        if (ec != FSEC_OK) {
            return ec;
        }
        return Store(path, newContent);
    }

    static string out;
    stringstream args;
    args << oldContent << FENCE_ARGS_SEP << newContent;
    fslib::ErrorCode ec = fslib::fs::FileSystem::forward(UPDATE_INLINE_COMMAND, path, args.str(), out);
    if (ec == fslib::EC_OK) {
        return FSEC_OK;
    } else if (ec == fslib::EC_NOTSUP) {
        AUTIL_LOG(WARN, "update pangu inline file [%s] with new content [%s] old content [%s] failed, ec[%d]",
                  path.c_str(), newContent.c_str(), oldContent.c_str(), ec);
        return FSEC_NOTSUP;
    } else if (ec == fslib::EC_PERMISSION) {
        AUTIL_LOG(WARN, "update pangu inline file [%s] with new content [%s] old content [%s] failed, ec[%d]",
                  path.c_str(), newContent.c_str(), oldContent.c_str(), ec);
        return FSEC_ERROR;
    } else {
        AUTIL_LOG(WARN, "update pangu inline file [%s] with new content [%s] old content [%s] failed, ec[%d]",
                  path.c_str(), newContent.c_str(), oldContent.c_str(), ec);
        return FSEC_ERROR;
    }
}

FSResult<void> FslibWrapper::CreatePanguInlineFile(const std::string& path) noexcept
{
    if (getenv("INDEXLIB_TEST_MOCK_PANGU_INLINE_FILE")) {
        ScopedLock lock(TEST_panguLock);
        auto ec = Store(path, "").Code();
        return ec;
    }

    constexpr const char* CREATE_INLINE_COMMAND = "pangu_CreateInlinefile";
    static string content, out;
    fslib::ErrorCode ec = fslib::fs::FileSystem::forward(CREATE_INLINE_COMMAND, path, content, out);
    if (ec == fslib::EC_OK) {
        return FSEC_OK;
    } else if (ec == fslib::EC_NOTSUP) {
        AUTIL_LOG(WARN, "create pangu inline file [%s] failed, ec[%d]", path.c_str(), ec);
        return FSEC_NOTSUP;
    } else if (ec == fslib::EC_EXIST) {
        AUTIL_LOG(WARN, "create pangu inline file [%s] failed, ec[%d]", path.c_str(), ec);
        return FSEC_EXIST;
    } else {
        AUTIL_LOG(WARN, "create pangu inline file [%s] failed, ec[%d]", path.c_str(), ec);
        return FSEC_ERROR;
    }
}

FSResult<void> FslibWrapper::StatPanguInlineFile(const std::string& path, string& out) noexcept
{
    if (getenv("INDEXLIB_TEST_MOCK_PANGU_INLINE_FILE")) {
        ScopedLock lock(TEST_panguLock);
        return Load(path, out);
    }

    constexpr const char* STAT_INLINE_COMMAND = "pangu_StatInlinefile";
    static string content;
    fslib::ErrorCode ec = fslib::fs::FileSystem::forward(STAT_INLINE_COMMAND, path, content, out);
    if (ec == fslib::EC_OK) {
        return FSEC_OK;
    } else if (ec == fslib::EC_NOTSUP) {
        AUTIL_LOG(INFO, "stat pangu inline file [%s] failed, ec[%d]", path.c_str(), ec);
        return FSEC_NOTSUP;
    } else if (ec == fslib::EC_NOENT) {
        AUTIL_LOG(WARN, "stat pangu inline file [%s] failed, ec[%d]", path.c_str(), ec);
        return FSEC_NOENT;
    } else {
        AUTIL_LOG(WARN, "stat pangu inline file [%s] failed, ec[%d]", path.c_str(), ec);
        return FSEC_ERROR;
    }
}

FSResult<void> FslibWrapper::DeletePanguPathCAS(const std::string& path, const std::string& args) noexcept
{
    if (getenv("INDEXLIB_TEST_MOCK_PANGU_INLINE_FILE")) {
        ScopedLock lock(TEST_panguLock);
        return Delete(path, FenceContext::NoFence());
    }

    constexpr const char* DELETE_WITH_INLINE_FILE = "pangu_DeleteWithInlinefile";
    static string output;
    auto fsEc = fslib::fs::FileSystem::forward(DELETE_WITH_INLINE_FILE, path, args, output);
    if (fsEc == fslib::EC_OK) {
        return FSEC_OK;
    } else if (fsEc == fslib::EC_NOTSUP) {
        assert(false);
        return FSEC_NOTSUP;
    } else if (fsEc == fslib::EC_PERMISSION) {
        AUTIL_LOG(WARN, "delete file cas [%s]  failed, ec[%d]", path.c_str(), fslib::EC_PERMISSION);
        return FSEC_ERROR;
    } else if (fsEc == fslib::EC_NOENT) {
        AUTIL_LOG(DEBUG, "delete [%s] failed, no entry", path.c_str());
        return FSEC_NOENT;
    } else {
        AUTIL_LOG(WARN, "delete pangu file [%s] failed, ec[%d], pangu ec [%d]", path.c_str(), FSEC_ERROR, fsEc);
        return FSEC_ERROR;
    }
}

FSResult<void> FslibWrapper::RenamePanguPathCAS(const std::string& srcName, const std::string& args) noexcept
{
    if (getenv("INDEXLIB_TEST_MOCK_PANGU_INLINE_FILE")) {
        ScopedLock lock(TEST_panguLock);
        vector<string> argVec;
        StringUtil::split(argVec, args, FENCE_ARGS_SEP, false);
        assert(argVec.size() == 3);
        string& destPath = argVec[0];
        return Rename(srcName, destPath);
    }
    constexpr const char* RENAME_WITH_INLINE_FILE = "pangu_RenameWithInlinefile";
    static string output;
    auto ec = fslib::fs::FileSystem::forward(RENAME_WITH_INLINE_FILE, srcName, args, output);
    vector<string> argVec;
    StringUtil::split(argVec, args, FENCE_ARGS_SEP, false);
    assert(argVec.size() == 3);
    const string& destName = argVec[0];
    if (ec == fslib::EC_OK) {
        return FSEC_OK;
    } else if (ec == fslib::EC_NOTSUP) {
        assert(false);
        return FSEC_NOTSUP;
    } else if (ec == fslib::EC_PERMISSION) {
        AUTIL_LOG(WARN, "src [%s] rename to dest [%s] failed, cas failed, ec[%d]", srcName.c_str(), destName.c_str(),
                  fslib::EC_PERMISSION);
        return FSEC_ERROR;
    } else if (ec == fslib::EC_NOENT) {
        AUTIL_LOG(WARN, "rename [%s] to [%s] failed, src not exist", srcName.c_str(), destName.c_str());
        return FSEC_NOENT;
    } else if (ec == fslib::EC_EXIST) {
        AUTIL_LOG(WARN, "rename [%s] to [%s] failed, dst exist", srcName.c_str(), destName.c_str());
        return FSEC_EXIST;
    } else {
        AUTIL_LOG(WARN, "src [%s] rename to dest [%s] failed, ec[%d]", srcName.c_str(), destName.c_str(), FSEC_ERROR);
        return FSEC_ERROR;
    }
}

//////////////////////////////////////////////////////////////////////
FSResult<bool> FslibWrapper::IsExist(const string& filePath) noexcept
{
    bool exist = false;
    auto ec = IsExist(filePath, exist).Code();
    return {ec, exist};
}

FSResult<bool> FslibWrapper::IsDir(const std::string& path) noexcept
{
    bool ret = false;
    auto ec = IsDir(path, ret).Code();
    return {ec, ret};
}

FSResult<bool> FslibWrapper::IsFile(const std::string& path) noexcept
{
    bool ret = false;
    auto ec = IsFile(path, ret).Code();
    return {ec, ret};
}

FSResult<unique_ptr<FslibFileWrapper>> FslibWrapper::OpenFile(const string& fileName, fslib::Flag mode,
                                                              bool useDirectIO, ssize_t fileLength) noexcept
{
    FslibFileWrapper* fileWrapper = NULL;
    auto ec = OpenFile(fileName, mode, useDirectIO, fileLength, fileWrapper).Code();
    return {ec, unique_ptr<FslibFileWrapper>(fileWrapper)};
}

FSResult<unique_ptr<fslib::fs::MMapFile>> FslibWrapper::MmapFile(const string& fileName, fslib::Flag openMode,
                                                                 char* start, int64_t length, int prot, int mapFlag,
                                                                 off_t offset, ssize_t fileLength) noexcept
{
    fslib::fs::MMapFile* mmapFile = NULL;
    auto ec = MmapFile(fileName, openMode, start, length, prot, mapFlag, offset, fileLength, mmapFile).Code();
    return {ec, unique_ptr<fslib::fs::MMapFile>(mmapFile)};
}

FSResult<size_t> FslibWrapper::GetFileLength(const string& fileName) noexcept
{
    int64_t fileLength = 0;
    auto ec = GetFileLength(fileName, fileLength).Code();
    return {ec, (size_t)fileLength};
}

FSResult<size_t> FslibWrapper::GetDirSize(const std::string& dirPath) noexcept
{
    size_t dirSize = 0;
    auto ec = GetDirSize(dirPath, dirSize).Code();
    return {ec, dirSize};
}

FSResult<fslib::FileMeta> FslibWrapper::GetFileMeta(const std::string& fileName) noexcept
{
    fslib::FileMeta fileMeta;
    auto ec = GetFileMeta(fileName, fileMeta);
    return {ec, fileMeta};
}

FSResult<fslib::PathMeta> FslibWrapper::GetPathMeta(const std::string& path) noexcept
{
    fslib::PathMeta pathMeta;
    auto ec = GetPathMeta(path, pathMeta).Code();
    return {ec, pathMeta};
}

void FslibWrapper::MkDirE(const string& dirPath, bool recursive, bool mayExist) noexcept(false)
{
    auto ec = MkDir(dirPath, recursive, mayExist).Code();
    if (ec != FSEC_OK) {
        INDEXLIB_FATAL_ERROR(FileIO, "make directory [%s] FAILED", dirPath.c_str());
    }
}

void FslibWrapper::DeleteFileE(const string& filePath, const DeleteOption& deleteOption) noexcept(false)
{
    auto ec = DeleteFile(filePath, deleteOption).Code();
    if (ec == FSEC_NOENT) {
        INDEXLIB_FATAL_ERROR(NonExist, "delete file FAILED, [%s] not exist.", filePath.c_str());
    } else if (unlikely(ec != FSEC_OK)) {
        INDEXLIB_FATAL_ERROR(FileIO, "delete file [%s] FAILED", filePath.c_str());
    }
}

void FslibWrapper::DeleteDirE(const string& dirPath, const DeleteOption& deleteOption) noexcept(false)
{
    auto ec = DeleteDir(dirPath, deleteOption).Code();
    if (ec == FSEC_NOENT) {
        INDEXLIB_FATAL_ERROR(NonExist, "dir [%s] not exist.", dirPath.c_str());
    } else if (unlikely(ec != FSEC_OK)) {
        INDEXLIB_FATAL_ERROR(FileIO, "delete dir [%s] FAILED", dirPath.c_str());
    }
}

void FslibWrapper::RenameE(const string& srcName, const string& dstName) noexcept(false)
{
    auto ec = Rename(srcName, dstName).Code();
    if (ec == FSEC_NOENT) {
        INDEXLIB_THROW_WARN(NonExist, "source file [%s] not exist.", srcName.c_str());
    } else if (ec == FSEC_EXIST) {
        INDEXLIB_FATAL_ERROR(Exist, "dest file [%s] already exist.", dstName.c_str());
    } else if (ec != FSEC_OK) {
        INDEXLIB_FATAL_ERROR(FileIO, "rename [%s] to [%s] FAILED", srcName.c_str(), dstName.c_str());
    }
}

void FslibWrapper::AtomicStoreE(const std::string& filePath, const std::string& fileContent) noexcept(false)
{
    auto ec = AtomicStore(filePath, fileContent.data(), fileContent.size()).Code();
    if (ec == FSEC_EXIST) {
        INDEXLIB_FATAL_ERROR(Exist, "file [%s] already exist.", filePath.c_str());
    } else if (ec != FSEC_OK) {
        INDEXLIB_FATAL_ERROR(FileIO, "store file [%s] FAILED", filePath.c_str());
    }
}

void FslibWrapper::AtomicLoadE(const std::string& filePath, std::string& fileContent) noexcept(false)
{
    auto ec = AtomicLoad(filePath, fileContent).Code();
    if (ec == FSEC_NOENT) {
        INDEXLIB_FATAL_ERROR(NonExist, "file [%s] not exist.", filePath.c_str());
    } else if (ec != FSEC_OK) {
        INDEXLIB_FATAL_ERROR(FileIO, "load file [%s] FAILED", filePath.c_str());
    }
}

void FslibWrapper::ListDirE(const string& dirPath, fslib::FileList& fileList) noexcept(false)
{
    auto ec = ListDir(dirPath, fileList).Code();
    if (ec == FSEC_NOENT) {
        INDEXLIB_THROW_WARN(NonExist, "Directory [%s] not exist.", dirPath.c_str());
    } else if (ec == FSEC_NOTDIR) {
        INDEXLIB_FATAL_ERROR(InconsistentState, "Path [%s] is not a dir.", dirPath.c_str());
    } else if (ec != FSEC_OK) {
        INDEXLIB_FATAL_ERROR(FileIO, "List directory [%s] FAILED", dirPath.c_str());
    }
}

FenceContext* FslibWrapper::CreateFenceContext(const std::string& fenceHintPath, const std::string& epochId) noexcept
{
    bool needFence = false;
    bool usePangu = false;
    if (epochId.empty() || EnvUtil::getEnv("INDEXLIB_FENCE_OPERATE_ROOT", "true") == "false") {
        return FenceContext::NoFence();
    }
    // for test
    if (EnvUtil::getEnv("INDEXLIB_TEST_MOCK_PANGU_INLINE_FILE", "false") == "true") {
        needFence = true;
        usePangu = true;
    } else {
        string out;
        constexpr const char* SUPPORT_FENCE_COMMAND = "pangu_SupportAtomicRename";
        auto ec = fslib::fs::FileSystem::forward(SUPPORT_FENCE_COMMAND, fenceHintPath, "", out);
        assert(ec == fslib::EC_OK || ec == fslib::EC_NOTSUP);
        needFence = (ec == fslib::EC_OK);
        usePangu = (out == "pangu");
    }
    return needFence ? FenceContext::Fence(usePangu, epochId, fenceHintPath) : FenceContext::NoFence();
}

std::string FslibWrapper::JoinPath(const std::string& path, const std::string& name) noexcept
{
    return PathUtil::JoinPath(path, name);
}

std::string FslibWrapper::GetErrorString(fslib::ErrorCode ec) noexcept
{
    return fslib::fs::FileSystem::getErrorString(ec);
}
}} // namespace indexlib::file_system
