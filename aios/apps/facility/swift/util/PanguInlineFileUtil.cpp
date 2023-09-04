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
#include "swift/util/PanguInlineFileUtil.h"

#include <memory>
#include <ostream>
#include <stdlib.h>
#include <sys/types.h>
#include <vector>

#include "autil/CommonMacros.h"
#include "autil/Lock.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"

using namespace std;
using namespace autil;
using namespace fslib::fs;

namespace swift {
namespace util {
AUTIL_LOG_SETUP(swift, PanguInlineFileUtil);

static const std::string SwiftTestMockPanguInlineFile = "SWIFT_TEST_MOCK_PANGU_INLINE_FILE";
static autil::ThreadMutex TestInlineFileLock;

bool PanguInlineFileUtil::_mockInlineFile = false;
InlineVersion::InlineVersion() {}
InlineVersion::InlineVersion(uint64_t masterVersion, uint64_t partVersion)
    : _masterVersion(masterVersion), _partVersion(partVersion) {}

bool InlineVersion::deserialize(const std::string &inlineVersion) {
    if (inlineVersion.size() != UNIT_COUNT * 2) {
        return false;
    }
    _masterVersion = 0;
    _partVersion = 0;
    for (size_t i = 0; i < UNIT_COUNT; ++i) {
        _masterVersion |= ((uint64_t)inlineVersion[i] & 0x7F) << i * UNIT;
    }
    for (size_t i = UNIT_COUNT; i < UNIT_COUNT * 2; ++i) {
        _partVersion |= ((uint64_t)inlineVersion[i] & 0x7F) << (i - UNIT_COUNT) * UNIT;
    }
    return true;
}

std::string InlineVersion::serialize() const {
    std::string result;
    if (!valid()) {
        return result;
    }
    for (size_t i = 0; i < UNIT_COUNT; ++i) {
        char c = ((_masterVersion >> (i * UNIT)) & 0x7F) | 0x80;
        result.push_back(c);
    }
    for (size_t i = 0; i < UNIT_COUNT; ++i) {
        char c = ((_partVersion >> (i * UNIT)) & 0x7F) | 0x80;
        result.push_back(c);
    }
    return result;
}

void InlineVersion::fromProto(const protocol::InlineVersion &inlineVersion) {
    _masterVersion = inlineVersion.masterversion();
    _partVersion = inlineVersion.partversion();
}

protocol::InlineVersion InlineVersion::toProto() {
    protocol::InlineVersion inlineVersion;
    inlineVersion.set_masterversion(_masterVersion);
    inlineVersion.set_partversion(_partVersion);
    return inlineVersion;
}

bool InlineVersion::valid() const { return _partVersion != 0; }

uint64_t InlineVersion::getMasterVersion() const { return _masterVersion; }

uint64_t InlineVersion::getPartVersion() const { return _partVersion; }

void InlineVersion::setMasterVersion(uint64_t value) { _masterVersion = value; }

std::string InlineVersion::toDebugString() const {
    return StringUtil::formatString("master version[%lu], part version[%lu]", _masterVersion, _partVersion);
}

bool InlineVersion::operator<(const InlineVersion &lh) const {
    if (_masterVersion < lh._masterVersion) {
        return true;
    } else if (_masterVersion == lh._masterVersion) {
        return _partVersion < lh._partVersion;
    } else {
        return false;
    }
}

bool InlineVersion::operator==(const InlineVersion &lh) const {
    return _masterVersion == lh._masterVersion && _partVersion == lh._partVersion;
}

PanguInlineFileUtil::PanguInlineFileUtil() {}

PanguInlineFileUtil::~PanguInlineFileUtil() {}

fslib::ErrorCode PanguInlineFileUtil::isInlineFileExist(const std::string &filePath, bool &isExist) {
    isExist = false;
    std::string args, content;
    auto ec = panguStatInlinefile(filePath, args, content);
    if (fslib::EC_NOENT == ec) {
        return fslib::EC_OK;
    } else if (fslib::EC_OK != ec) {
        AUTIL_LOG(ERROR,
                  "stat inline file[%s] fail, fslib error[%d %s]",
                  filePath.c_str(),
                  ec,
                  FileSystem::getErrorString(ec).c_str());
        return ec;
    }
    isExist = true;
    return fslib::EC_OK;
}

fslib::ErrorCode PanguInlineFileUtil::getInlineFile(const std::string &filePath, std::string &output) {
    std::string args;
    auto ec = panguStatInlinefile(filePath, args, output);
    if (fslib::EC_NOENT == ec) {
        auto cEc = panguCreateInlinefile(filePath, args, output);
        if (fslib::EC_OK == cEc) {
            AUTIL_LOG(INFO, "create inline file success, path [%s] output [%s]", filePath.c_str(), output.c_str());
        } else {
            AUTIL_LOG(ERROR,
                      "create inline file[%s] fail, fslib error[%d %s]",
                      filePath.c_str(),
                      cEc,
                      FileSystem::getErrorString(cEc).c_str());
            return cEc;
        }
    } else if (fslib::EC_OK != ec) {
        AUTIL_LOG(ERROR,
                  "stat inline file[%s] fail, fslib error[%d %s]",
                  filePath.c_str(),
                  ec,
                  FileSystem::getErrorString(ec).c_str());
        return ec;
    }
    return fslib::EC_OK;
}

fslib::ErrorCode
PanguInlineFileUtil::updateInlineFile(const std::string &filePath, uint64_t oldVersion, uint64_t newVersion) {
    std::string oldInlineVersion = oldVersion ? std::to_string(oldVersion) : "";
    std::string newInlineVersion = newVersion ? std::to_string(newVersion) : "";
    auto ec = updateInlineFile(filePath, oldInlineVersion, newInlineVersion);
    if (fslib::EC_OK == ec) {
        AUTIL_LOG(INFO,
                  "update inline file success, path [%s] from [%lu] to [%lu]",
                  filePath.c_str(),
                  oldVersion,
                  newVersion);
    } else {
        AUTIL_LOG(ERROR,
                  "update inline file failed, path [%s] from [%lu] to [%lu], fslib error[%d %s]",
                  filePath.c_str(),
                  oldVersion,
                  newVersion,
                  ec,
                  FileSystem::getErrorString(ec).c_str());
    }
    return ec;
}

fslib::ErrorCode PanguInlineFileUtil::updateInlineFile(const std::string &filePath,
                                                       const InlineVersion &oldInlineVersion,
                                                       const InlineVersion &newInlineVersion) {
    auto ec = updateInlineFile(filePath, oldInlineVersion.serialize(), newInlineVersion.serialize());
    if (fslib::EC_OK == ec) {
        AUTIL_LOG(INFO,
                  "update inline file success, path [%s] from [%s] to [%s]",
                  filePath.c_str(),
                  oldInlineVersion.toDebugString().c_str(),
                  newInlineVersion.toDebugString().c_str());
    } else {
        AUTIL_LOG(ERROR,
                  "update inline file failed, path [%s] from [%s] to [%s], fslib error[%d %s]",
                  filePath.c_str(),
                  oldInlineVersion.toDebugString().c_str(),
                  newInlineVersion.toDebugString().c_str(),
                  ec,
                  FileSystem::getErrorString(ec).c_str());
    }
    return ec;
}

fslib::ErrorCode PanguInlineFileUtil::updateInlineFile(const std::string &filePath,
                                                       const std::string &oldContent,
                                                       const std::string &newContent) {
    stringstream args;
    args << oldContent << ARGS_SEP << newContent;
    std::string content;
    return panguUpdateInlinefileCAS(filePath, args.str(), content);
}

File *PanguInlineFileUtil::openFileForWrite(const std::string &fileName,
                                            const std::string &inlineFilePath,
                                            const std::string &inlineContent,
                                            fslib::ErrorCode &ec) {
    std::string output;
    stringstream args;
    args << inlineFilePath << ARGS_SEP << inlineContent;
    ec = panguOpenForWriteWithInlinefile(fileName, args.str(), output);
    if (fslib::EC_OK != ec) {
        AUTIL_LOG(ERROR,
                  "write file with inline file failed, path args[%s] output[%s] error[%d %s]",
                  args.str().c_str(),
                  output.c_str(),
                  ec,
                  FileSystem::getErrorString(ec).c_str());
    }
    File *retFile = (File *)(atol(output.c_str()));
    return retFile;
}

fslib::ErrorCode PanguInlineFileUtil::sealFile(const std::string &filePath) {
    if (unlikely(isInlineFileMock())) {
        return fslib::EC_OK;
    }
    std::string args, output;
    fslib::ErrorCode ec = FileSystem::forward("pangu_SealFile", filePath, args, output);
    if (fslib::EC_OK == ec) {
        AUTIL_LOG(INFO, "seal file success, path[%s]", filePath.c_str());
    } else {
        AUTIL_LOG(ERROR,
                  "seal file fail, path[%s], error[%d %s %s]",
                  filePath.c_str(),
                  ec,
                  FileSystem::getErrorString(ec).c_str(),
                  output.c_str());
    }
    return ec;
}

void PanguInlineFileUtil::setInlineFileMock(bool needMock) { _mockInlineFile = needMock; }

bool PanguInlineFileUtil::isInlineFileMock() { return _mockInlineFile; }

fslib::ErrorCode
PanguInlineFileUtil::panguStatInlinefile(const std::string &filePath, const std::string &args, std::string &output) {
    if (unlikely(isInlineFileMock())) {
        ScopedLock lock(TestInlineFileLock);
        return loadFile(filePath, output);
    }
    return FileSystem::forward("pangu_StatInlinefile", filePath, args, output);
}

fslib::ErrorCode
PanguInlineFileUtil::panguCreateInlinefile(const std::string &filePath, const std::string &args, std::string &output) {
    if (unlikely(isInlineFileMock())) {
        ScopedLock lock(TestInlineFileLock);
        return storeFile(filePath, output);
    }
    return FileSystem::forward("pangu_CreateInlinefile", filePath, args, output);
}

fslib::ErrorCode PanguInlineFileUtil::panguUpdateInlinefileCAS(const std::string &filePath,
                                                               const std::string &args,
                                                               std::string &output) {
    if (unlikely(isInlineFileMock())) {
        std::vector<std::string> argsVec;
        StringUtil::split(argsVec, args, ARGS_SEP, false);
        if (argsVec.size() != 2) {
            return fslib::EC_BADARGS;
        }
        const std::string &oldContent = argsVec[0];
        const std::string &newContent = argsVec[1];
        std::string content;
        ScopedLock lock(TestInlineFileLock);
        auto ec = loadFile(filePath, content);
        if (ec != fslib::EC_OK) {
            return ec;
        }
        if (content != oldContent) {
            return fslib::EC_PERMISSION;
        }
        return storeFile(filePath, newContent);
    }
    return FileSystem::forward("pangu_UpdateInlinefileCAS", filePath, args, output);
}

fslib::ErrorCode PanguInlineFileUtil::panguOpenForWriteWithInlinefile(const std::string &filePath,
                                                                      const std::string &args,
                                                                      std::string &output) {
    if (unlikely(isInlineFileMock())) {
        std::vector<std::string> argsVec;
        StringUtil::split(argsVec, args, ARGS_SEP, false);
        if (argsVec.size() != 2) {
            return fslib::EC_BADARGS;
        }
        const std::string &inlineFilePath = argsVec[0];
        const std::string &inlineFileContent = argsVec[1];
        std::string content;
        {
            ScopedLock lock(TestInlineFileLock);
            auto ec = loadFile(inlineFilePath, content);
            if (ec != fslib::EC_OK) {
                return ec;
            }
            if (inlineFileContent != content) {
                return fslib::EC_PERMISSION;
            }
            auto *file = FileSystem::openFile(filePath, fslib::WRITE);
            if (unlikely(!file)) {
                AUTIL_LOG(ERROR, "open file failed, file[%s]", filePath.c_str());
                return fslib::EC_FALSE;
            }
            std::unique_ptr<fslib::fs::File> filePtr(file);
            if (!filePtr->isOpened()) {
                fslib::ErrorCode ec = filePtr->getLastError();
                if (ec == fslib::EC_EXIST) {
                    AUTIL_LOG(DEBUG, "file already exist, file[%s]", filePath.c_str());
                    return ec;
                }
                AUTIL_LOG(ERROR, "open file failed, file[%s], ec[%d]", filePath.c_str(), ec);
                return ec;
            }
            output = std::to_string(uint64_t(filePtr.release()));
            return ec;
        }
    }
    return FileSystem::forward("pangu_OpenForWriteWithInlinefile", filePath, args, output);
}

fslib::ErrorCode PanguInlineFileUtil::loadFile(const std::string &filePath, std::string &fileContent) noexcept {
    FilePtr file(FileSystem::openFile(filePath, fslib::READ));
    if (unlikely(!file)) {
        AUTIL_LOG(ERROR, "open file failed, file[%s]", filePath.c_str());
        return fslib::EC_FALSE;
    }
    if (!file->isOpened()) {
        fslib::ErrorCode ec = file->getLastError();
        if (ec == fslib::EC_NOENT) {
            AUTIL_LOG(DEBUG, "file not exist [%s].", filePath.c_str());
            return ec;
        } else {
            AUTIL_LOG(ERROR,
                      "file not exist [%s]. error: %s",
                      filePath.c_str(),
                      FileSystem::getErrorString(file->getLastError()).c_str());
            return ec;
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
                fileContent = std::string(buffer, readBytes);
            } else {
                fileContent.append(buffer, readBytes);
            }
        }
    } while (readBytes == bufSize);

    if (!file->isEof()) {
        AUTIL_LOG(ERROR,
                  "Read file[%s] FAILED, error: [%s]",
                  filePath.c_str(),
                  FileSystem::getErrorString(file->getLastError()).c_str());
        return fslib::EC_BADF;
    }

    fslib::ErrorCode ret = file->close();
    if (ret != fslib::EC_OK) {
        AUTIL_LOG(
            ERROR, "Close file[%s] FAILED, error: [%s]", filePath.c_str(), FileSystem::getErrorString(ret).c_str());
        return ret;
    }
    return fslib::EC_OK;
}

fslib::ErrorCode PanguInlineFileUtil::storeFile(const std::string &filePath, const std::string &data) noexcept {
    FilePtr file(FileSystem::openFile(filePath, fslib::WRITE, false, data.size()));
    if (unlikely(!file)) {
        AUTIL_LOG(ERROR, "open file failed, file[%s]", filePath.c_str());
        return fslib::EC_FALSE;
    }
    if (!file->isOpened()) {
        fslib::ErrorCode ec = file->getLastError();
        if (ec == fslib::EC_EXIST) {
            AUTIL_LOG(DEBUG, "file already exist, file[%s]", filePath.c_str());
            return ec;
        }
        AUTIL_LOG(ERROR, "open file failed, file[%s], ec[%d]", filePath.c_str(), ec);
        return ec;
    }
    auto ec = FileSystem::GENERATE_ERROR("write", file->getFileName());
    if (unlikely(ec != fslib::EC_OK)) {
        return ec;
    }
    ssize_t writeBytes = file->write(data.c_str(), data.size());
    if (writeBytes != static_cast<ssize_t>(data.size())) {
        AUTIL_LOG(ERROR,
                  "write file: [%s] failed, error: %s",
                  filePath.c_str(),
                  FileSystem::getErrorString(file->getLastError()).c_str());
        return fslib::EC_BADF;
    }

    ec = file->close();
    if (ec != fslib::EC_OK) {
        AUTIL_LOG(
            ERROR, "close file: [%s] failed, error: %s", filePath.c_str(), FileSystem::getErrorString(ec).c_str());
        return ec;
    }
    return fslib::EC_OK;
}

} // end namespace util
} // end namespace swift
