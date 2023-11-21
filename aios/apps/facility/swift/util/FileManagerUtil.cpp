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
#include "swift/util/FileManagerUtil.h"

#include <bits/types/struct_tm.h>
#include <iomanip>
#include <memory>
#include <ostream>
#include <stdio.h>
#include <time.h>

#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "fslib/fs/FileSystem.h"

namespace swift {
namespace util {
AUTIL_LOG_SETUP(swift, FileManagerUtil);

const std::string FileManagerUtil::DATA_SUFFIX = ".data";
const std::string FileManagerUtil::META_SUFFIX = ".meta";
const std::string FileManagerUtil::INLINE_FILE = "inline";

using namespace autil;
using namespace fslib;
using namespace fslib::fs;
using namespace swift::storage;

struct FilePairComparator {
    bool operator()(const FilePairPtr &lhs, const FilePairPtr &rhs) const {
        return lhs->beginMessageId < rhs->beginMessageId;
    }
};

FileManagerUtil::FileManagerUtil() {}

FileManagerUtil::~FileManagerUtil() {}

fslib::ErrorCode FileManagerUtil::listFiles(const fslib::FileList &paths, FileLists &fileList) {
    fslib::FileList files;
    for (const auto &path : paths) {
        files.clear();
        auto ec = FileSystem::listDir(path, files);
        if (fslib::EC_OK != ec) {
            AUTIL_LOG(ERROR,
                      "list dir[%s] failed, fslib error[%d %s]",
                      path.c_str(),
                      ec,
                      FileSystem::getErrorString(ec).c_str());
            return ec;
        }
        AUTIL_LOG(INFO, "list dir[%s] success", path.c_str());
        for (const auto &file : files) {
            fileList.push_back(std::make_pair(path, file));
        }
    }
    return fslib::EC_OK;
}

protocol::ErrorCode FileManagerUtil::initFilePairVec(const FileLists &fileList, FilePairVec &filePairVec) {
    MetaDataMap metaDataPairMap = generateMetaDataPairMap(fileList);
    for (MetaDataMap::const_iterator it = metaDataPairMap.begin(); it != metaDataPairMap.end(); ++it) {
        const std::string &prefix = it->first.first;
        const std::string &dataPath = it->first.second;
        const std::string &metaFileName = it->second.first;
        const std::string &dataFileName = it->second.second;
        if (metaFileName.empty()) {
            AUTIL_LOG(ERROR, "meta file [%s.meta] loss", prefix.c_str());
            continue;
        }
        if (dataFileName.empty()) {
            AUTIL_LOG(ERROR, "data file [%s.data] loss", prefix.c_str());
            continue;
        }
        int64_t messageId = -1;
        int64_t timestamp = 0;
        if (!extractMessageIdAndTimestamp(prefix, messageId, timestamp)) {
            AUTIL_LOG(ERROR, "unknown prefix [%s]", prefix.c_str());
            continue;
        }
        std::string fullMetaFilePath = FileSystem::joinFilePath(dataPath, metaFileName);
        std::string fullDataFilePath = FileSystem::joinFilePath(dataPath, dataFileName);
        FilePairPtr filePair(new FilePair(fullDataFilePath, fullMetaFilePath, messageId, timestamp, false));
        filePairVec.push_back(filePair);
    }
    stable_sort(filePairVec.begin(), filePairVec.end(), FilePairComparator());
    return protocol::ERROR_NONE;
}

bool FileManagerUtil::removeFilePair(const storage::FilePairPtr &filePair) {
    return removeFilePair(filePair->metaFileName, filePair->dataFileName);
}

bool FileManagerUtil::removeFilePair(const std::string &metaFileName, const std::string &dataFileName) {
    bool removeSuccess = true;
    fslib::ErrorCode ec = FileSystem::remove(metaFileName);
    if (ec != fslib::EC_OK && ec != fslib::EC_NOENT) {
        AUTIL_LOG(WARN, "remove obsolete meta file[%s] failed", metaFileName.c_str());
        AUTIL_LOG(ERROR, "fslib errorcode [%d], errormsg [%s]", ec, fslib::fs::FileSystem::getErrorString(ec).c_str());
        removeSuccess = false;
    } else if (ec == fslib::EC_OK) {
        AUTIL_LOG(INFO, "remove obsolete meta file[%s] successfully", metaFileName.c_str());
    }
    ec = FileSystem::remove(dataFileName);
    if (ec != fslib::EC_OK && ec != fslib::EC_NOENT) {
        AUTIL_LOG(WARN, "remove obsolete data file[%s] failed", dataFileName.c_str());
        AUTIL_LOG(ERROR, "fslib errorcode [%d], errormsg [%s]", ec, fslib::fs::FileSystem::getErrorString(ec).c_str());
        removeSuccess = false;
    } else if (ec == fslib::EC_OK) {
        AUTIL_LOG(INFO, "remove obsolete data file[%s] successfully", dataFileName.c_str());
    }
    return removeSuccess;
}

bool FileManagerUtil::removeFilePairWithSize(const storage::FilePairPtr &filePair, int64_t &fileSize) {
    fileSize = 0;
    int64_t size = filePair->getMetaLength();
    bool removeMetaSuccess = removeFile(filePair->metaFileName);
    if (removeMetaSuccess) {
        fileSize += size;
    }
    size = getFileSize(filePair->dataFileName);
    bool removeDataSuccess = removeFile(filePair->dataFileName);
    if (removeDataSuccess) {
        fileSize += size;
    }
    return removeMetaSuccess && removeDataSuccess;
}

std::string FileManagerUtil::generateFilePrefix(int64_t msgId, int64_t timestamp, bool isNew) {
    time_t sec = timestamp / 1000000;
    int64_t us = timestamp % 1000000;

    char timestr[256];
    struct tm tim;
    localtime_r(&sec, &tim);
    strftime(timestr, sizeof(timestr), "%Y-%m-%d-%H-%M-%S", &tim);
    std::ostringstream oss;
    if (isNew) {
        oss << timestr << "." << std::setfill('0') << std::setw(6) << us << "." << std::setfill('0') << std::setw(16)
            << msgId << "." << std::setfill('0') << std::setw(16) << timestamp;
    } else {
        oss << timestr << "." << std::setfill('0') << std::setw(6) << us << "." << std::setfill('0') << std::setw(16)
            << msgId;
    }
    return oss.str();
}

FileManagerUtil::MetaDataMap FileManagerUtil::generateMetaDataPairMap(const FileLists &fileList) {
    MetaDataMap metaDataPairMap;
    for (FileLists::const_iterator it = fileList.begin(); it != fileList.end(); ++it) {
        const std::string &dataPath = it->first;
        const std::string &fileName = it->second;
        if (StringUtil::endsWith(fileName, DATA_SUFFIX)) {
            std::string prefix = fileName.substr(0, fileName.size() - DATA_SUFFIX.size());
            std::pair<std::string, std::string> key = make_pair(prefix, dataPath);
            metaDataPairMap[key].second = fileName;
        } else if (StringUtil::endsWith(fileName, META_SUFFIX)) {
            std::string prefix = fileName.substr(0, fileName.size() - META_SUFFIX.size());
            std::pair<std::string, std::string> key = make_pair(prefix, dataPath);
            metaDataPairMap[key].first = fileName;
        } else if (INLINE_FILE == fileName) {
            continue;
        } else {
            AUTIL_LOG(ERROR, "unknown file[%s], path[%s]", fileName.c_str(), dataPath.c_str());
        }
    }

    return metaDataPairMap;
}

bool FileManagerUtil::extractMessageIdAndTimestamp(const std::string &prefix, int64_t &msgId, int64_t &timestamp) {
    int us = 0;
    struct tm xx;
    xx.tm_isdst = -1;
    int64_t scanTime = 0;
    if (prefix.find('_') != std::string::npos) {
        int ret = sscanf(prefix.c_str(),
                         "%4d-%2d-%2d-%2d-%2d-%2d.%d_%ld.%ld",
                         &xx.tm_year,
                         &xx.tm_mon,
                         &xx.tm_mday,
                         &xx.tm_hour,
                         &xx.tm_min,
                         &xx.tm_sec,
                         &us,
                         &scanTime,
                         &msgId);
        if (ret != 9) {
            return false;
        }
    } else {
        int ret = sscanf(prefix.c_str(),
                         "%4d-%2d-%2d-%2d-%2d-%2d.%d.%ld.%ld",
                         &xx.tm_year,
                         &xx.tm_mon,
                         &xx.tm_mday,
                         &xx.tm_hour,
                         &xx.tm_min,
                         &xx.tm_sec,
                         &us,
                         &msgId,
                         &scanTime);
        if (ret < 8) {
            return false;
        }
    }
    xx.tm_mon -= 1;
    xx.tm_year -= 1900;
    time_t sec = mktime(&xx);
    int64_t tmpTime = sec * 1000000ll + us;
    if (scanTime != 0) {
        timestamp = scanTime;
        if (tmpTime != scanTime) {
            AUTIL_LOG(WARN, "matchine time zone error, expect[%ld], time str[%ld]", scanTime, tmpTime)
        }
    } else {
        timestamp = tmpTime;
    }
    return true;
}

int64_t FileManagerUtil::getFileSize(const std::string &fileName) {
    fslib::FileMeta fileMeta;
    fslib::ErrorCode ec = FileSystem::getFileMeta(fileName, fileMeta);
    if (ec == fslib::EC_OK) {
        return fileMeta.fileLength;
    }
    if (ec != fslib::EC_NOENT) {
        AUTIL_LOG(WARN,
                  "get file meta[%s] failed, fslib error[%d %s]",
                  fileName.c_str(),
                  ec,
                  FileSystem::getErrorString(ec).c_str());
    }
    return 0;
}

bool FileManagerUtil::removeFile(const std::string &fileName) {
    fslib::ErrorCode ec = FileSystem::remove(fileName);
    if (ec == fslib::EC_OK) {
        AUTIL_LOG(INFO, "remove obsolete file[%s] successfully", fileName.c_str());
        return true;
    }

    if (ec != fslib::EC_NOENT) {
        AUTIL_LOG(ERROR,
                  "remove obsolete file[%s] failed, fslib error[%d %s]",
                  fileName.c_str(),
                  ec,
                  FileSystem::getErrorString(ec).c_str());
        return false;
    }
    return true;
}

} // end namespace util
} // end namespace swift
