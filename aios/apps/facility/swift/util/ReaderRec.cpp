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
#include "swift/util/ReaderRec.h"

#include <iostream>
#include <limits>
#include <stdint.h>
#include <stdio.h>

#include "autil/TimeUtility.h"
#include "sstream" // IWYU pragma: keep

namespace swift {
namespace util {
using namespace std;

const int64_t ONE_SEC_US = 1000000;
const int64_t ONE_MIN_US = 60 * ONE_SEC_US;
const int64_t INT64_MAX_VALUE = numeric_limits<int64_t>::max();
const string MAX_STRING_NAME(3, char(255));

bool ReaderDes::setIpPort(const std::string &addressStr) {
    int a = 0, b = 0, c = 0, d = 0, port = 0;
    int ret = sscanf(addressStr.c_str(), "%d.%d.%d.%d:%d", &a, &b, &c, &d, &port);
    if (ret != 5) {
        return false;
    }
    this->ip = (a << 24) | (b << 16) | (c << 8) | d;
    this->port = port;
    return true;
}

string ReaderDes::toIpPort() const {
    ostringstream oss;
    oss << ((ip & 0xff000000) >> 24) << "." << ((ip & 0x00ff0000) >> 16) << "." << ((ip & 0x0000ff00) >> 8) << "."
        << (ip & 0x000000ff) << ":" << port;
    return oss.str();
}

string ReaderDes::toString() const {
    ostringstream oss;
    oss << ((ip & 0xff000000) >> 24) << "." << ((ip & 0x00ff0000) >> 16) << "." << ((ip & 0x0000ff00) >> 8) << "."
        << (ip & 0x000000ff) << ":" << port << ", from:" << from << ", to:" << to;
    return oss.str();
}

string ReaderInfo::toString() const {
    ostringstream oss;
    oss << "requestTime:" << requestTime << ",readFile:" << dataInfo->getReadFile()
        << ",nextTimestamp:" << nextTimestamp << ",nextMsgId:" << nextMsgId << ",metaFile:" << metaInfo->fileName
        << ",metaBlock:" << metaInfo->blockId << ",metaRate:" << metaInfo->getReadRate()
        << ",dataFile:" << dataInfo->fileName << ",dataBlock:" << dataInfo->blockId
        << ",dataRate:" << dataInfo->getReadRate();
    return oss.str();
}

ReadFileInfo::ReadFileInfo(int64_t msize) : msgSize(msize), blockId(-1), _isReadFile(false), _lastIsReadFile(false) {}

ReadFileInfo::~ReadFileInfo() {}

void ReadFileInfo::updateRate(int64_t size) {
    int64_t curTime = autil::TimeUtility::currentTime();
    _latedData.push_back(make_pair(curTime, size));
}

void ReadFileInfo::setReadFile(bool isReadFile) {
    _lastIsReadFile = _isReadFile;
    _isReadFile = isReadFile;
    if (_isReadFile != _lastIsReadFile) {
        int64_t curTime = autil::TimeUtility::currentTime();
        clearHistoryData(curTime);
    }
}

int64_t ReadFileInfo::getReadRate() {
    int64_t rate = calcRate(ONE_SEC_US);
    if (rate > 0) {
        return rate;
    } else {
        return calcRate(ONE_MIN_US) / 60;
    }
}

int64_t ReadFileInfo::calcRate(int64_t interval) {
    if (0 == _latedData.size()) {
        return 0;
    }
    int64_t curTime = autil::TimeUtility::currentTime();
    LatedDataDeque::const_iterator it = _latedData.end();
    int64_t totalSize = 0;
    while (--it != _latedData.begin() && curTime - it->first < interval) {
        totalSize += it->second;
    }
    // begin
    if (curTime - it->first < interval) {
        totalSize += it->second;
    }
    return totalSize;
}

void ReadFileInfo::clearHistoryData(int64_t expiredTime) {
    LatedDataDeque::iterator it = _latedData.begin();
    for (; it != _latedData.end(); it++) {
        if (it->first >= expiredTime) {
            break;
        }
        _latedData.pop_front();
    }
}

ReaderInfo::ReaderInfo(int64_t nexTimeStamp_,
                       int64_t nextMsgId_,
                       int64_t requestTime_,
                       int64_t metaBlockId_,
                       int64_t dataBlockId_,
                       std::string metaFileName_,
                       std::string dataFileName_,
                       bool isReadingFile_)
    : nextTimestamp(nexTimeStamp_)
    , nextMsgId(nextMsgId_)
    , requestTime(requestTime_)
    , metaInfo(new ReadFileInfo())
    , dataInfo(new ReadFileInfo()) {
    metaInfo->blockId = metaBlockId_;
    metaInfo->fileName = metaFileName_;
    dataInfo->blockId = dataBlockId_;
    dataInfo->fileName = dataFileName_;
    setReadFile(isReadingFile_);
}

void getSlowestMemReaderInfo(const ReaderInfoMap *readerInfoMap, int64_t &minTimestamp, int64_t &minMsgId) {
    minTimestamp = INT64_MAX_VALUE;
    minMsgId = INT64_MAX_VALUE;
    if (NULL == readerInfoMap || 0 == readerInfoMap->size()) {
        return;
    }
    for (auto iter = readerInfoMap->begin(); iter != readerInfoMap->end(); ++iter) {
        if (iter->second->getReadFile()) {
            continue;
        }
        if (iter->second->nextTimestamp < minTimestamp) {
            minTimestamp = iter->second->nextTimestamp;
        }
        if (iter->second->nextMsgId < minMsgId) {
            minMsgId = iter->second->nextMsgId;
        }
    }
}

void getSlowestFileReaderInfo(const ReaderInfoMap *readerInfoMap,
                              string &metaFileName,
                              int64_t &metaBlockId,
                              string &dataFileName,
                              int64_t &dataBlockId) {
    metaBlockId = INT64_MAX_VALUE;
    dataBlockId = INT64_MAX_VALUE;
    metaFileName = MAX_STRING_NAME;
    dataFileName = MAX_STRING_NAME;
    if (NULL == readerInfoMap || 0 == readerInfoMap->size()) {
        return;
    }
    // get min meta file and meta block id
    for (auto iter = readerInfoMap->begin(); iter != readerInfoMap->end(); ++iter) {
        if (!iter->second->getReadFile()) {
            continue;
        }
        if (iter->second->metaInfo->fileName < metaFileName) {
            metaFileName = iter->second->metaInfo->fileName;
        }
        if (iter->second->dataInfo->fileName < dataFileName) {
            dataFileName = iter->second->dataInfo->fileName;
        }
    }
    // get min block id
    for (auto iter = readerInfoMap->begin(); iter != readerInfoMap->end(); ++iter) {
        if (!iter->second->getReadFile()) {
            continue;
        }
        if (iter->second->metaInfo->fileName == metaFileName && iter->second->metaInfo->blockId < metaBlockId) {
            metaBlockId = iter->second->metaInfo->blockId;
        }
        if (iter->second->dataInfo->fileName == dataFileName && iter->second->dataInfo->blockId < dataBlockId) {
            dataBlockId = iter->second->dataInfo->blockId;
        }
    }
}

} // namespace util
} // namespace swift
