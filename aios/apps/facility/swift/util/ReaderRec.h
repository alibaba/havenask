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
#include <deque>
#include <map>
#include <memory>
#include <stdint.h>
#include <string>
#include <utility>

#include "swift/common/Common.h"
#include "swift/common/FileMessageMeta.h"

namespace swift {
namespace util {

class ReadFileInfo {
public:
    typedef std::deque<std::pair<int64_t, int64_t>> LatedDataDeque;
    ReadFileInfo(int64_t msize = 0);
    ~ReadFileInfo();

public:
    void updateRate(int64_t size);
    void setReadFile(bool isReadFile = true);
    bool getReadFile() { return _isReadFile; }
    int64_t getReadRate();
    void clearHistoryData(int64_t expiredTime);

private:
    int64_t calcRate(int64_t interval);

public:
    int64_t msgSize;
    int64_t blockId;
    std::string fileName;

private:
    LatedDataDeque _latedData;
    bool _isReadFile;
    bool _lastIsReadFile;
};
SWIFT_TYPEDEF_PTR(ReadFileInfo);

typedef __uint128_t uint128_t;
class ReaderDes {
public:
    ReaderDes() : ip(0), port(0), from(0), to(0), padding(0) {}
    ReaderDes(const std::string &ipPort, uint16_t from_, uint16_t to_) : from(from_), to(to_), padding(0) {
        setIpPort(ipPort);
    }
    uint128_t toUint128() const { return *reinterpret_cast<const uint128_t *>(this); }
    void fromUint128(uint128_t i) { *reinterpret_cast<uint128_t *>(this) = i; }
    bool setIpPort(const std::string &addressStr);
    std::string toIpPort() const;
    std::string toString() const;

public:
    uint32_t ip      : 32;
    uint16_t port    : 16;
    uint16_t from    : 16;
    uint16_t to      : 16;
    uint64_t padding : 48;
};
SWIFT_TYPEDEF_PTR(ReaderDes);
inline bool operator<(ReaderDes r1, ReaderDes r2) { return r1.toUint128() < r2.toUint128(); }
inline bool operator==(ReaderDes r1, ReaderDes r2) { return r1.toUint128() == r2.toUint128(); }

class ReaderInfo {
public:
    ReaderInfo()
        : nextTimestamp(0)
        , nextMsgId(0)
        , requestTime(0)
        , metaInfo(new ReadFileInfo(common::FILE_MESSAGE_META_SIZE))
        , dataInfo(new ReadFileInfo()) {}
    ReaderInfo(int64_t requestTime_)
        : nextTimestamp(0)
        , nextMsgId(0)
        , requestTime(requestTime_)
        , metaInfo(new ReadFileInfo(common::FILE_MESSAGE_META_SIZE))
        , dataInfo(new ReadFileInfo()) {}
    ReaderInfo(int64_t nexTimeStamp_,
               int64_t nextMsgId_,
               int64_t requestTime_,
               int64_t metaBlockId_,
               int64_t dataBlockId_,
               std::string metaFileName_,
               std::string dataFileName_,
               bool isReadingFile_);
    std::string toString() const;
    void setReadFile(bool isReadFile = true) {
        metaInfo->setReadFile(isReadFile);
        dataInfo->setReadFile(isReadFile);
    }
    bool getReadFile() const { return dataInfo->getReadFile(); }

public:
    int64_t nextTimestamp;
    int64_t nextMsgId;
    int64_t requestTime;
    ReadFileInfoPtr metaInfo;
    ReadFileInfoPtr dataInfo;
};
SWIFT_TYPEDEF_PTR(ReaderInfo);
typedef std::map<ReaderDes, ReaderInfoPtr> ReaderInfoMap;
void getSlowestMemReaderInfo(const ReaderInfoMap *readerInfoMap, int64_t &minTimestamp, int64_t &minMsgId);
void getSlowestFileReaderInfo(const ReaderInfoMap *readerInfoMap,
                              std::string &metaFileName,
                              int64_t &metaBlockId,
                              std::string &dataFileName,
                              int64_t &dataBlockId);

} // namespace util
} // namespace swift
