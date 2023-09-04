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
#include "swift/broker/storage_dfs/FsMessageMetaReader.h"

#include <algorithm>
#include <bits/types/struct_tm.h>
#include <cstdint>
#include <iosfwd>
#include <memory>
#include <stdio.h>
#include <time.h>
#include <utility>

#include "autil/CommonMacros.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"
#include "fslib/util/FileUtil.h"
#include "swift/common/FileMessageMeta.h"
#include "swift/common/RangeUtil.h"
#include "swift/filter/MessageFilter.h"
#include "swift/protocol/SwiftMessage.pb.h"
#include "swift/util/Block.h"

using namespace std;
using namespace autil;
using namespace fslib::fs;
using namespace swift::protocol;
using namespace swift::util;
using namespace swift::common;
using namespace swift::filter;

namespace swift {
namespace storage {
AUTIL_LOG_SETUP(swift, FsMessageMetaReader);

const uint64_t INT64_MAX_VALUE = numeric_limits<uint64_t>::max();

struct MetaFileInfoComparator {
    bool operator()(const MetaFileInfo &lhs, const MetaFileInfo &rhs) const { return lhs.timestamp < rhs.timestamp; }
};

FsMessageMetaReader::FsMessageMetaReader(const string &dfsRoot, const string &topic, const Filter *filter) {
    if (!initMetaFileLst(dfsRoot, topic, filter)) {
        AUTIL_LOG(ERROR, "init meta file list failed!");
        return;
    }
    if (filter) {
        AUTIL_LOG(INFO, "init meta with filter[%s]", filter->ShortDebugString().c_str());
        _filter = new MessageFilter(*filter);
    }
}

FsMessageMetaReader::~FsMessageMetaReader() { DELETE_AND_SET_NULL(_filter); }

bool FsMessageMetaReader::readMeta(vector<MessageMeta> &outMetaVec, int64_t beginTimestamp, int64_t endTimestamp) {
    outMetaVec.clear();
    if (beginTimestamp > endTimestamp) {
        AUTIL_LOG(ERROR, "beginTimestamp[%ld] should smaller than endTimestamp[%ld]", beginTimestamp, endTimestamp);
        return false;
    }
    for (const auto &item : _metaFileMap) {
        string partPath = _dataPath + "/" + to_string(item.first);
        size_t beginPos = 0, endPos = 0;
        if (!findMetaFileIndex(item.second, beginTimestamp, endTimestamp, beginPos, endPos)) {
            AUTIL_LOG(INFO, "[%ld->%ld] no suitable meta in [%s]", beginTimestamp, endTimestamp, partPath.c_str());
            continue;
        }
        AUTIL_LOG(INFO, "match meta file[%ld - %ld], path[%s]", beginPos, endPos, partPath.c_str());
        for (size_t index = beginPos; index < endPos; ++index) {
            const MetaFileInfo &info = item.second[index];
            string metaPath = partPath + "/" + info.name;
            string readStr;
            if (!fslib::util::FileUtil::readFile(metaPath, readStr)) {
                AUTIL_LOG(ERROR, "read meta[%s] fail", metaPath.c_str());
                continue;
            }
            AUTIL_LOG(INFO, "read meta[%s], len[%ld]", metaPath.c_str(), readStr.size());
            fillMeta(readStr, info, metaPath, beginTimestamp, endTimestamp, outMetaVec);
        }
    }
    AUTIL_LOG(INFO, "meta totalMatchCount[%ld]", outMetaVec.size());
    stable_sort(outMetaVec.begin(), outMetaVec.end());
    return true;
}

bool FsMessageMetaReader::fillMeta(const string &readStr,
                                   const MetaFileInfo &info,
                                   const string &metaPath,
                                   int64_t beginTimestamp,
                                   int64_t endTimestamp,
                                   vector<MessageMeta> &outMetaVec) {
    const FileMessageMeta *fileMetaVec = (const FileMessageMeta *)readStr.c_str();
    size_t msgCount = readStr.size() / FILE_MESSAGE_META_SIZE;
    int64_t endMsgId = info.beginMsgId + msgCount;
    if (endMsgId == info.endMsgId) {
        // exactly the same, most of the case
    } else if (endMsgId < info.endMsgId) {
        AUTIL_LOG(
            WARN, "some data loss, expect end[%ld] actual[%ld], file[%s]", info.endMsgId, endMsgId, metaPath.c_str());
    } else { // endMsgId > info.endMsgId
        AUTIL_LOG(INFO, "more data, expect end[%ld] actual[%ld], file[%s]", info.endMsgId, endMsgId, metaPath.c_str());
    }
    size_t oldSize = outMetaVec.size();
    for (size_t cursor = 0; cursor < msgCount; ++cursor) {
        const FileMessageMeta &fMeta = fileMetaVec[cursor];
        if (_filter && !_filter->filter(fMeta)) {
            continue;
        }
        if (fMeta.timestamp < beginTimestamp || fMeta.timestamp > endTimestamp) {
            continue;
        }
        MessageMeta meta(cursor + info.beginMsgId, fMeta.timestamp, fMeta.payload, fMeta.maskPayload, fMeta.isMerged);
        outMetaVec.push_back(meta);
    }
    AUTIL_LOG(INFO, "%s seekCount[%ld], matchCount[%ld]", metaPath.c_str(), msgCount, outMetaVec.size() - oldSize);
    return true;
}

bool FsMessageMetaReader::initMetaFileLst(const string &dfsRoot, const string &topic, const Filter *filter) {
    _metaFileMap.clear();
    if (dfsRoot.empty() || topic.empty()) {
        AUTIL_LOG(ERROR, "dfsRoot[%s] or topic[%s] is empty", dfsRoot.c_str(), topic.c_str());
        return false;
    }
    if (filter && (filter->from() > filter->to())) {
        AUTIL_LOG(ERROR, "filter from[%d] > to[%d] invalid", filter->from(), filter->to());
        return false;
    }
    _dataPath = dfsRoot;
    if (*(_dataPath.rbegin()) != '/') {
        _dataPath += "/";
    }
    _dataPath += topic;
    fslib::FileList fileList;
    fslib::ErrorCode ec = FileSystem::listDir(_dataPath, fileList);
    if (fslib::EC_OK != ec) {
        AUTIL_LOG(ERROR, "list dir[%s] failed", _dataPath.c_str());
        return false;
    }

    uint32_t partCount = fileList.size();
    vector<uint32_t> partIds;
    if (filter) {
        RangeUtil rangeUtil(partCount);
        partIds = rangeUtil.getPartitionIds(filter->from(), filter->to());
    } else {
        for (uint32_t i = 0; i < partCount; ++i) {
            partIds.push_back(i);
        }
    }
    for (size_t index = 0; index < partIds.size(); ++index) {
        string partPath = _dataPath + "/" + to_string(partIds[index]);
        vector<MetaFileInfo> &metaLst = _metaFileMap[partIds[index]];
        if (!getPartMetaFileList(partPath, metaLst)) {
            AUTIL_LOG(ERROR, "get partition meta file list[%s] failed", partPath.c_str());
            return false;
        }
    }

    return true;
}

bool FsMessageMetaReader::getPartMetaFileList(const string &partPath, vector<MetaFileInfo> &metaLst) {
    fslib::FileList fileLst;
    fslib::ErrorCode ec = FileSystem::listDir(partPath, fileLst);
    if (fslib::EC_OK != ec) {
        AUTIL_LOG(ERROR, "list dir[%s] failed", partPath.c_str());
        return false;
    }
    for (size_t i = 0; i < fileLst.size(); ++i) {
        if (StringUtil::endsWith(fileLst[i], ".meta")) {
            MetaFileInfo info(fileLst[i]);
            if (extractMessageIdAndTimestamp(fileLst[i], info.beginMsgId, info.timestamp)) {
                metaLst.push_back(info);
            }
        }
    }
    AUTIL_LOG(INFO, "[%s] has [%ld] files, meta[%ld]", partPath.c_str(), fileLst.size(), metaLst.size());
    if (0 == metaLst.size()) {
        AUTIL_LOG(INFO, "meta file[%s] empty", partPath.c_str());
        return true;
    }
    stable_sort(metaLst.begin(), metaLst.end(), MetaFileInfoComparator());
    for (size_t index = 1; index < metaLst.size(); ++index) {
        metaLst[index - 1].endMsgId = metaLst[index].beginMsgId;
    }
    // last meta file
    size_t lastMetaFilePos = metaLst.size() - 1;
    string metaPath = partPath + "/" + metaLst[lastMetaFilePos].name;
    fslib::FileMeta fileMeta;
    ec = FileSystem::getFileMeta(metaPath, fileMeta);
    if (ec != fslib::EC_OK) {
        AUTIL_LOG(ERROR, "get file meta [%s] failed", metaPath.c_str());
        return false;
    }
    uint32_t msgCnt = fileMeta.fileLength / FILE_MESSAGE_META_SIZE;
    metaLst[lastMetaFilePos].endMsgId = metaLst[lastMetaFilePos].beginMsgId + msgCnt;
    return true;
}

bool FsMessageMetaReader::findMetaFileIndex(const vector<MetaFileInfo> &metaFileInfo,
                                            uint64_t beginTimestamp,
                                            uint64_t endTimestamp,
                                            size_t &beginPos,
                                            size_t &endPos) {
    if (0 == metaFileInfo.size()) {
        return false;
    }
    if (0 == beginTimestamp && INT64_MAX_VALUE == endTimestamp) {
        beginPos = 0;
        endPos = metaFileInfo.size();
        return true;
    }
    beginPos = endPos = 0;
    bool hasSetBegin = false;
    for (; endPos < metaFileInfo.size(); ++endPos) {
        if (metaFileInfo[endPos].timestamp > endTimestamp) {
            break;
        }
        if (!hasSetBegin && metaFileInfo[endPos].timestamp > beginTimestamp) {
            beginPos = endPos;
            hasSetBegin = true;
        }
    }
    beginPos = (beginPos == 0) ? 0 : beginPos - 1;
    return true;
}

bool FsMessageMetaReader::extractMessageIdAndTimestamp(const string &prefix, int64_t &msgId, int64_t &timestamp) {
    int us = 0;
    struct tm xx;
    xx.tm_isdst = 1;
    int64_t scanTime = 0;
    if (prefix.find('_') != string::npos) {
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
            AUTIL_LOG(WARN, "matchine time zone error, expect [%ld], time str [%ld].", scanTime, tmpTime)
        }
    } else {
        timestamp = tmpTime;
    }
    return true;
}

} // namespace storage
} // namespace swift
