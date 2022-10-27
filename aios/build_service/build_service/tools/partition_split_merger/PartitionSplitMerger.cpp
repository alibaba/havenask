#include "build_service/tools/partition_split_merger/PartitionSplitMerger.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/util/FileUtil.h"
#include "build_service/util/RangeUtil.h"
#include <indexlib/index_base/version_loader.h>
#include <indexlib/index_base/deploy_index_wrapper.h>
#include <autil/StringUtil.h>
#include <fslib/fslib.h>
#include <sstream>

using namespace std;
using namespace autil;
using namespace fslib;
using namespace fslib::fs;
IE_NAMESPACE_USE(index_base);

using namespace build_service::config;
using namespace build_service::util;
using namespace build_service::proto;

namespace build_service {
namespace tools {

BS_LOG_SETUP(tools, PartitionSplitMerger);

PartitionSplitMerger::PartitionSplitMerger() {
}

PartitionSplitMerger::~PartitionSplitMerger() {
}

bool PartitionSplitMerger::init(const Param &param) {
    uint32_t partSplitNum = param.partSplitNum;
    if (1 == partSplitNum) {
        string errorMsg = "no part split needed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    if (param.buildPartIdxFrom + param.buildPartCount > param.globalPartCount) {
        stringstream ss ;
        ss << "buildPartIdxFrom[" << param.buildPartIdxFrom 
           << "] + buildPartCount[" << param.buildPartCount 
           << "] > globalPartCount[" << param.globalPartCount << "]";
        string errorMsg = ss.str();
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }

    vector<Range> builtPartitions = calculateBuiltPartitions(param);
    if (builtPartitions.empty()) {
        string errorMsg = "no partitions built";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    for (uint32_t i = 0; i < builtPartitions.size(); i += partSplitNum) {
        SplitMergeInfo item;
        Range targetRange;
        const Range &startRange = builtPartitions[i];
        const Range &endRange = builtPartitions[i + partSplitNum - 1];
        targetRange.set_from(startRange.from());
        targetRange.set_to(endRange.to());
        item.mergePartDir = FileUtil::joinFilePath(param.generationDir,
                getPartitionName(targetRange));
        for (uint32_t j = i; j < i + partSplitNum; j++) {
            string splitDir = FileUtil::joinFilePath(
                    param.generationDir, getPartitionName(builtPartitions[j]));
            bool exist = false;
            if (!FileUtil::isExist(splitDir, exist) || !exist) {
                string errorMsg = "partition " + splitDir + " not exist";
                BS_LOG(ERROR, "%s", errorMsg.c_str());
                return false;
            }
            item.splitDirs.push_back(splitDir);
        }
        _splitMergeInfos.push_back(item);
    }
    return !_splitMergeInfos.empty();
}

vector<Range> PartitionSplitMerger::calculateBuiltPartitions(const Param &param) {
    vector<Range> allRanges = RangeUtil::splitRange(
            RANGE_MIN, RANGE_MAX, param.globalPartCount);
    vector<Range> builtRanges;
    for (uint32_t i = param.buildPartIdxFrom;
         i < param.buildPartIdxFrom + param.buildPartCount; i++)
    {
        vector<Range> ranges = RangeUtil::splitRange(allRanges[i].from(),
                allRanges[i].to(), param.buildParallelNum * param.partSplitNum);

        for (uint32_t j = 0; j < param.partSplitNum; ++j) {
            uint32_t first = j * param.buildParallelNum;
            uint32_t last = first + param.buildParallelNum - 1;
            Range range;
            range.set_from(ranges[first].from());
            range.set_to(ranges[last].to());
            builtRanges.push_back(range);
        }
    }
    return builtRanges;
}

bool PartitionSplitMerger::merge() {
    for (size_t i = 0; i < _splitMergeInfos.size(); ++i) {
        if (!mergeOnePartition(_splitMergeInfos[i])) {
            string splitDirStr = StringUtil::toString(
                    _splitMergeInfos[i].splitDirs, ";");
            cout << "ERROR: split [" << splitDirStr <<"] merge partition ["
                 << _splitMergeInfos[i].mergePartDir <<"] fail!" << endl;

            return false;
        }
    }
    return true;
}

bool PartitionSplitMerger::mergeOnePartition(const SplitMergeInfo& splitMergeInfo) {
    if (splitMergeInfo.splitDirs.size() == 1) {
        return true;
    }
    bool exist = false;
    if (FileUtil::isExist(splitMergeInfo.mergePartDir, exist) && exist) {
        string errorMsg = "merged partition ["
                          + splitMergeInfo.mergePartDir +"] already exist";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }

    if (!FileUtil::mkDir(splitMergeInfo.mergePartDir)) {
        string errorMsg = "mkdir for merged partition ["
                          + splitMergeInfo.mergePartDir + "] failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }

    _mergeVersionPtr.reset(new Version(0));
    for (size_t i = 0; i < splitMergeInfo.splitDirs.size(); ++i) {
        if (!mergeOneSplit(splitMergeInfo.splitDirs[i],
                           splitMergeInfo.mergePartDir)) {
            string errorMsg = "merge split ["
                              + splitMergeInfo.splitDirs[i] + "] to ["
                              + splitMergeInfo.mergePartDir + "]";
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return false;
        }
    }

    for (size_t i = 0; i < splitMergeInfo.splitDirs.size(); ++i) {
        if (!FileUtil::remove(splitMergeInfo.splitDirs[i])) {
            string errorMsg = "delete dir : ["
                              + splitMergeInfo.splitDirs[i] + "] fail!";
            BS_LOG(WARN, "%s", errorMsg.c_str());
        }
    }

    stringstream ss;
    ss << splitMergeInfo.mergePartDir
       << "/version." << _mergeVersionPtr->GetVersionId();

    DeployIndexWrapper::DumpDeployIndexMeta(splitMergeInfo.mergePartDir, *_mergeVersionPtr);
    _mergeVersionPtr->Store(splitMergeInfo.mergePartDir, false);
    return true;
}

bool PartitionSplitMerger::mergeOneSplit(
        const string &splitDir, const string &mergeDir)
{
    Version splitVersion;
    VersionLoader::GetVersion(splitDir, splitVersion, INVALID_VERSION);
    if (_mergeVersionPtr->GetSegmentCount() == 0)
    {
        moveDataExceptSegmentAndVersion(splitDir, mergeDir);
    }

    if (splitVersion.GetSegmentCount() == 0)
    {
        return true;
    }
    segmentid_t lastSegmentId = _mergeVersionPtr->GetLastSegment();
    moveSegmentData(splitDir, splitVersion, mergeDir, lastSegmentId);
    mergeVersion(splitVersion, _mergeVersionPtr);
    return true;
}

void PartitionSplitMerger::moveDataExceptSegmentAndVersion(
        const string &splitDir, const string &mergeDir)
{
    vector<string> entryVec;
    FileUtil::listDir(splitDir, entryVec);
    for (size_t i = 0; i < entryVec.size(); ++i) {
        if (isSegmentOrVersion(entryVec[i])) {
            continue;
        }

        string srcPath = splitDir + "/" + entryVec[i];
        string dstPath = mergeDir + "/" + entryVec[i];
        ErrorCode ec = FileSystem::rename(srcPath, dstPath);
        if (ec != EC_OK) {
            string errorMsg = "Rename [" + srcPath + "] to ["
                              + dstPath + "] failed";
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return;
        }
    }
}

void PartitionSplitMerger::moveSegmentData(const string &splitDir,
        const Version &splitVersion, const string &mergeDir, segmentid_t lastSegmentId)
{
    Version::Iterator iter = splitVersion.CreateIterator();
    while(iter.HasNext()) {
        segmentid_t splitSegId = iter.Next();
        segmentid_t newSegId = lastSegmentId + 1 + splitSegId;

        string splitSegmentPath = splitDir + "/segment_" + StringUtil::toString(splitSegId);
        string mergeSegmentPath = mergeDir + "/segment_" + StringUtil::toString(newSegId);

        ErrorCode ec = FileSystem::rename(splitSegmentPath, mergeSegmentPath);
        if (ec != EC_OK) {
            string errorMsg = "rename [" + splitSegmentPath + "] to ["
                              + mergeSegmentPath + "]";
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return;
        }
    }
}

void PartitionSplitMerger::mergeVersion(const Version &splitVersion, VersionPtr& mergeVersionPtr)
{
    // segment id
    segmentid_t lastSegmentId = mergeVersionPtr->GetLastSegment();
    Version::Iterator iter = splitVersion.CreateIterator();
    while(iter.HasNext()) {
        segmentid_t splitSegId = iter.Next();
        segmentid_t newSegId = lastSegmentId + 1 + splitSegId;
        mergeVersionPtr->AddSegment(newSegId);
    }

    // time stamp
    int64_t splitTimeStamp = splitVersion.GetTimestamp();
    if (splitTimeStamp > mergeVersionPtr->GetTimestamp()) {
        mergeVersionPtr->SetTimestamp(splitTimeStamp);
    }

    assert(mergeVersionPtr->GetFormatVersion() >= splitVersion.GetFormatVersion());
    mergeVersionPtr->SetFormatVersion(splitVersion.GetFormatVersion());
}

string PartitionSplitMerger::getPartitionName(const proto::Range &range) {
    stringstream ss;
    ss << "partition_" << range.from() << "_" << range.to();
    return ss.str();
}

bool PartitionSplitMerger::isSegmentOrVersion(const std::string &entry)
{
    string::const_reverse_iterator iter = entry.rbegin();
    uint32_t trimLen = 0;
    while (*iter == '/') {
        ++iter;
        ++trimLen;
    }

    bool match = false;
    string number;
    if (!match && entry.find("segment_") == 0) {
        number = entry.substr(8, entry.size() - trimLen - 8);
        match = true;
    }

    if (!match && entry.find("version.") == 0) {
        number = entry.substr(8, entry.size() - 8);
        match = true;
    }

    if (match)
    {
        int32_t value;
        return StringUtil::fromString(number, value);
    }
    return false;
}

}
}

