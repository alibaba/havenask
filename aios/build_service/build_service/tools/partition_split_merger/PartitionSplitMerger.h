#ifndef ISEARCH_BS_PARTITIONSPLITMERGER_H
#define ISEARCH_BS_PARTITIONSPLITMERGER_H

#include "build_service/util/Log.h"
#include "build_service/common_define.h"
#include "build_service/proto/BasicDefs.pb.h"
#include <indexlib/index_base/index_meta/version.h>

namespace build_service {
namespace tools {

class PartitionSplitMerger
{
public:
    struct Param {
        std::string generationDir;
        uint32_t buildPartIdxFrom;
        uint32_t buildPartCount;
        uint32_t globalPartCount;
        uint32_t partSplitNum;
        uint32_t buildParallelNum;
    };

    struct SplitMergeInfo
    {
        std::vector<std::string> splitDirs;
        std::string mergePartDir;
    };
    typedef std::vector<SplitMergeInfo> SplitMergeInfos;

public:
    PartitionSplitMerger();
    ~PartitionSplitMerger();
private:
    PartitionSplitMerger(const PartitionSplitMerger &);
    PartitionSplitMerger& operator=(const PartitionSplitMerger &);

public:
    bool init(const Param& param);
    bool merge();

private:
    bool mergeOnePartition(const SplitMergeInfo &splitMergeInfo);

    bool mergeOneSplit(const std::string &splitDir, 
                       const std::string &mergeDir);

    std::vector<proto::Range> calculateBuiltPartitions(const Param &param);

    static void moveDataExceptSegmentAndVersion(const std::string &splitDir, 
            const std::string &mergeDir);

    static void moveSegmentData(const std::string &splitDir, 
                                const IE_NAMESPACE(index_base)::Version &splitVersion, 
                                const std::string &mergeDir,
                                segmentid_t lastSegId);

    static void mergeVersion(
            const IE_NAMESPACE(index_base)::Version &splitVersion,
            IE_NAMESPACE(index_base)::VersionPtr& mergeVersion);

    static std::string getPartitionName(const proto::Range &range);

    static bool isSegmentOrVersion(const std::string &entry);

private:
    SplitMergeInfos _splitMergeInfos;
    IE_NAMESPACE(index_base)::VersionPtr _mergeVersionPtr;
private:
    BS_LOG_DECLARE();
};

}
}

#endif //ISEARCH_BS_PARTITIONSPLITMERGER_H
