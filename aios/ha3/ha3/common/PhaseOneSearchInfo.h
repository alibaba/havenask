#ifndef ISEARCH_PHASEONESEARCHINFO_H
#define ISEARCH_PHASEONESEARCHINFO_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/DataBuffer.h>
#include <ha3/common/QrsSearchInfo.h>
BEGIN_HA3_NAMESPACE(common);

struct PhaseOneSearchInfo
{
public:
    PhaseOneSearchInfo(uint32_t partitionCount_ = 0,
                       uint32_t useTruncateOptimizerCount_ = 0,
                       uint32_t fromFullCacheCount_ = 0,
                       uint32_t seekCount_ = 0,
                       uint32_t matchCount_ = 0,
                       int64_t rankLatency_ = 0,
                       int64_t rerankLatency_ = 0,
                       int64_t extraLatency_ = 0,
                       int64_t searcherProcessLatency_ = 0,
                       uint32_t seekDocCount_ = 0);
    ~PhaseOneSearchInfo();

public:
    void serialize(autil::DataBuffer &dataBuffer) const;
    void deserialize(autil::DataBuffer &dataBuffer);
    void mergeFrom(const std::vector<PhaseOneSearchInfo*> &inputSearchInfos);
    std::string toString() const;
    void reset();
public:
    bool operator == (const PhaseOneSearchInfo &other) const {
        return partitionCount == other.partitionCount
            && useTruncateOptimizerCount == other.useTruncateOptimizerCount
            && fromFullCacheCount == other.fromFullCacheCount
            && seekCount == other.seekCount
            && seekDocCount == other.seekDocCount
            && matchCount == other.matchCount
            && rankLatency == other.rankLatency
            && rerankLatency == other.rerankLatency
            && extraLatency == other.extraLatency
            && searcherProcessLatency == other.searcherProcessLatency
            && otherInfoStr == other.otherInfoStr
            && qrsSearchInfo == other.qrsSearchInfo;
    }
public:
    static PhaseOneSearchInfo merge(const std::vector<PhaseOneSearchInfo*> &searchInfos);
public:
    uint32_t partitionCount;
    uint32_t useTruncateOptimizerCount;
    uint32_t fromFullCacheCount;
    uint32_t seekCount; // root query executor seek count
    uint32_t seekDocCount; // term query executor seek count
    uint32_t matchCount;
    int64_t rankLatency;
    int64_t rerankLatency;
    int64_t extraLatency;
    int64_t searcherProcessLatency;
    bool needReport; // for report metric more accurately when has qrs cache 
    std::string otherInfoStr;
    QrsSearchInfo qrsSearchInfo;
};

HA3_TYPEDEF_PTR(PhaseOneSearchInfo);

END_HA3_NAMESPACE(common);

#endif //ISEARCH_PHASEONESEARCHINFO_H
