#ifndef ISEARCH_PHASETWOSEARCHINFO_H
#define ISEARCH_PHASETWOSEARCHINFO_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/DataBuffer.h>

BEGIN_HA3_NAMESPACE(common);

class PhaseTwoSearchInfo
{
public:
    PhaseTwoSearchInfo()
        : summaryLatency(0)
    {
    }
    ~PhaseTwoSearchInfo() {}
private:
    PhaseTwoSearchInfo(const PhaseTwoSearchInfo &);
    PhaseTwoSearchInfo& operator=(const PhaseTwoSearchInfo &);
public:
    void serialize(autil::DataBuffer &dataBuffer) const;
    void deserialize(autil::DataBuffer &dataBuffer);
    void mergeFrom(const std::vector<PhaseTwoSearchInfo*> &inputSearchInfos);
public:
    int64_t summaryLatency;
};

END_HA3_NAMESPACE(common);

#endif //ISEARCH_PHASETWOSEARCHINFO_H
