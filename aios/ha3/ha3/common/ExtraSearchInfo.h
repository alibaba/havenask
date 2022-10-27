#ifndef ISEARCH_EXTRASEARCHINFO_H
#define ISEARCH_EXTRASEARCHINFO_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/DataBuffer.h>
#include <ha3/common/PhaseTwoSearchInfo.h>

BEGIN_HA3_NAMESPACE(common);

class ExtraSearchInfo
{
public:
    ExtraSearchInfo()
        : seekDocCount(0)
        , phaseTwoSearchInfo(NULL)
    {
    }
    ~ExtraSearchInfo() {}
private:
    ExtraSearchInfo(const ExtraSearchInfo &);
    ExtraSearchInfo& operator=(const ExtraSearchInfo &);
public:
    void serialize(autil::DataBuffer &dataBuffer) const;
    void deserialize(autil::DataBuffer &dataBuffer);

    void toString(std::string &str, autil::mem_pool::Pool *pool);
    void fromString(const std::string &str, autil::mem_pool::Pool *pool);
public:
    std::string otherInfoStr;
    uint32_t seekDocCount;
    PhaseTwoSearchInfo *phaseTwoSearchInfo;
};

HA3_TYPEDEF_PTR(ExtraSearchInfo);

END_HA3_NAMESPACE(common);

#endif //ISEARCH_EXTRASEARCHINFO_H
