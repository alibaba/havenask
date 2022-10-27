#include <ha3/common/ExtraSearchInfo.h>

using namespace std;

BEGIN_HA3_NAMESPACE(common);

void ExtraSearchInfo::serialize(autil::DataBuffer &dataBuffer) const {
    dataBuffer.write(otherInfoStr);
    dataBuffer.write(seekDocCount);
    dataBuffer.write(phaseTwoSearchInfo);
}

void ExtraSearchInfo::deserialize(autil::DataBuffer &dataBuffer) {
    dataBuffer.read(otherInfoStr);
    dataBuffer.read(seekDocCount);
    dataBuffer.read(phaseTwoSearchInfo);
}

void ExtraSearchInfo::toString(string &str, autil::mem_pool::Pool *pool) {
    autil::DataBuffer dataBuffer(1024, pool);
    serialize(dataBuffer);
    str.assign(dataBuffer.getData(), dataBuffer.getDataLen());
}

void ExtraSearchInfo::fromString(const std::string &str,
                                 autil::mem_pool::Pool *pool)
{
    autil::DataBuffer dataBuffer((void *)str.c_str(), str.size(), pool);
    deserialize(dataBuffer);
}

END_HA3_NAMESPACE(common);

