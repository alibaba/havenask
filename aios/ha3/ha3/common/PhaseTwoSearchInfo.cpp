#include <ha3/common/PhaseTwoSearchInfo.h>

using namespace std;

BEGIN_HA3_NAMESPACE(common);

void PhaseTwoSearchInfo::serialize(autil::DataBuffer &dataBuffer) const {
    dataBuffer.write(summaryLatency);
}

void PhaseTwoSearchInfo::deserialize(autil::DataBuffer &dataBuffer) {
    dataBuffer.read(summaryLatency);
}

void PhaseTwoSearchInfo::mergeFrom(
        const vector<PhaseTwoSearchInfo*> &searchInfos)
{
    for (size_t i = 0; i < searchInfos.size(); ++i) {
        summaryLatency += searchInfos[i]->summaryLatency;
    }
}

END_HA3_NAMESPACE(common);

