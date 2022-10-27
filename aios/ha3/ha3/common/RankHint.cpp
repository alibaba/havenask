#include <ha3/common/RankHint.h>
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
BEGIN_HA3_NAMESPACE(common);

RankHint::RankHint() {
    sortPattern = sp_asc;
}

void RankHint::serialize(autil::DataBuffer &dataBuffer) const {
    dataBuffer.write(sortField);
    dataBuffer.write(sortPattern);
}

void RankHint::deserialize(autil::DataBuffer &dataBuffer) {
    dataBuffer.read(sortField);
    dataBuffer.read(sortPattern);
}

string RankHint::toString() const {
    string rankHintStr;
    rankHintStr.append("sortfield:");
    rankHintStr.append(sortField);
    rankHintStr.append(",sortpattern:");
    rankHintStr.append(StringUtil::toString((int)sortPattern));
    return rankHintStr;
}

END_HA3_NAMESPACE(common);

