#include <ha3/turing/ops/AuxSearchInfo.h>
#include <autil/StringUtil.h>

using namespace std;

BEGIN_HA3_NAMESPACE(turing);

string AuxSearchInfo::toString() {
    string resultStr = "aux:" + autil::StringUtil::toString(_elapsedTime) + "ms_" +
                       autil::StringUtil::toString(_resultCount) + "^^";
    return resultStr;
}


END_HA3_NAMESPACE(turing);
