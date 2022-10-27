#include <ha3/common/ClauseBase.h>
#include <ha3/config/TypeDefine.h>

BEGIN_HA3_NAMESPACE(common);
HA3_LOG_SETUP(common, ClauseBase);

ClauseBase::ClauseBase() {
}

ClauseBase::~ClauseBase() {
}

std::string ClauseBase::boolToString(bool flag)  {
    if (flag) {
        return std::string("true");
    } else {
        return std::string("false");
    }
}

std::string ClauseBase::cluster2ZoneName(const std::string& clusterName) {
    std::string zoneName = clusterName;
    if (clusterName.find(".") == std::string::npos) {
        zoneName += ZONE_BIZ_NAME_SPLITTER + DEFAULT_BIZ_NAME;
    }
    return zoneName;
}


END_HA3_NAMESPACE(common);
