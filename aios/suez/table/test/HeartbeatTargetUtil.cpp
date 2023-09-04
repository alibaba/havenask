#include "HeartbeatTargetUtil.h"

#include "autil/HashAlgorithm.h"
#include "autil/StringUtil.h"
#include "suez/common/test/TableMetaUtil.h"

using namespace std;
using namespace autil;

namespace suez {

HeartbeatTarget
HeartbeatTargetUtil::prepareTarget(const vector<pair<string, string>> &strVec, bool cleanDisk, bool allowPreload) {
    TableMetas metas;
    string signatureStr = "";
    for (const auto &it : strVec) {
        auto meta = TableMetaUtil::makeTableMeta(it.first, it.second);
        metas[meta.first] = meta.second;
        signatureStr += it.first + ";" + it.second;
    }
    HeartbeatTarget target;
    target.setTableMetas(metas);
    target._cleanDisk = cleanDisk;
    target._preload = allowPreload;
    target._signature = StringUtil::toString(HashAlgorithm::hashString64(signatureStr.c_str(), signatureStr.size()));
    return target;
}

} // namespace suez
