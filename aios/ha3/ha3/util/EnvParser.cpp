#include <ha3/util/EnvParser.h>
#include <autil/StringUtil.h>

using namespace autil;
using namespace std;
BEGIN_HA3_NAMESPACE(util);
HA3_LOG_SETUP(util, EnvParser);

bool EnvParser::parseParaWays(const string &paraEnv,
                              vector<string> &paraWaysVec)
{ //paraWays=2,4,8
    paraWaysVec.clear();
    StringUtil::split(paraWaysVec, paraEnv, ',');
    if (0 == paraWaysVec.size()) {
        return false;
    }
    for (const auto &paraVal : paraWaysVec) {
        if (!isWayValid(paraVal)) {
            HA3_LOG(ERROR, "para search ways param [(%s) from (%s)] invalid",
                    paraVal.c_str(), paraEnv.c_str());
            paraWaysVec.clear();
            return false;
        }
    }
    return true;
}

bool EnvParser::isWayValid(const string& paraVal) {
    if ("2" == paraVal || "4" == paraVal || "8" == paraVal || "16" == paraVal) {
        return true;
    }
    return false;
}

END_HA3_NAMESPACE(util);
