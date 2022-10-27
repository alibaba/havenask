#ifndef ISEARCH_ENVPARSER_H
#define ISEARCH_ENVPARSER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <string>
#include <vector>

BEGIN_HA3_NAMESPACE(util);
class EnvParser {
public:
    static bool parseParaWays(const std::string &paraEnv,
                              std::vector<std::string> &paraWaysVec);
private:
    static bool isWayValid(const std::string& paraVal);
private:
    HA3_LOG_DECLARE();
};
END_HA3_NAMESPACE(util);

#endif
