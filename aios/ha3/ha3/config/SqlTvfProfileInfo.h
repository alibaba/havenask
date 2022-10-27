#pragma once

#include <ha3/common.h>
#include <ha3/util/Log.h>
#include <autil/legacy/jsonizable.h>

BEGIN_HA3_NAMESPACE(config);

class SqlTvfProfileInfo : public autil::legacy::Jsonizable {
public:
    SqlTvfProfileInfo() {}
    SqlTvfProfileInfo(const std::string &tvfName_,
                      const std::string &funcName_,
                      const KeyValueMap &parameters_ = {})
        : tvfName(tvfName_)
        , funcName(funcName_)
        , parameters(parameters_)
    {
    }
    ~SqlTvfProfileInfo() {}
public:
    bool empty() const {
        return tvfName.empty() || funcName.empty();
    }
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
        json.Jsonize("tvf_name", tvfName, tvfName);
        json.Jsonize("func_name", funcName, funcName);
        json.Jsonize("parameters", parameters, parameters);
    }
public:
    std::string tvfName;
    std::string funcName;
    KeyValueMap parameters;
    HA3_LOG_DECLARE();
};
typedef std::vector<SqlTvfProfileInfo> SqlTvfProfileInfos;

HA3_TYPEDEF_PTR(SqlTvfProfileInfo);

END_HA3_NAMESPACE(config);
