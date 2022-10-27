#ifndef ISEARCH_BS_COUNTERCONFIG_H
#define ISEARCH_BS_COUNTERCONFIG_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include <autil/legacy/jsonizable.h>

namespace build_service {
namespace config {

static const std::string COUNTER_CONFIG_POSITION = "position";    // default : "zookeeper"
static const std::string COUNTER_CONFIG_PARAMS = "parameters";
static const std::string COUNTER_PARAM_HOSTNAME = "hostname";
static const std::string COUNTER_PARAM_PORT = "port";
static const std::string COUNTER_PARAM_PASSWORD = "password";
static const std::string COUNTER_PARAM_FILEPATH = "path";
static const std::string COUNTER_PARAM_REDIS_KEY = "key";
static const std::string COUNTER_PARAM_REDIS_KEY_TTL = "key_ttl"; // key expire time in seconds; set it to negative can make key never expires
static const std::string COUNTER_PARAM_REDIS_FIELD = "field";

class CounterConfig : public autil::legacy::Jsonizable
{
public:
    CounterConfig();
    ~CounterConfig();
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override;
    bool validate() const;
    bool operator == (const CounterConfig &other) const;
    bool operator != (const CounterConfig &other) const;
public:
    std::string position;
    KeyValueMap params;
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(CounterConfig);

}
}

#endif //ISEARCH_BS_COUNTERCONFIG_H
