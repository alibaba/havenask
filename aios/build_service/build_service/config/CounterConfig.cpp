#include "build_service/config/CounterConfig.h"
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
using namespace autil::legacy;

namespace build_service {
namespace config {
BS_LOG_SETUP(config, CounterConfig);

CounterConfig::CounterConfig()
    : position("zookeeper")
{
}

CounterConfig::~CounterConfig() {
}

void CounterConfig::Jsonize(Jsonizable::JsonWrapper &json) {
    json.Jsonize("position", position, position);
    json.Jsonize("parameters", params, params);
}

bool CounterConfig::validate() const {
    if (position != "zookeeper" && position != "redis") {
        BS_LOG(ERROR, "invalid counter_position [%s], only support {%s, %s}",
               position.c_str(), "zookeeper", "redis");
        return false;
    }

    if (position == "redis") {
        auto it = params.find(COUNTER_PARAM_HOSTNAME);
        if (it == params.end()) {
            BS_LOG(ERROR, "missing [%s] in parameters", COUNTER_PARAM_HOSTNAME.c_str());
            return false;
        }
        it = params.find(COUNTER_PARAM_PORT);
        if (it == params.end()) {
            BS_LOG(ERROR, "missing [%s] in parameters", COUNTER_PARAM_PORT.c_str());
            return false;
        } else {
            uint16_t port;
            if (!StringUtil::fromString<uint16_t>(it->second, port)) {
                BS_LOG(ERROR, "invalid 'port' value [%s]", it->second.c_str());
                return false;
            }
        }
        if (params.find(COUNTER_PARAM_PASSWORD) == params.end()) {
            BS_LOG(ERROR, "missing [%s] in parameters", COUNTER_PARAM_PASSWORD.c_str());
            return false;
        }
        it = params.find(COUNTER_PARAM_REDIS_KEY_TTL);
        if (it != params.end()) {
            int32_t ttl;
            if (!StringUtil::fromString<int32_t>(it->second, ttl)) {
                BS_LOG(ERROR, "invalid [%s] value [%s], only integer is allowed",
                       COUNTER_PARAM_REDIS_KEY_TTL.c_str(), it->second.c_str());
                return false;
            }
        }
        
    }
    return true;
}

bool CounterConfig::operator == (const CounterConfig &other) const {
    if (position != other.position) {
        return false;
    }
    if (params.size() != other.params.size()) {
        return false;
    }
    auto itl = params.begin();
    auto itr = other.params.begin();

    for (; itl != params.end(); ++itl, ++itr) {
        if (itl->second != itr->second) {
            return false;
        }
    }
    return true;
}

bool CounterConfig::operator != (const CounterConfig &other) const {
    return !(*this == other);
}

}
}
