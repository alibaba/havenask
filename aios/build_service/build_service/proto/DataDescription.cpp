#include "build_service/proto/DataDescription.h"
#include "build_service/config/CLIOptionNames.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;

namespace build_service {
namespace proto {
BS_LOG_SETUP(proto, DataDescription);

DataDescription::DataDescription() {
}

DataDescription::~DataDescription() {
}

bool DataDescription::operator == (const DataDescription& other) const {
    if (kvMap != other.kvMap) {
        return false;
    }

    if (parserConfigs.size() != other.parserConfigs.size()) {
        return false;
    }

    for (size_t i = 0; i < parserConfigs.size(); ++i) {
        if (parserConfigs[i] != other.parserConfigs[i]) {
            return false;
        }
    }
    return true;
}

void DataDescription::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    if (json.GetMode() == TO_JSON) {
        for (auto& kv : kvMap) {
            json.Jsonize(kv.first, kv.second);
        }
        if (!parserConfigs.empty()) {
            json.Jsonize(config::DATA_DESCRIPTION_PARSER_CONFIG, parserConfigs);
        }
    } else {
        kvMap.clear();
        parserConfigs.clear();
        json.Jsonize(config::DATA_DESCRIPTION_PARSER_CONFIG, parserConfigs, parserConfigs);
        auto jsonMap = json.GetMap();
        for (const auto& kv : jsonMap) {
            if (kv.first != config::DATA_DESCRIPTION_PARSER_CONFIG) {
                KeyValueMap::mapped_type v;
                FromJson(v, kv.second);
                kvMap.insert(make_pair(kv.first, v));
            }
        }
    }
}
}
}
