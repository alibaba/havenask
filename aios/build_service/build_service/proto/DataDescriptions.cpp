#include "build_service/proto/DataDescriptions.h"
#include "build_service/config/CLIOptionNames.h"
#include <autil/legacy/jsonizable.h>
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
using namespace build_service::config;

namespace build_service {
namespace proto {
BS_LOG_SETUP(config, DataDescriptions);

DataDescriptions::DataDescriptions() {
}

DataDescriptions::~DataDescriptions() {
}

bool DataDescriptions::validate() const {
    set<string> srcs;
    for (size_t i = 0; i < _dataDescriptions.size(); i++) {
        const DataDescription &dataDescription = _dataDescriptions[i];
        string src = getValueFromKeyValueMap(dataDescription.kvMap, READ_SRC);
        string type = getValueFromKeyValueMap(dataDescription.kvMap, READ_SRC_TYPE);
        if (src.empty() || type.empty()) {
            string errorMsg = "data description[" + autil::legacy::ToJsonString(dataDescription)
                              + "] must have src and type";
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return false;
        }
        if (srcs.end() != srcs.find(src)) {
            BS_LOG(INFO, "multi src [%s] in data description", src.c_str());
            return false;
        }

        string fullProcessorCountStr = getValueFromKeyValueMap(dataDescription.kvMap, "full_processor_count");
        if (!fullProcessorCountStr.empty()) {
            uint32_t fullProcessorCount = 0;
            if (!StringUtil::fromString(fullProcessorCountStr, fullProcessorCount)) {
                BS_LOG(ERROR, "invalid full_processor_count[%s], only integer is allowed",
                       fullProcessorCountStr.c_str());
                return false;
            }
            if (fullProcessorCount == 0) {
                BS_LOG(ERROR, "invalid full_processor_count[%s], only positive integer is allowed",
                       fullProcessorCountStr.c_str());
                return false;
            }
        }
        srcs.insert(src);
    }
    return true;
}

}
}
