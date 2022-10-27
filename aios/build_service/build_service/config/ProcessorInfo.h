#ifndef ISEARCH_BS_PROCESSORINFO_H
#define ISEARCH_BS_PROCESSORINFO_H


#include <autil/legacy/jsonizable.h>
#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service {
namespace config {

class ProcessorInfo : public autil::legacy::Jsonizable
{
public:
    ProcessorInfo(const std::string &className_ = "",
                  const std::string &moduleName_ = "",
                  const KeyValueMap &parameters_ = KeyValueMap())
        : className(className_)
        , moduleName(moduleName_)
        , parameters(parameters_)
    {
    }
    ~ProcessorInfo() {
    }
public:
    /* override */ void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);
    bool operator== (const ProcessorInfo &other) {
        return className == other.className
            && moduleName == other.moduleName
            && parameters == other.parameters;
    }
public:
    std::string className;
    std::string moduleName;
    KeyValueMap parameters;
};

typedef std::vector<ProcessorInfo> ProcessorInfos;

}
}

#endif //ISEARCH_BS_PROCESSORINFO_H
