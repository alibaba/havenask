#ifndef ISEARCH_BS_DOCPROCESSORCHAINCONFIG_H
#define ISEARCH_BS_DOCPROCESSORCHAINCONFIG_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/plugin/ModuleInfo.h"
#include "build_service/config/ProcessorInfo.h"
#include <autil/legacy/jsonizable.h>

namespace build_service {
namespace config {

class DocProcessorChainConfig : public autil::legacy::Jsonizable
{
public:
    DocProcessorChainConfig();
    ~DocProcessorChainConfig();
public:
    /* override */ void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);
    bool validate() const;
public:
    std::vector<std::string> clusterNames;
    plugin::ModuleInfos moduleInfos;
    ProcessorInfos processorInfos;
    ProcessorInfos subProcessorInfos;
    uint32_t indexDocSerializeVersion;
    bool tolerateFormatError;
    bool useRawDocAsDoc;
    std::string chainName;
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(DocProcessorChainConfig);
typedef std::vector<DocProcessorChainConfig> DocProcessorChainConfigVec;
BS_TYPEDEF_PTR(DocProcessorChainConfigVec);

}
}

#endif //ISEARCH_BS_DOCPROCESSORCHAINCONFIG_H
