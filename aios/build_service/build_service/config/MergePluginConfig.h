#ifndef ISEARCH_BS_MERGEPLUGINCONFIG_H
#define ISEARCH_BS_MERGEPLUGINCONFIG_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include <autil/legacy/jsonizable.h>
#include "build_service/plugin/ModuleInfo.h"
#include "build_service/config/ProcessorInfo.h"

namespace build_service {
namespace config {

class MergePluginConfig : public autil::legacy::Jsonizable
{
public:
    MergePluginConfig();
    ~MergePluginConfig();

public:
/* override */ void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json);
    const plugin::ModuleInfo& getModuleInfo() const { return _moduleInfo; }
    const ProcessorInfo& getProcessorInfo() const { return _processorInfo; }
private:
    plugin::ModuleInfo _moduleInfo;
    ProcessorInfo _processorInfo;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(MergePluginConfig);

}
}

#endif //ISEARCH_BS_MERGEPLUGINCONFIG_H
