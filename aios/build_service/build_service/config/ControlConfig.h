#ifndef ISEARCH_BS_CONTROLCONFIG_H
#define ISEARCH_BS_CONTROLCONFIG_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include <autil/legacy/jsonizable.h>

namespace build_service {
namespace config {
class ResourceReader;

class ControlConfig : public autil::legacy::Jsonizable
{
public:
    ControlConfig()
        : isIncProcessorExist(true) {}
    ~ControlConfig() {}
    
public:    
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
public:
    bool isIncProcessorExist{true};
};

}
}

#endif //ISEARCH_BS_CONTROLCONFIG_H
