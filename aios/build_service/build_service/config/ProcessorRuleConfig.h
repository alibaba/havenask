#ifndef ISEARCH_BS_PROCESSORRULECONFIG_H
#define ISEARCH_BS_PROCESSORRULECONFIG_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include <autil/legacy/jsonizable.h>

namespace build_service {
namespace config {

class ProcessorRuleConfig : public autil::legacy::Jsonizable
{
public:
    ProcessorRuleConfig();
    ~ProcessorRuleConfig();
public:
    /* override */ void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);
    bool validate() const;
public:
    uint32_t partitionCount;
    uint32_t parallelNum;
private:
    BS_LOG_DECLARE();
};

}
}

#endif //ISEARCH_BS_PROCESSORRULECONFIG_H
