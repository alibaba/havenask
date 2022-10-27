#ifndef ISEARCH_BS_BUILDRULECONFIG_H
#define ISEARCH_BS_BUILDRULECONFIG_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include <autil/legacy/jsonizable.h>

namespace build_service {
namespace config {

class BuildRuleConfig : public autil::legacy::Jsonizable
{
public:
    BuildRuleConfig();
    ~BuildRuleConfig();
public:
    /* override */ void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json);
public:
    bool validate() const;
public:
    uint32_t partitionCount;
    uint32_t buildParallelNum;
    uint32_t incBuildParallelNum;    
    uint32_t mergeParallelNum;
    uint32_t mapReduceRatio;
    uint32_t partitionSplitNum;
    bool needPartition;
    bool alignVersion;
    bool disableIncParallelInstanceDir;
    
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(BuildRuleConfig);

}
}

#endif //ISEARCH_BS_BUILDRULECONFIG_H
