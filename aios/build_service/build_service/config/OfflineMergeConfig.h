#ifndef ISEARCH_BS_OFFLINEMERGECONFIG_H
#define ISEARCH_BS_OFFLINEMERGECONFIG_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/config/MergePluginConfig.h"
#include <indexlib/config/merge_config.h>
#include <autil/legacy/jsonizable.h>

namespace build_service {
namespace config {

class OfflineMergeConfig : public autil::legacy::Jsonizable
{
public:
    OfflineMergeConfig();
    ~OfflineMergeConfig();
public:
/* override */ void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json);

public:
    IE_NAMESPACE(config)::MergeConfig mergeConfig;
    std::string periodMergeDescription;
    uint32_t mergeParallelNum;
    uint32_t mergeSleepTime; // for test only
    MergePluginConfig mergePluginConfig;
    bool needWaitAlterField;
    
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(OfflineMergeConfig);

}
}

#endif //ISEARCH_BS_OFFLINEMERGECONFIG_H
