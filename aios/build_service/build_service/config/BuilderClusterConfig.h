#ifndef ISEARCH_BS_BUILDERCLUSTERCONFIG_H
#define ISEARCH_BS_BUILDERCLUSTERCONFIG_H

#include "build_service/common_define.h"
#include "build_service/config/BuilderConfig.h"
#include "build_service/util/Log.h"
#include "build_service/config/ResourceReader.h"
#include <indexlib/config/index_partition_options.h>

namespace build_service {
namespace config {

class BuilderClusterConfig
{
public:
    BuilderClusterConfig();
    ~BuilderClusterConfig();
private:
    BuilderClusterConfig(const BuilderClusterConfig &);
    BuilderClusterConfig& operator=(const BuilderClusterConfig &);
public:
    bool init(const std::string &clusterName,
              const config::ResourceReader &resourceReader,
              const std::string &mergeConfigName = "");
    bool validate() const;
public:
    std::string clusterName;
    std::string tableName;
    BuilderConfig builderConfig;
    IE_NAMESPACE(config)::IndexPartitionOptions indexOptions;
    bool overwriteSchemaName = false;
private:
    BS_LOG_DECLARE();
};

}
}

#endif //ISEARCH_BS_BUILDERCLUSTERCONFIG_H
