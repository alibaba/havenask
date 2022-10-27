#ifndef ISEARCH_BS_INDEXPARTITIONOPTIONSWRAPPER_H
#define ISEARCH_BS_INDEXPARTITIONOPTIONSWRAPPER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/config/ResourceReader.h"
#include <indexlib/config/index_partition_options.h>

namespace build_service {
namespace config {

class IndexPartitionOptionsWrapper
{
public:
    typedef std::map<std::string, IE_NAMESPACE(config)::IndexPartitionOptions> IndexPartitionOptionsMap;
    
public:
    bool load(const ResourceReader &resourceReader, const std::string &clusterName);
    bool hasIndexPartitionOptions(const std::string &configName) const;
    bool getIndexPartitionOptions(const std::string &configName,
                                  IE_NAMESPACE(config)::IndexPartitionOptions &indexOptions) const;
    
    const IndexPartitionOptionsMap& getIndexPartitionOptionsMap() const {
        return _indexPartitionOptionsMap;
    }

    static bool getFullMergeConfigName(
        ResourceReader* resourceReader, const std::string& dataTable,
        const std::string& clusterName, std::string& fullMergeConfig);
    
public:
    static bool getIndexPartitionOptions(
            const ResourceReader &resourceReader,
            const std::string &clusterName,
            const std::string &configName,
            IE_NAMESPACE(config)::IndexPartitionOptions &indexOptions);

    static bool initReservedVersions(const KeyValueMap& kvMap,
                                     IE_NAMESPACE(config)::IndexPartitionOptions &options);

    static void initOperationIds(const KeyValueMap& kvMap,
                                 IE_NAMESPACE(config)::IndexPartitionOptions &options);
    
private:
    IndexPartitionOptionsMap _indexPartitionOptionsMap;
    static const std::string DEFAULT_FULL_MERGE_CONFIG;
private:
    BS_LOG_DECLARE();
};

}
}

#endif //ISEARCH_BS_INDEXPARTITIONOPTIONSWRAPPER_H
