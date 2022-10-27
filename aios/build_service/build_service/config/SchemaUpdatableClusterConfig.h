#ifndef ISEARCH_BS_SCHEMAUPDATABLECLUSTERCONFIG_H
#define ISEARCH_BS_SCHEMAUPDATABLECLUSTERCONFIG_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include <autil/legacy/jsonizable.h>

namespace build_service {
namespace config {

class SchemaUpdatableClusterConfig : public autil::legacy::Jsonizable
{
public:
    SchemaUpdatableClusterConfig();
    ~SchemaUpdatableClusterConfig();

public:
    /* override */ void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json);
    bool validate() const;
    
public:
    std::string clusterName;
    uint32_t partitionCount;
    
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(SchemaUpdatableClusterConfig);
typedef std::vector<SchemaUpdatableClusterConfig> SchemaUpdatableClusterConfigVec;

}
}

#endif //ISEARCH_BS_SCHEMAUPDATABLECLUSTERCONFIG_H
