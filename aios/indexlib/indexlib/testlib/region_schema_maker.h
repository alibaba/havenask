#ifndef __INDEXLIB_REGION_SCHEMA_MAKER_H
#define __INDEXLIB_REGION_SCHEMA_MAKER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/testlib/schema_maker.h"

IE_NAMESPACE_BEGIN(testlib);

class RegionSchemaMaker
{
public:
    RegionSchemaMaker(const std::string& fieldNames,
                      const std::string& tableType);
    ~RegionSchemaMaker();
    
public:
    void AddRegion(const std::string& regionName,
                   const std::string& pkeyName,
                   const std::string& skeyName,
                   const std::string& valueNames,
                   const std::string& fieldNames,
                   int64_t ttl = INVALID_TTL);

    void AddRegion(const std::string& regionName,
                   const std::string& pkeyName,
                   const std::string& valueNames,
                   const std::string& fieldNames,
                   int64_t ttl = INVALID_TTL);

    const config::IndexPartitionSchemaPtr& GetSchema() const;

private:
    config::IndexPartitionSchemaPtr mSchema;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(RegionSchemaMaker);

IE_NAMESPACE_END(testlib);

#endif //__INDEXLIB_REGION_SCHEMA_MAKER_H
