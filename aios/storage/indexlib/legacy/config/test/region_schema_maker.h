#pragma once

#include <memory>

#include "indexlib/config/index_partition_schema.h"
#include "indexlib/legacy/indexlib.h"

namespace indexlib { namespace test {

class RegionSchemaMaker
{
public:
    RegionSchemaMaker(const std::string& fieldNames, const std::string& tableType);
    ~RegionSchemaMaker();

public:
    void AddRegion(const std::string& regionName, const std::string& pkeyName, const std::string& skeyName,
                   const std::string& valueNames, const std::string& fieldNames, int64_t ttl = INVALID_TTL,
                   bool impactValue = false);

    void AddRegion(const std::string& regionName, const std::string& pkeyName, const std::string& valueNames,
                   const std::string& fieldNames, int64_t ttl = INVALID_TTL, bool impactValue = false);

    const config::IndexPartitionSchemaPtr& GetSchema() const;

private:
    config::IndexPartitionSchemaPtr mSchema;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<RegionSchemaMaker> RegionSchemaMakerPtr;
}} // namespace indexlib::test
