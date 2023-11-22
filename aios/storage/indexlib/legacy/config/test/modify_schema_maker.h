#pragma once
#include <memory>

#include "indexlib/config/index_partition_schema.h"
#include "indexlib/legacy/indexlib.h"

namespace indexlib { namespace test {

class ModifySchemaMaker
{
public:
    ModifySchemaMaker();
    ~ModifySchemaMaker();

public:
    static void AddModifyOperations(const config::IndexPartitionSchemaPtr& schema,
                                    const std::string& deleteIndexInfo, /* fields=a,b,c;indexs=i1,i2;attributes=a1 */
                                    const std::string& addFieldNames, const std::string& addIndexNames,
                                    const std::string& addAttributeNames);

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<ModifySchemaMaker> ModifySchemaMakerPtr;
}} // namespace indexlib::test
