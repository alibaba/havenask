#ifndef __INDEXLIB_MODIFY_SCHEMA_MAKER_H
#define __INDEXLIB_MODIFY_SCHEMA_MAKER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/indexlib.h"

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
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ModifySchemaMaker);
}} // namespace indexlib::test

#endif //__INDEXLIB_MODIFY_SCHEMA_MAKER_H
