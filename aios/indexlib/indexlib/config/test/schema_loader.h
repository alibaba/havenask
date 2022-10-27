#ifndef __INDEXLIB_TEST_SCHEMA_LOADER_H
#define __INDEXLIB_TEST_SCHEMA_LOADER_H

#include <tr1/memory>
#include <fslib/fslib.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"

IE_NAMESPACE_BEGIN(config);

class SchemaLoader
{
public:
    SchemaLoader();
    ~SchemaLoader();
public:
    static config::IndexPartitionSchemaPtr LoadSchema(
            const std::string& path, const std::string& schemaFileName);

    static void RewriteToRtSchema(
            const config::IndexPartitionSchemaPtr& schema)
    {
        RewriteForRealtimeTimestamp(schema);
        RewriteForRemoveTFBitmapFlag(schema);
    }

private:
    static void RewriteForRealtimeTimestamp(
            const config::IndexPartitionSchemaPtr& schema);
    
    static void RewriteForRemoveTFBitmapFlag(
            const config::IndexPartitionSchemaPtr& schema);

    static void DoRewriteForRemoveTFBitmapFlag(
            const config::IndexPartitionSchemaPtr& schema);
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SchemaLoader);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_TEST_SCHEMA_LOADER_H
