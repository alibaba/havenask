#ifndef __INDEXLIB_TEST_SCHEMA_LOADER_H
#define __INDEXLIB_TEST_SCHEMA_LOADER_H

#include <memory>

#include "fslib/fslib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace config {

class SchemaLoader
{
public:
    SchemaLoader();
    ~SchemaLoader();

public:
    static config::IndexPartitionSchemaPtr LoadSchema(const std::string& path, const std::string& schemaFileName);

    static void RewriteToRtSchema(const config::IndexPartitionSchemaPtr& schema)
    {
        RewriteForRealtimeTimestamp(schema);
        RewriteForRemoveTFBitmapFlag(schema);
    }

private:
    static void RewriteForRealtimeTimestamp(const config::IndexPartitionSchemaPtr& schema);

    static void RewriteForRemoveTFBitmapFlag(const config::IndexPartitionSchemaPtr& schema);

    static void DoRewriteForRemoveTFBitmapFlag(const config::IndexPartitionSchemaPtr& schema);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SchemaLoader);
}} // namespace indexlib::config

#endif //__INDEXLIB_TEST_SCHEMA_LOADER_H
