#pragma once

#include <memory>

#include "autil/Log.h"
#include "fslib/fslib.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/legacy/indexlib.h"

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
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SchemaLoader> SchemaLoaderPtr;
}} // namespace indexlib::config
