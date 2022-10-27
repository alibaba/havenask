#ifndef __INDEXLIB_SCHEMA_ADAPTER_H
#define __INDEXLIB_SCHEMA_ADAPTER_H

#include <tr1/memory>
#include <fslib/fslib.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(config, IndexPartitionOptions);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);

IE_NAMESPACE_BEGIN(index_base);

// TODO: add UT
class SchemaAdapter
{
public:
    SchemaAdapter();
    ~SchemaAdapter();
public:
    static config::IndexPartitionSchemaPtr LoadSchema(
            const file_system::DirectoryPtr& rootDir, schemavid_t schemaId);
    static config::IndexPartitionSchemaPtr LoadSchema(
            const file_system::DirectoryPtr& rootDir, const std::string& schemaFileName);
    static config::IndexPartitionSchemaPtr LoadAndRewritePartitionSchema(
            const file_system::DirectoryPtr& rootDir, const config::IndexPartitionOptions& options,
            schemavid_t schemaId = DEFAULT_SCHEMAID);
    static config::IndexPartitionSchemaPtr RewritePartitionSchema(
            const config::IndexPartitionSchemaPtr& schema,
            const file_system::DirectoryPtr& rootDir, const config::IndexPartitionOptions& options);
    static void ListSchemaFile(const file_system::DirectoryPtr& rootDir, fslib::FileList &fileList);

public:
    // load schema according to version
    static config::IndexPartitionSchemaPtr LoadSchemaForVersion(
            const std::string& path, versionid_t versionId = INVALID_VERSION);
    
    static config::IndexPartitionSchemaPtr LoadSchema(
            const std::string& path, const std::string& schemaFileName);
    static void LoadSchema(
        const std::string& schemaJson, config::IndexPartitionSchemaPtr& schema);

    // load schema according to latest version
    static config::IndexPartitionSchemaPtr LoadSchema(const std::string& root);
    static config::IndexPartitionSchemaPtr LoadSchema(const std::string& root, schemavid_t schemaId);

    static void StoreSchema(const std::string& dir, 
                            const config::IndexPartitionSchemaPtr& schema);

    static config::IndexPartitionSchemaPtr LoadAndRewritePartitionSchema(
            const std::string& root, const config::IndexPartitionOptions& options,
            schemavid_t schemaId = DEFAULT_SCHEMAID);
    
    static config::IndexPartitionSchemaPtr RewritePartitionSchema(
            const config::IndexPartitionSchemaPtr& schema,
            const std::string& root, const config::IndexPartitionOptions& options);

    static void RewriteToRtSchema(
            const config::IndexPartitionSchemaPtr& schema);

    static void ListSchemaFile(const std::string& root, fslib::FileList &fileList);


private:
    static void LoadAdaptiveBitmapTerms(const std::string& dir,
            const config::IndexPartitionSchemaPtr& schema);
    static void LoadAdaptiveBitmapTerms(const file_system::DirectoryPtr& rootDir,
            const config::IndexPartitionSchemaPtr& schema);
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SchemaAdapter);

IE_NAMESPACE_END(index_base);

#endif //__INDEXLIB_SCHEMA_ADAPTER_H
