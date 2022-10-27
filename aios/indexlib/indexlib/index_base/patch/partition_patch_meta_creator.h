#ifndef __INDEXLIB_PARTITION_PATCH_META_CREATOR_H
#define __INDEXLIB_PARTITION_PATCH_META_CREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(index_base, Version);
DECLARE_REFERENCE_CLASS(index_base, PartitionPatchMeta);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);

IE_NAMESPACE_BEGIN(index_base);

class PartitionPatchMetaCreator
{
public:
    PartitionPatchMetaCreator();
    ~PartitionPatchMetaCreator();
    
public:
    static PartitionPatchMetaPtr Create(
            const std::string& rootPath,
            const config::IndexPartitionSchemaPtr& schema,
            const index_base::Version& version);

    static PartitionPatchMetaPtr Create(
            const file_system::DirectoryPtr& rootDir,
            const config::IndexPartitionSchemaPtr& schema,
            const index_base::Version& version);

private:
    static PartitionPatchMetaPtr GetLastPartitionPatchMeta(
            const std::string& rootPath, schemavid_t& lastSchemaId);

    static void GetPatchIndexInfos(const std::string& segPath,
                                   const config::IndexPartitionSchemaPtr& schema,
                                   std::vector<std::string>& indexNames,
                                   std::vector<std::string>& attrNames);

    static PartitionPatchMetaPtr CreatePatchMeta(
            const std::string& rootPath,
            const config::IndexPartitionSchemaPtr& schema,
            const index_base::Version& version,
            const PartitionPatchMetaPtr& lastPatchMeta,
            schemavid_t lastSchemaId);

    static PartitionPatchMetaPtr CreatePatchMeta(
            const std::string& rootPath,
            const config::IndexPartitionSchemaPtr& schema,
            const index_base::Version& version,
            const std::vector<schemavid_t>& schemaIdVec,
            const PartitionPatchMetaPtr& lastPatchMeta);

    static void GetValidIndexAndAttribute(
            const config::IndexPartitionSchemaPtr& schema,
            const std::vector<std::string>& inputIndexNames,
            const std::vector<std::string>& inputAttrNames,
            std::vector<std::string>& indexNames,
            std::vector<std::string>& attrNames);

    static PartitionPatchMetaPtr CreatePatchMetaForModifySchema(
            const std::string& rootPath,
            const config::IndexPartitionSchemaPtr& schema,
            const index_base::Version& version);

    static bool GetValidIndexAndAttributeForModifyOperation(
            const config::IndexPartitionSchemaPtr& schema, schema_opid_t opId,
            std::vector<std::string>& indexNames, std::vector<std::string>& attrNames);
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PartitionPatchMetaCreator);

IE_NAMESPACE_END(index_base);

#endif //__INDEXLIB_PARTITION_PATCH_META_CREATOR_H
