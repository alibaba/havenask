#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/config/schema_configurator.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/index_define.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/version_loader.h"
#include "indexlib/index_base/schema_rewriter.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/high_freq_vocabulary_creator.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/storage/archive_folder.h"

using namespace std;
using namespace autil;
using namespace fslib;
using namespace autil::legacy::json;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(index_base);
IE_LOG_SETUP(index_base, SchemaAdapter);

SchemaAdapter::SchemaAdapter() 
{
}

SchemaAdapter::~SchemaAdapter() 
{
}

IndexPartitionSchemaPtr SchemaAdapter::LoadSchema(
        const std::string& root, schemavid_t schemaId)
{
    IndexPartitionSchemaPtr schema = LoadSchema(root,
            Version::GetSchemaFileName(schemaId));
    if (schema && schema->GetSchemaVersionId() != schemaId)
    {
        INDEXLIB_FATAL_ERROR(InconsistentState,
                             "schema version id not match: in schema file[%u], target[%u]",
                             schema->GetSchemaVersionId(), schemaId);
    }
    return schema;
}

IndexPartitionSchemaPtr SchemaAdapter::LoadSchema(
        const DirectoryPtr& rootDir, schemavid_t schemaId)
{
    IndexPartitionSchemaPtr schema = LoadSchema(rootDir,
            Version::GetSchemaFileName(schemaId));
    if (schema && schema->GetSchemaVersionId() != schemaId)
    {
        INDEXLIB_FATAL_ERROR(InconsistentState,
                             "schema version id not match: in schema file[%u], target[%u]",
                             schema->GetSchemaVersionId(), schemaId);
    }
    return schema;
}

IndexPartitionSchemaPtr SchemaAdapter::LoadAndRewritePartitionSchema(
        const std::string& root, const IndexPartitionOptions& options, schemavid_t schemaId)
{
    IndexPartitionSchemaPtr schemaPtr = LoadSchema(root, schemaId);
    
    if (!schemaPtr)
    {
        return IndexPartitionSchemaPtr();
    }
    return RewritePartitionSchema(schemaPtr, root, options);
}

IndexPartitionSchemaPtr SchemaAdapter::LoadAndRewritePartitionSchema(
        const DirectoryPtr& rootDir, const IndexPartitionOptions& options, schemavid_t schemaId)
{
    IndexPartitionSchemaPtr schemaPtr = LoadSchema(rootDir, schemaId);
    if (schemaPtr)
    {
        return RewritePartitionSchema(schemaPtr, rootDir, options);
    }
    if (options.IsOnline())
    {
        string schemaPath = FileSystemWrapper::JoinPath(rootDir->GetPath(),
                Version::GetSchemaFileName(schemaId));
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "[%s] not exist",
                             schemaPath.c_str());        
    }
    return schemaPtr;
}

IndexPartitionSchemaPtr SchemaAdapter::RewritePartitionSchema(
        const IndexPartitionSchemaPtr& schema,
        const string& root, const IndexPartitionOptions& options)
{
    if (schema->GetTableType() == tt_customized)
    {
        return schema;
    }
    if (options.IsOnline() || !options.GetOfflineConfig().NeedRebuildAdaptiveBitmap())
    {
        LoadAdaptiveBitmapTerms(root, schema);
        IndexPartitionSchemaPtr subSchema = schema->GetSubIndexPartitionSchema();
        if (subSchema)
        {
            LoadAdaptiveBitmapTerms(root, subSchema);
        }
    }
    SchemaRewriter::Rewrite(schema, options, root);
    return schema;
}

IndexPartitionSchemaPtr SchemaAdapter::RewritePartitionSchema(
        const IndexPartitionSchemaPtr& schema,
        const DirectoryPtr& rootDir, const IndexPartitionOptions& options)
{
    if (schema->GetTableType() == tt_customized)
    {
        return schema;
    }
    if (options.IsOnline() || !options.GetOfflineConfig().NeedRebuildAdaptiveBitmap())
    {
        LoadAdaptiveBitmapTerms(rootDir, schema);
        IndexPartitionSchemaPtr subSchema = schema->GetSubIndexPartitionSchema();
        if (subSchema)
        {
            LoadAdaptiveBitmapTerms(rootDir, subSchema);
        }
    }
    SchemaRewriter::Rewrite(schema, options, rootDir);
    return schema;
}

void SchemaAdapter::LoadSchema(
    const string& schemaJson, IndexPartitionSchemaPtr& schema)
{
    try
    {
        schema.reset(new IndexPartitionSchema());
        FromJsonString(*schema, schemaJson);
        schema->Check();        
    }
    catch (...)
    {
        schema.reset();
        throw;
    }
}

IndexPartitionSchemaPtr SchemaAdapter::LoadSchema(
        const std::string& root, const std::string& schemaFileName)
{
    string schemaFilePath = FileSystemWrapper::JoinPath(root, schemaFileName);
    string jsonString;
    try
    {
        if (!FileSystemWrapper::AtomicLoad(schemaFilePath, jsonString, true))
        {
            return IndexPartitionSchemaPtr();
        }
    }
    catch (...)
    {
        throw;
    }
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema());
    FromJsonString(*schema, jsonString);
    schema->Check();
    return schema;
}

IndexPartitionSchemaPtr SchemaAdapter::LoadSchemaForVersion(
        const string& root, versionid_t versionId)
{
    Version version;
    VersionLoader::GetVersion(root, version, versionId);
    return LoadSchema(root, version.GetSchemaVersionId());
}

IndexPartitionSchemaPtr SchemaAdapter::LoadSchema(
        const DirectoryPtr& rootDir, const string& schemaFileName)
{
    string jsonString;
    if (!rootDir || !rootDir->LoadMayNonExist(schemaFileName, jsonString, true))
    {
        return IndexPartitionSchemaPtr();
    }
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema());
    FromJsonString(*schema, jsonString);
    schema->Check();
    return schema;
}

void SchemaAdapter::StoreSchema(const string& dir, 
                                const IndexPartitionSchemaPtr& schema)
{
    autil::legacy::Any any = autil::legacy::ToJson(*schema);
    string str = ToString(any);
    string filePath = FileSystemWrapper::JoinPath(
            dir, Version::GetSchemaFileName(schema->GetSchemaVersionId()));
    FileSystemWrapper::AtomicStore(filePath, str);
}

void SchemaAdapter::LoadAdaptiveBitmapTerms(const std::string& dir,
            const IndexPartitionSchemaPtr& schema)
{
    if (schema->GetTableType() != tt_index)
    {
        return;
    }
    
    vector<IndexConfigPtr> adaptiveDictIndexConfigs;
    IndexSchemaPtr indexSchema = schema->GetIndexSchema();
    auto indexConfigs = indexSchema->CreateIterator(true);
    auto iter = indexConfigs->Begin();
    for (; iter != indexConfigs->End(); iter++)
    {
        const IndexConfigPtr& indexConfig = *iter;
        if (indexConfig->GetShardingIndexConfigs().size() > 0)
        {
            // if is sharding index, don't read HFV for original index name
            continue;
        }
        if (indexConfig->GetAdaptiveDictionaryConfig())
        {
            adaptiveDictIndexConfigs.push_back(indexConfig);
        }
    }

    string adaptiveDictPath = FileSystemWrapper::JoinPath(
            dir, ADAPTIVE_DICT_DIR_NAME);
    if (adaptiveDictIndexConfigs.empty() || !FileSystemWrapper::IsExist(adaptiveDictPath))
    {
        return;
    }

    ArchiveFolderPtr adaptiveDictFolder(new ArchiveFolder(true));
    adaptiveDictFolder->Open(adaptiveDictPath);
    for (size_t i = 0; i < adaptiveDictIndexConfigs.size(); i++) {
        IndexConfigPtr indexConfig = adaptiveDictIndexConfigs[i];
        HighFrequencyVocabularyPtr vol = 
            HighFreqVocabularyCreator::LoadAdaptiveVocabulary(
                    adaptiveDictFolder, indexConfig->GetIndexName(),
                    indexConfig->GetIndexType(), indexConfig->GetDictConfig());
        if (vol)
        {
            indexConfig->SetHighFreqVocabulary(vol);
        }
    }
    adaptiveDictFolder->Close();
}

void SchemaAdapter::LoadAdaptiveBitmapTerms(const DirectoryPtr& rootDir,
            const IndexPartitionSchemaPtr& schema)
{
    if (schema->GetTableType() != tt_index)
    {
        return;
    }
    
    vector<IndexConfigPtr> adaptiveDictIndexConfigs;
    IndexSchemaPtr indexSchema = schema->GetIndexSchema();
    auto indexConfigs = indexSchema->CreateIterator(true);
    auto iter = indexConfigs->Begin();
    for (; iter != indexConfigs->End(); iter++)
    {
        const IndexConfigPtr& indexConfig = *iter;
        if (indexConfig->GetShardingIndexConfigs().size() > 0)
        {
            // if is sharding index, don't read HFV for original index name
            continue;
        }
        if (indexConfig->GetAdaptiveDictionaryConfig())
        {
            adaptiveDictIndexConfigs.push_back(indexConfig);
        }
    }

    if (adaptiveDictIndexConfigs.empty())
    {
        return;
    }
    const DirectoryPtr& directory = rootDir->GetDirectory(ADAPTIVE_DICT_DIR_NAME, false);
    if (!directory)
    {
        return;
    }
    ArchiveFolderPtr adaptiveDictFolder = directory->GetArchiveFolder();
    for (size_t i = 0; i < adaptiveDictIndexConfigs.size(); i++) {
        IndexConfigPtr indexConfig = adaptiveDictIndexConfigs[i];
        HighFrequencyVocabularyPtr vol = 
            HighFreqVocabularyCreator::LoadAdaptiveVocabulary(
                    adaptiveDictFolder, indexConfig->GetIndexName(),
                    indexConfig->GetIndexType(), indexConfig->GetDictConfig());
        if (vol)
        {
            indexConfig->SetHighFreqVocabulary(vol);
        }
    }
    adaptiveDictFolder->Close();
}

void SchemaAdapter::RewriteToRtSchema(
        const IndexPartitionSchemaPtr& schema)
{
    assert(schema);
    SchemaRewriter::RewriteForRealtimeTimestamp(schema);
    SchemaRewriter::RewriteForRemoveTFBitmapFlag(schema);
}

void SchemaAdapter::ListSchemaFile(const string& root, FileList &fileList)
{
    FileList tmpFiles;
    FileSystemWrapper::ListDir(root, tmpFiles);
    for (const auto& file : tmpFiles)
    {
        schemavid_t schemaId = DEFAULT_SCHEMAID;
        if (Version::ExtractSchemaIdBySchemaFile(file, schemaId))
        {
            fileList.push_back(file);
        }
    }
}

void SchemaAdapter::ListSchemaFile(const DirectoryPtr& rootDir, FileList &fileList)
{
    if (!rootDir)
    {
        return;
    }
    
    FileList tmpFiles;
    rootDir->ListFile("", tmpFiles);
    for (const auto& file : tmpFiles)
    {
        schemavid_t schemaId = DEFAULT_SCHEMAID;
        if (Version::ExtractSchemaIdBySchemaFile(file, schemaId))
        {
            fileList.push_back(file);
        }
    }
}

IndexPartitionSchemaPtr SchemaAdapter::LoadSchema(const string& root)
{
    Version version;
    VersionLoader::GetVersion(root, version, INVALID_VERSION);
    return LoadSchema(root, version.GetSchemaVersionId());
}

IE_NAMESPACE_END(index_base);

