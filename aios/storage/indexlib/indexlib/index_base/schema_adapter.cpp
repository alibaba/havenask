/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "indexlib/index_base/schema_adapter.h"

#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/legacy_schema_adapter.h"
#include "indexlib/config/schema_configurator.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/framework/TabletSchemaLoader.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/index_meta/version_loader.h"
#include "indexlib/index_base/schema_rewriter.h"
#include "indexlib/index_define.h"
#include "indexlib/legacy/index_base/SchemaAdapter.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace autil;
using namespace fslib;
using namespace autil::legacy::json;

using namespace indexlib::config;
using namespace indexlib::util;
using namespace indexlib::file_system;

namespace indexlib { namespace index_base {
IE_LOG_SETUP(index_base, SchemaAdapter);

SchemaAdapter::SchemaAdapter() {}

SchemaAdapter::~SchemaAdapter() {}

IndexPartitionSchemaPtr SchemaAdapter::LoadSchema(const DirectoryPtr& rootDir, schemavid_t schemaId)
{
    IndexPartitionSchemaPtr schema = LoadSchema(rootDir, Version::GetSchemaFileName(schemaId));
    if (schema && schema->GetSchemaVersionId() != schemaId) {
        INDEXLIB_FATAL_ERROR(InconsistentState, "schema version id not match: in schema file[%u], target[%u]",
                             schema->GetSchemaVersionId(), schemaId);
    }
    return schema;
}

IndexPartitionSchemaPtr SchemaAdapter::LoadAndRewritePartitionSchema(const DirectoryPtr& rootDir,
                                                                     const IndexPartitionOptions& options,
                                                                     schemavid_t schemaId)
{
    IndexPartitionSchemaPtr schemaPtr = LoadSchema(rootDir, schemaId);
    if (schemaPtr) {
        return RewritePartitionSchema(schemaPtr, rootDir, options);
    }
    if (options.IsOnline()) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "[%s] not in [%s] exist", Version::GetSchemaFileName(schemaId).c_str(),
                             rootDir->DebugString().c_str());
    }
    return schemaPtr;
}

IndexPartitionSchemaPtr SchemaAdapter::RewritePartitionSchema(const IndexPartitionSchemaPtr& schema,
                                                              const DirectoryPtr& rootDir,
                                                              const IndexPartitionOptions& options)
{
    if (schema->GetTableType() == tt_customized) {
        return schema;
    }
    if (options.IsOnline() || !options.GetOfflineConfig().NeedRebuildAdaptiveBitmap()) {
        LoadAdaptiveBitmapTerms(rootDir, schema);
        IndexPartitionSchemaPtr subSchema = schema->GetSubIndexPartitionSchema();
        if (subSchema) {
            LoadAdaptiveBitmapTerms(rootDir, subSchema);
        }
    }
    SchemaRewriter::Rewrite(schema, options, rootDir);
    return schema;
}

IndexPartitionSchemaPtr SchemaAdapter::LoadSchemaForVersion(const file_system::DirectoryPtr& dir, versionid_t versionId)
{
    Version version;
    VersionLoader::GetVersion(dir, version, versionId);
    return LoadSchema(dir, version.GetSchemaVersionId());
}

IndexPartitionSchemaPtr SchemaAdapter::LoadSchema(const DirectoryPtr& rootDir, const string& schemaFileName)
{
    string jsonString;
    if (!rootDir || !rootDir->LoadMayNonExist(schemaFileName, jsonString, true)) {
        return IndexPartitionSchemaPtr();
    }
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema());
    schema->SetLoadFromIndex(true);
    FromJsonString(*schema, jsonString);
    schema->Check();
    return schema;
}

void SchemaAdapter::StoreSchema(const file_system::DirectoryPtr& directory, const IndexPartitionSchemaPtr& schema)
{
    if (directory == nullptr) {
        return;
    }
    autil::legacy::Any any = autil::legacy::ToJson(*schema);
    string str = ToString(any);
    directory->Store(Version::GetSchemaFileName(schema->GetSchemaVersionId()), str, WriterOption::BufferAtomicDump());
}

void SchemaAdapter::StoreSchema(const string& dir, const IndexPartitionSchemaPtr& schema, FenceContext* fenceContext)
{
    autil::legacy::Any any = autil::legacy::ToJson(*schema);
    string str = ToString(any);
    string filePath = PathUtil::JoinPath(dir, Version::GetSchemaFileName(schema->GetSchemaVersionId()));
    auto ec = FslibWrapper::AtomicStore(filePath, str, false, fenceContext).Code();
    THROW_IF_FS_ERROR(ec, "store schema failed. [%s]", filePath.c_str());
}

void SchemaAdapter::LoadAdaptiveBitmapTerms(const DirectoryPtr& rootDir, const IndexPartitionSchemaPtr& schema)
{
    LoadAdaptiveBitmapTerms(rootDir, schema.get());
}

void SchemaAdapter::LoadAdaptiveBitmapTerms(const DirectoryPtr& rootDir, IndexPartitionSchema* schema)
{
    indexlib::legacy::index_base::SchemaAdapter::LoadAdaptiveBitmapTerms(rootDir, schema);
}

void SchemaAdapter::RewriteToRtSchema(const IndexPartitionSchemaPtr& schema)
{
    assert(schema);
    SchemaRewriter::RewriteForRealtimeTimestamp(schema);
    SchemaRewriter::RewriteForRemoveTFBitmapFlag(schema);
}

void SchemaAdapter::ListSchemaFile(const DirectoryPtr& rootDir, FileList& fileList)
{
    if (!rootDir) {
        return;
    }

    FileList tmpFiles;
    rootDir->ListDir("", tmpFiles);
    for (const auto& file : tmpFiles) {
        schemavid_t schemaId = DEFAULT_SCHEMAID;
        if (Version::ExtractSchemaIdBySchemaFile(file, schemaId)) {
            fileList.push_back(file);
        }
    }
}

IndexPartitionSchemaPtr SchemaAdapter::LoadSchema(const file_system::DirectoryPtr& rootDirectory)
{
    Version version;
    VersionLoader::GetVersion(rootDirectory, version, INVALID_VERSION);
    return LoadSchema(rootDirectory, version.GetSchemaVersionId());
}

void SchemaAdapter::LoadSchema(const string& schemaJson, IndexPartitionSchemaPtr& schema)
{
    try {
        schema.reset(new IndexPartitionSchema());
        FromJsonString(*schema, schemaJson);
        schema->Check();
    } catch (...) {
        schema.reset();
        throw;
    }
}

std::unique_ptr<indexlibv2::config::ITabletSchema> SchemaAdapter::LoadSchema(const std::string& jsonStr)
{
    try {
        auto any = autil::legacy::json::ParseJson(jsonStr);
        auto jsonMap = autil::legacy::AnyCast<autil::legacy::json::JsonMap>(any);
        bool isLegacySchema = indexlibv2::config::TabletSchema::IsLegacySchema(jsonMap);
        if (!isLegacySchema) {
            auto schema = indexlibv2::framework::TabletSchemaLoader::LoadSchema(jsonStr);
            if (!schema) {
                AUTIL_LOG(ERROR, "load schema failed");
                return nullptr;
            }
            auto status = indexlibv2::framework::TabletSchemaLoader::ResolveSchema(nullptr, "", schema.get());
            if (!status.IsOK()) {
                AUTIL_LOG(ERROR, "resolve schema failed");
                return nullptr;
            }
            return schema;
        }
        IndexPartitionSchemaPtr legacySchema;
        bool ret = indexlibv2::config::TabletSchema::LoadLegacySchema(jsonStr, legacySchema);
        if (!ret || !legacySchema) {
            AUTIL_LOG(ERROR, "load legacy schema failed");
            return nullptr;
        }
        return std::make_unique<indexlib::config::LegacySchemaAdapter>(legacySchema);
    } catch (autil::legacy::ExceptionBase& e) {
        AUTIL_LOG(ERROR, "schema's json format error, e.what(): %s, schemaStr: %s", e.what(), jsonStr.c_str());
        return nullptr;
    }
    return nullptr;
}

IndexPartitionSchemaPtr SchemaAdapter::TEST_LoadSchema(const std::string& schemaFilePath)
{
    return IndexPartitionSchema::Load(schemaFilePath);
}

config::IndexPartitionSchemaPtr SchemaAdapter::LoadSchemaByVersionId(const std::string& physicalDirPath,
                                                                     versionid_t versionId)
{
    Version version;
    VersionLoader::GetVersionS(physicalDirPath, version, versionId);
    return IndexPartitionSchema::Load(PathUtil::JoinPath(physicalDirPath, version.GetSchemaFileName()), true);
}
}} // namespace indexlib::index_base
