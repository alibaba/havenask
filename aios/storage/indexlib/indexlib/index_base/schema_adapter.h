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
#pragma once

#include <memory>

#include "fslib/fslib.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/fslib/FenceContext.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(config, IndexPartitionOptions);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);

namespace indexlibv2::config {
class ITabletSchema;
}

namespace indexlib { namespace index_base {

// TODO: add UT
class SchemaAdapter
{
public:
    SchemaAdapter();
    ~SchemaAdapter();

public:
    static config::IndexPartitionSchemaPtr LoadSchema(const file_system::DirectoryPtr& rootDirectory);
    static config::IndexPartitionSchemaPtr LoadSchema(const file_system::DirectoryPtr& rootDir, schemaid_t schemaId);
    static config::IndexPartitionSchemaPtr LoadSchema(const file_system::DirectoryPtr& rootDir,
                                                      const std::string& schemaFileName);

    static std::unique_ptr<indexlibv2::config::ITabletSchema> LoadSchema(const std::string& jsonStr);
    // load schema according to version
    static void LoadSchema(const std::string& schemaJson, config::IndexPartitionSchemaPtr& schema);
    static config::IndexPartitionSchemaPtr LoadSchemaForVersion(const file_system::DirectoryPtr& dir,
                                                                versionid_t versionId = INVALID_VERSIONID);

    static config::IndexPartitionSchemaPtr LoadAndRewritePartitionSchema(const file_system::DirectoryPtr& rootDir,
                                                                         const config::IndexPartitionOptions& options,
                                                                         schemaid_t schemaId = DEFAULT_SCHEMAID);
    static config::IndexPartitionSchemaPtr RewritePartitionSchema(const config::IndexPartitionSchemaPtr& schema,
                                                                  const file_system::DirectoryPtr& rootDir,
                                                                  const config::IndexPartitionOptions& options);
    static void ListSchemaFile(const file_system::DirectoryPtr& rootDir, fslib::FileList& fileList);

public:
    // stirng interface for suez & build_service
    static config::IndexPartitionSchemaPtr LoadSchemaByVersionId(const std::string& physicalDirPath,
                                                                 versionid_t versionId = INVALID_VERSIONID);

public:
    static void StoreSchema(const file_system::DirectoryPtr& directory, const config::IndexPartitionSchemaPtr& schema);
    static void StoreSchema(const std::string& dir, const config::IndexPartitionSchemaPtr& schema,
                            file_system::FenceContext* fenceContext = file_system::FenceContext::NoFence());
    static void RewriteToRtSchema(const config::IndexPartitionSchemaPtr& schema);

    static void LoadAdaptiveBitmapTerms(const file_system::DirectoryPtr& rootDir,
                                        const config::IndexPartitionSchemaPtr& schema);
    static void LoadAdaptiveBitmapTerms(const file_system::DirectoryPtr& rootDir, config::IndexPartitionSchema* schema);

public:
    static config::IndexPartitionSchemaPtr TEST_LoadSchema(const std::string& schemaFilePath);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SchemaAdapter);
}} // namespace indexlib::index_base
