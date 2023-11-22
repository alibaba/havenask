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

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(index_base, Version);
DECLARE_REFERENCE_CLASS(index_base, PartitionPatchMeta);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);

namespace indexlib { namespace index_base {

class PartitionPatchMetaCreator
{
public:
    PartitionPatchMetaCreator();
    ~PartitionPatchMetaCreator();

public:
    static PartitionPatchMetaPtr Create(const std::string& rootPath, const config::IndexPartitionSchemaPtr& schema,
                                        const index_base::Version& version);

    static PartitionPatchMetaPtr Create(const file_system::DirectoryPtr& rootDir,
                                        const config::IndexPartitionSchemaPtr& schema,
                                        const index_base::Version& version);

private:
    static PartitionPatchMetaPtr GetLastPartitionPatchMeta(const file_system::DirectoryPtr& rootDirectory,
                                                           schemaid_t& lastSchemaId);

    static void GetPatchIndexInfos(const file_system::DirectoryPtr& segDir,
                                   const config::IndexPartitionSchemaPtr& schema, std::vector<std::string>& indexNames,
                                   std::vector<std::string>& attrNames);

    static PartitionPatchMetaPtr CreatePatchMeta(const file_system::DirectoryPtr& rootDirectory,
                                                 const config::IndexPartitionSchemaPtr& schema,
                                                 const index_base::Version& version,
                                                 const PartitionPatchMetaPtr& lastPatchMeta, schemaid_t lastSchemaId);

    static PartitionPatchMetaPtr CreatePatchMeta(const file_system::DirectoryPtr& rootDirectory,
                                                 const config::IndexPartitionSchemaPtr& schema,
                                                 const index_base::Version& version,
                                                 const std::vector<schemaid_t>& schemaIdVec,
                                                 const PartitionPatchMetaPtr& lastPatchMeta);

    static void GetValidIndexAndAttribute(const config::IndexPartitionSchemaPtr& schema,
                                          const std::vector<std::string>& inputIndexNames,
                                          const std::vector<std::string>& inputAttrNames,
                                          std::vector<std::string>& indexNames, std::vector<std::string>& attrNames);

    static PartitionPatchMetaPtr CreatePatchMetaForModifySchema(const file_system::DirectoryPtr& rootDirectory,
                                                                const config::IndexPartitionSchemaPtr& schema,
                                                                const index_base::Version& version);

    static bool GetValidIndexAndAttributeForModifyOperation(const config::IndexPartitionSchemaPtr& schema,
                                                            schema_opid_t opId, std::vector<std::string>& indexNames,
                                                            std::vector<std::string>& attrNames);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PartitionPatchMetaCreator);
}} // namespace indexlib::index_base
