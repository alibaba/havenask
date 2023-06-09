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
#ifndef __INDEXLIB_SCHEMA_REWRITER_H
#define __INDEXLIB_SCHEMA_REWRITER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/primary_key_load_strategy_param.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionOptions);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(index_base, IndexFormatVersion);
DECLARE_REFERENCE_CLASS(file_system, Directory);

namespace indexlib { namespace index_base {

class SchemaRewriter
{
public:
    SchemaRewriter();
    ~SchemaRewriter();

public:
    static void Rewrite(const config::IndexPartitionSchemaPtr& schema, const config::IndexPartitionOptions& options,
                        const file_system::DirectoryPtr& rootDir);

    static void RewriteForRealtimeTimestamp(const config::IndexPartitionSchemaPtr& schema);

    static void RewriteForRemoveTFBitmapFlag(const config::IndexPartitionSchemaPtr& schema);

    static void RewriteFieldType(const config::IndexPartitionSchemaPtr& schema,
                                 const config::IndexPartitionOptions& options);

    static void DisablePKLoadParam(const config::IndexPartitionSchemaPtr& schema);

public:
    // public for test
    static void RewriteForSubTable(const config::IndexPartitionSchemaPtr& schema);
    static void SetPrimaryKeyLoadParam(const config::IndexPartitionSchemaPtr& schema,
                                       config::PrimaryKeyLoadStrategyParam::PrimaryKeyLoadMode loadMode,
                                       const std::string& loadParam, bool needLookupReverse);

    static void RewriteForKV(const config::IndexPartitionSchemaPtr& schema,
                             const config::IndexPartitionOptions& options,
                             const IndexFormatVersion& indexFormatVersion);

private:
    static void RewriteForEnableBloomFilter(const config::IndexPartitionSchemaPtr& schema,
                                            const config::IndexPartitionOptions& options);

    static void RewriteForDisableFields(const config::IndexPartitionSchemaPtr& schema,
                                        const config::IndexPartitionOptions& options);

    static void RewriteForDisableSource(const config::IndexPartitionSchemaPtr& schema,
                                        const config::IndexPartitionOptions& options);

    static void RewriteForDisableSummary(const config::IndexPartitionSchemaPtr& schema,
                                         const config::IndexPartitionOptions& options);

    static void RewriteForDisableIndexs(const config::IndexPartitionSchemaPtr& schema,
                                        const config::IndexPartitionOptions& options);

    static void RewriteForTruncateIndexConfigs(const config::IndexPartitionSchemaPtr& schema,
                                               const config::IndexPartitionOptions& options,
                                               const file_system::DirectoryPtr& rootDir);

    static void RewriteForPrimaryKey(const config::IndexPartitionSchemaPtr& schema,
                                     const config::IndexPartitionOptions& options);

    static void AddOfflineJoinVirtualAttribute(const std::string& attrName,
                                               const config::IndexPartitionSchemaPtr& destSchema);

    static void DoRewriteForRemoveTFBitmapFlag(const config::IndexPartitionSchemaPtr& schema);

    static void RewriteOneRegionForKV(const config::IndexPartitionSchemaPtr& schema,
                                      const config::IndexPartitionOptions& options,
                                      const IndexFormatVersion& indexFormatVersion, regionid_t id);

    static void UnifyKKVBuildParam(const config::IndexPartitionSchemaPtr& schema,
                                   const IndexFormatVersion& indexFormatVersion);

    static void UnifyKKVIndexPreference(const config::IndexPartitionSchemaPtr& schema);

    static void UnifyKVIndexConfig(const config::IndexPartitionSchemaPtr& schema,
                                   const IndexFormatVersion& indexFormatVersion);

    static bool GetRewriteBuildProtectionThreshold(const config::IndexPartitionSchemaPtr& schema,
                                                   const config::IndexPartitionOptions& options,
                                                   uint32_t& buildProtectThreshold);
    static void RewriteForUpdateSchemaStandards(const config::IndexPartitionSchemaPtr& schema,
                                                const config::IndexPartitionOptions& options);

private:
    friend class SchemaRewriterTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SchemaRewriter);
}} // namespace indexlib::index_base

#endif //__INDEXLIB_SCHEMA_REWRITER_H
