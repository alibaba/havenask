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

#include "autil/mem_pool/Pool.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/framework/index_writer.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(config, IndexConfig);
DECLARE_REFERENCE_CLASS(config, FieldConfig);
DECLARE_REFERENCE_CLASS(document, Field);
DECLARE_REFERENCE_CLASS(document, IndexDocument);
DECLARE_REFERENCE_CLASS(document, SectionAttributeAppender);
DECLARE_REFERENCE_CLASS(document, IndexFieldConvertor);
DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(plugin, PluginManager);
DECLARE_REFERENCE_CLASS(util, BuildResourceMetrics);

namespace indexlib { namespace partition {

class IndexDataPatcher
{
public:
    IndexDataPatcher(const config::IndexPartitionSchemaPtr& schema, const file_system::DirectoryPtr& rootDir,
                     size_t initTermCount, const plugin::PluginManagerPtr& pluginManager = plugin::PluginManagerPtr())
        : mSchema(schema)
        , mRootDir(rootDir)
        , mPluginManager(pluginManager)
        , mDocCount(0)
        , mPatchDocCount(0)
        , mInitDistinctTermCount(initTermCount)
    {
        assert(mRootDir);
    }

    ~IndexDataPatcher() {}

public:
    bool Init(const config::IndexConfigPtr& indexConfig, const file_system::DirectoryPtr& segDir, uint32_t docCount);

    // fieldValue input format : Token1Token2Token3...
    void AddField(const std::string& fieldValue, fieldid_t fieldId);

    void EndDocument();

    void Close();

    config::IndexConfigPtr GetIndexConfig() const { return mIndexConfig; }
    document::IndexDocumentPtr GetIndexDocument() const { return mIndexDoc; }
    uint32_t GetTotalDocCount() const { return mDocCount; }
    uint32_t GetPatchDocCount() const { return mPatchDocCount; }

    uint32_t GetDistinctTermCount() const;
    int64_t GetCurrentTotalMemoryUse() const;

private:
    bool DoInit();

private:
    typedef std::shared_ptr<autil::mem_pool::Pool> PoolPtr;
    typedef std::map<fieldid_t, config::FieldConfigPtr> FieldId2FieldConfigMap;

    static const size_t MAX_POOL_MEMORY_THRESHOLD = 10 * 1024 * 1024;

private:
    std::unique_ptr<index::IndexWriter> mIndexWriter;
    config::IndexConfigPtr mIndexConfig;
    PoolPtr mPool;
    document::IndexDocumentPtr mIndexDoc;

    FieldId2FieldConfigMap mFieldConfigMap;
    util::BuildResourceMetricsPtr mBuildResourceMetrics;
    config::IndexPartitionSchemaPtr mSchema;
    file_system::DirectoryPtr mRootDir;
    document::IndexFieldConvertorPtr mConvertor;
    document::SectionAttributeAppenderPtr mSectionAttrAppender;
    plugin::PluginManagerPtr mPluginManager;
    file_system::DirectoryPtr mIndexDir;
    uint32_t mDocCount;
    uint32_t mPatchDocCount;
    size_t mInitDistinctTermCount;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexDataPatcher);
}} // namespace indexlib::partition
