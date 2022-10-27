#ifndef __INDEXLIB_INDEX_DATA_PATCHER_H
#define __INDEXLIB_INDEX_DATA_PATCHER_H

#include <tr1/memory>
#include <autil/mem_pool/Pool.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(config, IndexConfig);
DECLARE_REFERENCE_CLASS(config, FieldConfig);
DECLARE_REFERENCE_CLASS(document, Field);
DECLARE_REFERENCE_CLASS(document, IndexDocument);
DECLARE_REFERENCE_CLASS(document, SectionAttributeAppender);
DECLARE_REFERENCE_CLASS(index, IndexWriter);
DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(plugin, PluginManager);
DECLARE_REFERENCE_CLASS(util, BuildResourceMetrics);
DECLARE_REFERENCE_CLASS(partition, IndexFieldConvertor);

IE_NAMESPACE_BEGIN(partition);

class IndexDataPatcher
{
public:
    IndexDataPatcher(const config::IndexPartitionSchemaPtr& schema,
                     const file_system::DirectoryPtr& rootDir,
                     size_t initTermCount,
                     const plugin::PluginManagerPtr& pluginManager =
                     plugin::PluginManagerPtr())
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
    bool Init(const config::IndexConfigPtr& indexConfig,
              const std::string& segDirPath, uint32_t docCount);

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
    typedef std::tr1::shared_ptr<autil::mem_pool::Pool> PoolPtr;
    typedef std::map<fieldid_t, config::FieldConfigPtr> FieldId2FieldConfigMap;

    static const size_t MAX_POOL_MEMORY_THRESHOLD = 10 * 1024 * 1024;
    
private:
    index::IndexWriterPtr mIndexWriter;
    config::IndexConfigPtr mIndexConfig;
    PoolPtr mPool;
    document::IndexDocumentPtr mIndexDoc;

    FieldId2FieldConfigMap mFieldConfigMap;
    util::BuildResourceMetricsPtr mBuildResourceMetrics;
    config::IndexPartitionSchemaPtr mSchema;
    file_system::DirectoryPtr mRootDir;
    IndexFieldConvertorPtr mConvertor;
    document::SectionAttributeAppenderPtr mSectionAttrAppender;
    plugin::PluginManagerPtr mPluginManager;
    std::string mIndexDirPath;
    uint32_t mDocCount;
    uint32_t mPatchDocCount;
    size_t mInitDistinctTermCount;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexDataPatcher);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_INDEX_DATA_PATCHER_H
