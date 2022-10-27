#ifndef __INDEXLIB_INDEX_CUSTOMIZED_INDEX_WRITER_H
#define __INDEXLIB_INDEX_CUSTOMIZED_INDEX_WRITER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/accessor/index_writer.h"
#include "indexlib/index/normal/inverted_index/customized_index/indexer.h"
#include <autil/ConstString.h>
#include "indexlib/plugin/plugin_manager.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(util, CounterMap);
IE_NAMESPACE_BEGIN(index);

class CustomizedIndexWriter : public IndexWriter
{
public:
    CustomizedIndexWriter(const plugin::PluginManagerPtr& pluginManager,
                          size_t lastSegmentDistinctTermCount)
        : mPluginManager(pluginManager)
        , mLastSegDistinctTermCount(lastSegmentDistinctTermCount)
    {}
    
    ~CustomizedIndexWriter() {}
public:
    void Init(const config::IndexConfigPtr& indexConfig,
              util::BuildResourceMetrics* buildResourceMetrics,
              const index_base::PartitionSegmentIteratorPtr& segIter =
              index_base::PartitionSegmentIteratorPtr()) override;
    
    size_t EstimateInitMemUse(const config::IndexConfigPtr& indexConfig,
                              const index_base::PartitionSegmentIteratorPtr& segIter =
                              index_base::PartitionSegmentIteratorPtr()) override;

    void AddField(const document::Field* field) override;
    void EndDocument(const document::IndexDocument& indexDocument) override;
    void EndSegment() override;
    void Dump(const file_system::DirectoryPtr& indexDir,
              autil::mem_pool::PoolBase* dumpPool) override;
    index::IndexSegmentReaderPtr CreateInMemReader() override;

private:
    uint32_t GetDistinctTermCount() const override;
    void UpdateBuildResourceMetrics() override;

    bool GetIndexerSegmentDatas(
            const config::IndexConfigPtr& indexConfig,            
            const index_base::PartitionSegmentIteratorPtr& segIter,
            std::vector<IndexerSegmentData> &indexerSegDatas);

private:
    plugin::PluginManagerPtr mPluginManager;
    size_t mLastSegDistinctTermCount;
    IndexerPtr mIndexer;
    std::vector<const document::Field*> mFieldVec;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(CustomizedIndexWriter);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_CUSTOMIZED_INDEX_WRITER_H
