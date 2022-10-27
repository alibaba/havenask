#ifndef __INDEXLIB_IN_MEMORY_INDEX_SEGMENT_WRITER_H
#define __INDEXLIB_IN_MEMORY_INDEX_SEGMENT_WRITER_H

#include <tr1/memory>
#include <queue>
#include <list>
#include <autil/mem_pool/Pool.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(config, IndexPartitionOptions);
DECLARE_REFERENCE_CLASS(config, IndexConfig);
DECLARE_REFERENCE_CLASS(index_base, SegmentMetrics);
DECLARE_REFERENCE_CLASS(index_base, PartitionSegmentIterator);
DECLARE_REFERENCE_CLASS(common, DumpItem);
DECLARE_REFERENCE_CLASS(util, BuildResourceMetrics);
DECLARE_REFERENCE_CLASS(util, CounterMap);
DECLARE_REFERENCE_CLASS(plugin, PluginManager);
DECLARE_REFERENCE_CLASS(document, NormalDocument);
DECLARE_REFERENCE_CLASS(document, IndexDocument);
DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(index, PrimaryKeyIndexWriter);
DECLARE_REFERENCE_CLASS(index, IndexWriter);
DECLARE_REFERENCE_CLASS(index, MultiFieldIndexSegmentReader);
DECLARE_REFERENCE_CLASS(index, IndexSegmentReader);

IE_NAMESPACE_BEGIN(index);

class InMemoryIndexSegmentWriter
{
public:
    InMemoryIndexSegmentWriter();
    ~InMemoryIndexSegmentWriter();

public:
    typedef std::vector<index::IndexWriterPtr> IndexWriterVec;
    typedef std::list<index::IndexWriterPtr> IndexWriterList;
    typedef std::tr1::shared_ptr<IndexWriterList> IndexWriterListPtr;
    typedef std::vector<IndexWriterListPtr> IndexWriterListVec;

public:
    void Init(const config::IndexPartitionSchemaPtr& schema,
              const config::IndexPartitionOptions& options,
              const index_base::SegmentMetricsPtr& lastSegmentMetrics,
              util::BuildResourceMetrics* buildResourceMetrics,
              const plugin::PluginManagerPtr& pluginManager,
              const index_base::PartitionSegmentIteratorPtr& segIter);

    static size_t EstimateInitMemUse(
            const index_base::SegmentMetricsPtr& metrics,
            const config::IndexPartitionSchemaPtr& schema,
            const config::IndexPartitionOptions& options,
            const plugin::PluginManagerPtr& pluginManager,
            const index_base::PartitionSegmentIteratorPtr& segIter);
    
    void CollectSegmentMetrics();
    bool AddDocument(const document::NormalDocumentPtr& doc);
    void EndSegment();
    void CreateDumpItems(const file_system::DirectoryPtr& directory, 
                         std::vector<common::DumpItem*>& dumpItems);
    bool IsPrimaryKeyStrValid(const std::string& str) const;

    MultiFieldIndexSegmentReaderPtr CreateInMemSegmentReader() const;
    IndexSegmentReaderPtr GetInMemPrimaryKeyReader() const;

    void GetDumpEstimateFactor(std::priority_queue<double>& factors,
                               std::priority_queue<size_t>& minMemUses) const;
    // TODO: remove
    double GetIndexPoolToDumpFileRatio();
    // TODO: remove
    const index_base::SegmentMetricsPtr& GetSegmentMetrics() const
    { return mSegmentMetrics; }

private:
    static bool IsTruncateIndex(const config::IndexConfigPtr& indexConfig);

    index::IndexWriterPtr CreateIndexWriter(
            const config::IndexConfigPtr& indexConfig,
            const config::IndexPartitionOptions& options,            
            util::BuildResourceMetrics* buildResourceMetrics,
            const plugin::PluginManagerPtr& pluginManager,
            const index_base::PartitionSegmentIteratorPtr& segIter =
            index_base::PartitionSegmentIteratorPtr()) const;

    void EndDocument(const document::IndexDocumentPtr& indexDocument);
    static constexpr double DEFAULT_INDEX_BYTE_SLICE_POOL_DUMP_FILE_RATIO = 0.2;
    static constexpr double TRIE_INDEX_BYTE_SLICE_POOL_DUMP_FILE_RATIO = 3.5;

private:
    IndexWriterVec mIndexWriters;
    IndexWriterListVec mFieldToIndexWriters;
    index_base::SegmentMetricsPtr mSegmentMetrics;
    index::PrimaryKeyIndexWriterPtr mPrimaryKeyIndexWriter;

private:
    IE_LOG_DECLARE();
    friend class InMemoryIndexSegmentWriterTest;
};

DEFINE_SHARED_PTR(InMemoryIndexSegmentWriter);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_IN_MEMORY_INDEX_SEGMENT_WRITER_H
