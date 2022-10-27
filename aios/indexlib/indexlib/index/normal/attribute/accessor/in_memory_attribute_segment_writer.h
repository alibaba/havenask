#ifndef __INDEXLIB_IN_MEMORY_ATTRIBUTE_SEGMENT_WRITER_H
#define __INDEXLIB_IN_MEMORY_ATTRIBUTE_SEGMENT_WRITER_H

#include <tr1/memory>
#include <queue>
#include <autil/mem_pool/Pool.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/index/normal/attribute/accessor/pack_attribute_writer.h"
#include "indexlib/index/in_memory_segment_reader.h"

DECLARE_REFERENCE_CLASS(common, DumpItem);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(config, IndexPartitionOptions);
DECLARE_REFERENCE_CLASS(config, AttributeSchema);
DECLARE_REFERENCE_CLASS(index, AttributeWriter);
DECLARE_REFERENCE_CLASS(index, UpdateFieldExtractor);
DECLARE_REFERENCE_CLASS(util, CounterMap);
DECLARE_REFERENCE_CLASS(util, AccumulativeCounter);
DECLARE_REFERENCE_CLASS(document, NormalDocument);
DECLARE_REFERENCE_CLASS(file_system, Directory);

IE_NAMESPACE_BEGIN(index);

class InMemoryAttributeSegmentWriter
{
public:
    typedef std::vector<AttributeWriterPtr> AttrWriterVector;
    typedef std::vector<PackAttributeWriterPtr> PackAttrWriterVector;
    typedef std::vector<util::AccumulativeCounterPtr> AttrCounterVector;
public:
    InMemoryAttributeSegmentWriter(bool isVirtual = false);
    virtual ~InMemoryAttributeSegmentWriter();

public:
    void Init(const config::IndexPartitionSchemaPtr& schema,
              const config::IndexPartitionOptions& options,
              util::BuildResourceMetrics* buildResourceMetrics = NULL,
              const util::CounterMapPtr& counterMap = util::CounterMapPtr());

    bool AddDocument(const document::NormalDocumentPtr& doc);

    virtual bool UpdateDocument(docid_t docId, const document::NormalDocumentPtr& doc);

    void CreateDumpItems(const file_system::DirectoryPtr& directory, 
                         std::vector<common::DumpItem*>& dumpItems);

    bool UpdateEncodedFieldValue(docid_t docId, fieldid_t fieldId,
                                 const autil::ConstString& value);

    void CreateInMemSegmentReaders(
            InMemorySegmentReader::String2AttrReaderMap& attrReaders);

    // for sub table offline join
    AttributeWriterPtr GetAttributeWriter(const std::string& attrName);

    void GetDumpEstimateFactor(std::priority_queue<double>& factors,
                               std::priority_queue<size_t>& minMemUses);

protected:
    void InitAttributeWriters(const config::AttributeSchemaPtr& attrSchema,
                              const config::IndexPartitionOptions& options,
                              util::BuildResourceMetrics* buildResourceMetrics,
                              const util::CounterMapPtr& counterMapContent);

    virtual const config::AttributeSchemaPtr& GetAttributeSchema() const;

private:
    // virtual for test
    virtual AttributeWriterPtr CreateAttributeWriter(
            const config::AttributeConfigPtr& attrConfig,
            const config::IndexPartitionOptions& options,
            util::BuildResourceMetrics* buildResourceMetrics) const;

    void UpdateAttributeFields(docid_t docId, 
                               const UpdateFieldExtractor& extractor);
    
    void UpdatePackAttributeFields(docid_t docId);
    
protected:
    typedef std::vector<PackAttributeWriter::PackAttributeFields> PackAttrFieldsVector;
    
    config::IndexPartitionSchemaPtr mSchema;
    AttrWriterVector mAttrIdToAttributeWriters;
    PackAttrWriterVector mPackIdToPackAttrWriters;
    PackAttrFieldsVector mPackIdToPackFields;
    AttrCounterVector mAttrIdToCounters;
    bool mIsVirtual;
    bool mEnableCounters;
private:
    friend class InMemoryAttributeSegmentWriterTest;
    friend class MockInMemorySegmentWriter;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(InMemoryAttributeSegmentWriter);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_IN_MEMORY_ATTRIBUTE_SEGMENT_WRITER_H
