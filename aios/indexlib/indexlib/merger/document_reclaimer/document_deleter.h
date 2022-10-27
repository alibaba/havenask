#ifndef __INDEXLIB_DOCUMENT_DELETER_H
#define __INDEXLIB_DOCUMENT_DELETER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index/normal/deletionmap/deletion_map_writer.h"
#include "indexlib/index/normal/attribute/accessor/join_docid_attribute_reader.h"

IE_NAMESPACE_BEGIN(merger);

class DocumentDeleter
{
public:
    DocumentDeleter(const config::IndexPartitionSchemaPtr& schema);
    ~DocumentDeleter();
public:
    void Init(const index_base::PartitionDataPtr& partitionData);
    bool Delete(docid_t docId);
    void Dump(const file_system::DirectoryPtr& directory);
    bool IsDirty() const;
    uint32_t GetMaxTTL() const;
private:
    void GetSubDocIdRange(docid_t mainDocId, docid_t& subDocIdBegin,
                          docid_t& subDocIdEnd) const;
private:
    typedef std::tr1::shared_ptr<autil::mem_pool::Pool> PoolPtr;
    PoolPtr mPool;
    index_base::PartitionDataPtr mPartitionData;
    config::IndexPartitionSchemaPtr mSchema;
    index::DeletionMapWriterPtr mDeletionMapWriter;
    index::DeletionMapWriterPtr mSubDeletionMapWriter;
    index::JoinDocidAttributeReaderPtr mMainJoinAttributeReader;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DocumentDeleter);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_DOCUMENT_DELETER_H
