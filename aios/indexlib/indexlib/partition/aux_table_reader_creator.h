#ifndef __INDEXLIB_AUX_TABLE_READER_CREATOR_H
#define __INDEXLIB_AUX_TABLE_READER_CREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/partition/index_partition_reader.h"
#include "indexlib/partition/aux_table_reader_typed.h"
#include "indexlib/config/index_partition_schema.h"

IE_NAMESPACE_BEGIN(partition);

class AuxTableReaderCreator
{
public:
    AuxTableReaderCreator() = default;
    ~AuxTableReaderCreator() = default;
public:
    template <typename T>
    static AuxTableReaderTyped<T> *Create(
            const IndexPartitionReaderPtr &indexPartReader,
            const std::string &attrName,
            autil::mem_pool::Pool *pool,
            const std::string &regionName = "")
    {
        const auto &schema = indexPartReader->GetSchema();
        TableType tableType = schema->GetTableType();
        if (tableType == tt_index) {
            const auto &attrReader = indexPartReader->GetAttributeReader(attrName);
            if (!attrReader) {
                return nullptr;
            }
            const index::PrimaryKeyIndexReaderPtr &pkReaderPtr = indexPartReader->GetPrimaryKeyReader();
            if (!pkReaderPtr) {
                return nullptr;
            }
            index::AttributeIteratorBase *iteratorBase = attrReader->CreateIterator(pool);
            typedef index::AttributeIteratorTyped<T> IteratorType;
            IteratorType *iterator = dynamic_cast<IteratorType*>(iteratorBase);
            if (!iterator) {
                IE_POOL_COMPATIBLE_DELETE_CLASS(pool, iteratorBase);
                return nullptr;
            }
            AuxTableReaderImpl<T> impl(pkReaderPtr.get(), iterator);
            return IE_POOL_COMPATIBLE_NEW_CLASS(pool, AuxTableReaderTyped<T>, impl, pool);
        }
        return nullptr;
    }
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AuxTableReaderCreator);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_AUX_TABLE_READER_CREATOR_H
