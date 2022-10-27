#ifndef __INDEXLIB_PARTITION_DEFINE_H
#define __INDEXLIB_PARTITION_DEFINE_H

#include <tr1/unordered_map>

DECLARE_REFERENCE_CLASS(partition, IndexPartitionReader);
DECLARE_REFERENCE_CLASS(index, AttributeReader);
DECLARE_REFERENCE_CLASS(index, PackAttributeReader);

IE_NAMESPACE_BEGIN(partition);

enum IndexPartitionReaderType {
    READER_UNKNOWN_TYPE,
    READER_PRIMARY_MAIN_TYPE,
    READER_PRIMARY_SUB_TYPE,
};

struct IndexPartitionReaderInfo {
    IndexPartitionReaderType readerType;
    IndexPartitionReaderPtr indexPartReader;
    std::string tableName;
    uint64_t partitionIdentifier;
};

struct AttributeReaderInfo {
    index::AttributeReaderPtr attrReader;
    IndexPartitionReaderPtr indexPartReader;
    IndexPartitionReaderType indexPartReaderType;
};

struct PackAttributeReaderInfo {
    index::PackAttributeReaderPtr packAttrReader;
    IndexPartitionReaderPtr indexPartReader;
    IndexPartitionReaderType indexPartReaderType;
};

struct RawPointerAttributeReaderInfo {
    index::AttributeReader* attrReader = nullptr;
    IndexPartitionReader* indexPartReader = nullptr;
    IndexPartitionReaderType indexPartReaderType;
};

typedef std::map<std::pair<std::string, std::string>, uint32_t> TableMem2IdMap;
typedef std::tr1::unordered_map<std::string, uint32_t> Name2IdMap;

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_PARTITION_DEFINE_H

