#ifndef ISEARCH_INDEXPARTITIONREADERWRAPPERCREATOR_H
#define ISEARCH_INDEXPARTITIONREADERWRAPPERCREATOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/IndexPartitionReaderWrapper.h>
#include <indexlib/partition/index_partition_reader.h>

BEGIN_HA3_NAMESPACE(search);
struct ReaderWrapperContainer 
{
    //todo support join field info
    std::map<std::string, uint32_t> indexName2IdMap; 
    std::map<std::string, uint32_t> attrName2IdMap; 
    std::vector<IE_NAMESPACE(partition)::IndexPartitionReaderPtr> indexPartReaders;
    IndexPartitionReaderWrapperPtr readerWrapper;
};

class IndexPartitionReaderWrapperCreator
{
public:
    IndexPartitionReaderWrapperCreator();
    ~IndexPartitionReaderWrapperCreator();
private:
    IndexPartitionReaderWrapperCreator(const IndexPartitionReaderWrapperCreator &);
    IndexPartitionReaderWrapperCreator& operator=(const IndexPartitionReaderWrapperCreator &);
public:
    static void CreateIndexPartitionReaderWrapper(
        const std::vector<IE_NAMESPACE(partition)::IndexPartitionPtr>& indexPartitions,
        ReaderWrapperContainer& container);
    
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(IndexPartitionReaderWrapperCreator);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_INDEXPARTITIONREADERWRAPPERCREATOR_H
