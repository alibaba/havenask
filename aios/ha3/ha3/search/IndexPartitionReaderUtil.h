#ifndef ISEARCH_INDEXPARTITIONREADERUTIL_H
#define ISEARCH_INDEXPARTITIONREADERUTIL_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <indexlib/partition/index_partition_reader.h>
#include <ha3/search/IndexPartitionReaderWrapper.h>

BEGIN_HA3_NAMESPACE(search);

class IndexPartitionReaderUtil
{
public:
    IndexPartitionReaderUtil();
    ~IndexPartitionReaderUtil();
private:
    IndexPartitionReaderUtil(const IndexPartitionReaderUtil &);
    IndexPartitionReaderUtil& operator=(const IndexPartitionReaderUtil &);
public:
    static search::IndexPartitionReaderWrapperPtr createIndexPartitionReaderWrapper(
            IE_NAMESPACE(partition)::PartitionReaderSnapshot *partitionReaderSnapshot,
            const std::string& mainTableName);
private:
    static void addIndexPartition(const IE_NAMESPACE(config)::IndexPartitionSchemaPtr &schemaPtr,
                                  uint32_t id,
                                  std::map<std::string, uint32_t> &indexName2IdMap,
                                  std::map<std::string, uint32_t> &attrName2IdMap);


private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(IndexPartitionReaderUtil);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_INDEXPARTITIONREADERUTIL_H
