#ifndef ISEARCH_INDEXPARTITIONWRAPPER_H
#define ISEARCH_INDEXPARTITIONWRAPPER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/ErrorDefine.h>
#include <ha3/config/JoinConfig.h>
#include <ha3/config/ConfigAdapter.h>
#include <indexlib/partition/index_partition.h>
#include <indexlib/partition/partition_reader_snapshot.h>
#include <ha3/search/IndexPartitionReaderWrapper.h>
#include <suez/turing/expression/util/TableInfo.h>
BEGIN_HA3_NAMESPACE(search);

typedef std::map<std::string, IE_NAMESPACE(partition)::IndexPartitionPtr> Cluster2IndexPartitionMap;
HA3_TYPEDEF_PTR(Cluster2IndexPartitionMap);

class IndexPartitionWrapper;
HA3_TYPEDEF_PTR(IndexPartitionWrapper);

class IndexPartitionWrapper
{
private:
public:
    IndexPartitionWrapper();
    ~IndexPartitionWrapper();
private:
    IndexPartitionWrapper(const IndexPartitionWrapper &);
    IndexPartitionWrapper& operator=(const IndexPartitionWrapper &);
public:
    static search::IndexPartitionWrapperPtr createIndexPartitionWrapper(
            const config::ConfigAdapterPtr &configAdapter, 
            const IE_NAMESPACE(partition)::IndexPartitionPtr &indexPart, 
            const std::string &clusterName);
public:
    bool init(const IE_NAMESPACE(partition)::IndexPartitionPtr &indexPart);

    IndexPartitionReaderWrapperPtr createReaderWrapper(
            IE_NAMESPACE(partition)::PartitionReaderSnapshotPtr &indexSnapshot) const;

    IndexPartitionReaderWrapperPtr createPartialReaderWrapper(
            IE_NAMESPACE(partition)::PartitionReaderSnapshotPtr &indexSnapshot) const;

    std::string getIndexRoot() const {
        return _mainIndexPart->GetRootDirectory()->GetPath();
    }

    versionid_t getCurrentVersion() const;

    const IE_NAMESPACE(config)::SummarySchemaPtr &getSummarySchema() const {
        return _mainIndexPart->GetSchema()->GetSummarySchema();
    }
    const IE_NAMESPACE(config)::FieldSchemaPtr &getFieldSchema() const {
        return _mainIndexPart->GetSchema()->GetFieldSchema();
    }
    const std::string& getMainTableName() const {
        return _mainIndexPart->GetSchema()->GetSchemaName();
    }

public:
    // for test
    const IE_NAMESPACE(partition)::IndexPartitionPtr &getMainTablePart() const {
        return _mainIndexPart;
    }
private:
    void addMainIndexPartition(
            const IE_NAMESPACE(partition)::IndexPartitionPtr &indexPart);
    void addIndexPartition(
            const IE_NAMESPACE(config)::IndexPartitionSchemaPtr &schemaPtr,
            uint32_t currentId);
    bool isSubField(const IE_NAMESPACE(config)::IndexPartitionSchemaPtr &schemaPtr,
                    const std::string &joinField) const;
    bool getIndexPartReaders(
            IE_NAMESPACE(partition)::PartitionReaderSnapshotPtr indexSnapshot,
            std::vector<IE_NAMESPACE(partition)::IndexPartitionReaderPtr>
            &indexPartReaderPtrs) const;

private:
    std::map<std::string, uint32_t> _indexName2IdMap;
    std::map<std::string, uint32_t> _attrName2IdMap;
    IE_NAMESPACE(partition)::IndexPartitionPtr _mainIndexPart;
    
private:
    friend class IndexPartitionWrapperTest;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(IndexPartitionWrapper);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_INDEXPARTITIONWRAPPER_H
