#ifndef ISEARCH_ATTRIBUTEMETAINFO_H
#define ISEARCH_ATTRIBUTEMETAINFO_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <indexlib/partition/index_partition_reader.h>
#include <indexlib/partition/join_cache/join_info.h>

BEGIN_HA3_NAMESPACE(search);

enum AttributeType {
    AT_MAIN_ATTRIBUTE,
    AT_SUB_ATTRIBUTE,
    AT_UNKNOWN,
};

class AttributeMetaInfo
{
public:
    typedef IE_NAMESPACE(partition)::IndexPartitionReaderPtr IndexPartitionReaderPtr;
    typedef IE_NAMESPACE(index)::AttributeReaderPtr AttributeReaderPtr;
public:
    AttributeMetaInfo(
            const std::string &attrName = "", 
            AttributeType attrType = AT_MAIN_ATTRIBUTE,
            const IndexPartitionReaderPtr &indexPartReaderPtr = IndexPartitionReaderPtr());
    ~AttributeMetaInfo();
public:
    const std::string& getAttributeName() const {
        return _attrName;
    }
    void setAttributeName(const std::string &attributeName) {
        _attrName = attributeName;
    }
    AttributeType getAttributeType() const {
        return _attrType;
    }
    void setAttributeType(AttributeType attrType) {
        _attrType = attrType;
    }
    const IndexPartitionReaderPtr& getIndexPartReader() const {
        return _indexPartReaderPtr;
    }
    void setIndexPartReader(const IndexPartitionReaderPtr& idxPartReaderPtr) {
        _indexPartReaderPtr = idxPartReaderPtr;
    }
    
private:
    std::string _attrName;
    AttributeType _attrType;
    IndexPartitionReaderPtr _indexPartReaderPtr;    
private:
    HA3_LOG_DECLARE();
};
HA3_TYPEDEF_PTR(AttributeMetaInfo);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_ATTRIBUTEMETAINFO_H
