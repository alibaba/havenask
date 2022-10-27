#ifndef __INDEXLIB_MULTI_FIELD_ATTRIBUTE_READER_H
#define __INDEXLIB_MULTI_FIELD_ATTRIBUTE_READER_H

#include <tr1/memory>
#include <tr1/unordered_map>
#include <autil/AtomicCounter.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

DECLARE_REFERENCE_CLASS(config, AttributeSchema);
DECLARE_REFERENCE_CLASS(config, AttributeConfig);
DECLARE_REFERENCE_CLASS(index, AttributeReader);
DECLARE_REFERENCE_CLASS(index, AttributeMetrics);
DECLARE_REFERENCE_CLASS(index_base, PartitionData);

IE_NAMESPACE_BEGIN(index);

class MultiFieldAttributeReader
{
public:
    MultiFieldAttributeReader(const config::AttributeSchemaPtr& attributeSchema,
                              AttributeMetrics* attributeMetrics = NULL,
                              bool lazyLoad = false, int32_t initReaderThreadCount = 0);
    virtual ~MultiFieldAttributeReader();

public:
    void Open(const index_base::PartitionDataPtr& partitionData);

    const AttributeReaderPtr& GetAttributeReader(const std::string& field) const;
    void EnableAccessCountors() { mEnableAccessCountors = true; }

// for test
public:
    typedef std::tr1::unordered_map<std::string, AttributeReaderPtr> AttributeReaderMap;

    void AddAttributeReader(const std::string& attrName, 
                            const AttributeReaderPtr& attrReader);

    const AttributeReaderMap& GetAttributeReaders() const
    { return mAttrReaderMap; }

    void InitAttributeReader(const config::AttributeConfigPtr& attrConfig,
                             const index_base::PartitionDataPtr& partitionData);

private:
    void InitAttributeReaders(const index_base::PartitionDataPtr& partitionData);
    void MultiThreadInitAttributeReaders(const index_base::PartitionDataPtr& partitionData,
            int32_t threadNum);
    AttributeReaderPtr CreateAttributeReader(const config::AttributeConfigPtr& attrConfig,
            const index_base::PartitionDataPtr& partitionData);

private:
    config::AttributeSchemaPtr mAttrSchema;
    AttributeReaderMap mAttrReaderMap;
    mutable AttributeMetrics* mAttributeMetrics;
    mutable bool mEnableAccessCountors;
    bool mLazyLoad;
    int32_t mInitReaderThreadCount;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MultiFieldAttributeReader);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_MULTI_FIELD_ATTRIBUTE_READER_H
