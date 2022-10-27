#ifndef __INDEXLIB_LAZY_LOAD_ATTRIBUTE_READER_FACTORY_H
#define __INDEXLIB_LAZY_LOAD_ATTRIBUTE_READER_FACTORY_H

#include <map>
#include <tr1/memory>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include "indexlib/misc/singleton.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader_creator.h"
#include "indexlib/index/normal/attribute/attribute_metrics.h"
#include "indexlib/index_base/partition_data.h"

IE_NAMESPACE_BEGIN(index);

class LazyLoadAttributeReaderFactory : public misc::Singleton<LazyLoadAttributeReaderFactory>
{
public:
    typedef std::map<FieldType, AttributeReaderCreatorPtr> CreatorMap;

protected:
    LazyLoadAttributeReaderFactory();
    friend class misc::LazyInstantiation;

public:
    ~LazyLoadAttributeReaderFactory();

public:
    static AttributeReaderPtr CreateAttributeReader(
            const config::AttributeConfigPtr& attrConfig,
            const index_base::PartitionDataPtr& partitionData,
            AttributeMetrics* metrics = NULL);
    
    AttributeReader* CreateAttributeReader(
            const config::FieldConfigPtr& fieldConfig,
            AttributeMetrics* metrics = NULL);
    void RegisterCreator(AttributeReaderCreatorPtr creator);
    void RegisterMultiValueCreator(AttributeReaderCreatorPtr creator);

private:
    void Init();
    
private:
    autil::RecursiveThreadMutex mLock;
    CreatorMap mReaderCreators;
    CreatorMap mMultiValueReaderCreators;
};

typedef std::tr1::shared_ptr<LazyLoadAttributeReaderFactory> LazyLoadAttributeReaderFactoryPtr;

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_LAZY_LOAD_ATTRIBUTE_READER_FACTORY_H
