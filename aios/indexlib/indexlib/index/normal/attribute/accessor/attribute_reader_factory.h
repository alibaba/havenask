#ifndef __INDEXLIB_ATTRIBUTE_READER_FACTORY_H
#define __INDEXLIB_ATTRIBUTE_READER_FACTORY_H

#include <map>
#include <tr1/memory>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/singleton.h"

DECLARE_REFERENCE_CLASS(index, AttributeReader);
DECLARE_REFERENCE_CLASS(index, AttributeReaderCreator);
DECLARE_REFERENCE_CLASS(index, AttributeMetrics);
DECLARE_REFERENCE_CLASS(index, JoinDocidAttributeReader);
DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(config, FieldConfig);
DECLARE_REFERENCE_CLASS(config, AttributeConfig);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);

IE_NAMESPACE_BEGIN(index);

class AttributeReaderFactory : public misc::Singleton<AttributeReaderFactory>
{
public:
    typedef std::map<FieldType, AttributeReaderCreatorPtr> CreatorMap;

protected:
    AttributeReaderFactory();
    friend class misc::LazyInstantiation;

public:
    ~AttributeReaderFactory();

public:
    AttributeReader* CreateAttributeReader(
            const config::FieldConfigPtr& fieldConfig,
            AttributeMetrics* metrics = NULL);
    void RegisterCreator(AttributeReaderCreatorPtr creator);
    void RegisterMultiValueCreator(AttributeReaderCreatorPtr creator);

    static JoinDocidAttributeReaderPtr CreateJoinAttributeReader(
            const config::IndexPartitionSchemaPtr& schema,
            const std::string& attrName,
            const index_base::PartitionDataPtr& partitionData);

    static AttributeReaderPtr CreateAttributeReader(
            const config::AttributeConfigPtr& attrConfig,
            const index_base::PartitionDataPtr& partitionData,
            AttributeMetrics* metrics = NULL);

protected:
#ifdef ATTRIBUTE_READER_FACTORY_UNITTEST
    friend class AttributeReaderFactoryTest;
    CreatorMap& GetMap() { return mReaderCreators;}
#endif

private:
    void Init();
    AttributeReader* CreateJoinDocidAttributeReader();
    static JoinDocidAttributeReaderPtr CreateJoinAttributeReader(
            const config::IndexPartitionSchemaPtr& schema,
            const std::string& attrName,
            const index_base::PartitionDataPtr& partitionData,
            const index_base::PartitionDataPtr& joinPartitionData);
private:
    autil::RecursiveThreadMutex mLock;
    CreatorMap mReaderCreators;
    CreatorMap mMultiValueReaderCreators;
};

typedef std::tr1::shared_ptr<AttributeReaderFactory> AttributeReaderFactoryPtr;

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_ATTRIBUTE_READER_FACTORY_H
