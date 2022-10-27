#ifndef __INDEXLIB_ATTRIBUTE_WRITER_FACTORY_H
#define __INDEXLIB_ATTRIBUTE_WRITER_FACTORY_H

#include <map>
#include <tr1/memory>
#include <autil/mem_pool/Pool.h>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/singleton.h"
#include "indexlib/index/normal/attribute/accessor/attribute_writer.h"
#include "indexlib/index/normal/attribute/accessor/pack_attribute_writer.h"
#include "indexlib/index/normal/attribute/accessor/attribute_writer_creator.h"
#include "indexlib/config/index_partition_options.h"

IE_NAMESPACE_BEGIN(index);

class AttributeWriterFactory : public misc::Singleton<AttributeWriterFactory>
{
public:
    typedef std::map<FieldType, AttributeWriterCreatorPtr> CreatorMap;

protected:
    AttributeWriterFactory();
    friend class misc::LazyInstantiation;

public:
    ~AttributeWriterFactory();

public:
    AttributeWriter* CreateAttributeWriter(
            const config::AttributeConfigPtr& attrConfig,
            const config::IndexPartitionOptions& options,
            util::BuildResourceMetrics* buildResourceMetrics);

    PackAttributeWriter* CreatePackAttributeWriter(
            const config::PackAttributeConfigPtr& attrConfig,
            const config::IndexPartitionOptions& options,
            util::BuildResourceMetrics* buildResourceMetrics); 

    void RegisterCreator(AttributeWriterCreatorPtr creator);
    void RegisterMultiValueCreator(AttributeWriterCreatorPtr creator);

protected:
#ifdef ATTRIBUTE_WRITER_FACTORY_UNITTEST
    friend class AttributeWriterFactoryTest;
    CreatorMap& GetMap() { return mWriterCreators;}
#endif

private:
    void Init();

private:
    autil::RecursiveThreadMutex mLock;
    CreatorMap mWriterCreators;
    CreatorMap mMultiValueWriterCreators;
};

typedef std::tr1::shared_ptr<AttributeWriterFactory> AttributeWriterFactoryPtr;

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_ATTRIBUTE_WRITER_FACTORY_H
