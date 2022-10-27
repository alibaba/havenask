#ifndef __INDEXLIB_ATTRIBUTE_UPDATER_FACTORY_H
#define __INDEXLIB_ATTRIBUTE_UPDATER_FACTORY_H

#include <map>
#include <tr1/memory>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include "indexlib/misc/singleton.h"
#include "indexlib/index/normal/attribute/accessor/attribute_updater.h"
#include "indexlib/index/normal/attribute/accessor/attribute_updater_creator.h"

IE_NAMESPACE_BEGIN(index);

class AttributeUpdaterFactory : public misc::Singleton<AttributeUpdaterFactory>
{
public:
    typedef std::map<FieldType, AttributeUpdaterCreatorPtr> CreatorMap;

protected:
    AttributeUpdaterFactory();
    friend class misc::LazyInstantiation;

public:
    ~AttributeUpdaterFactory();

public:
    AttributeUpdater* CreateAttributeUpdater(
            util::BuildResourceMetrics* buildResourceMetrics,
            segmentid_t segId, const config::AttributeConfigPtr& attrConfig);

    void RegisterCreator(AttributeUpdaterCreatorPtr creator);
    void RegisterMultiValueCreator(AttributeUpdaterCreatorPtr creator);

    bool IsAttributeUpdatable(const config::AttributeConfigPtr& attrConfig);

protected:
#ifdef ATTRIBUTE_UPDATER_FACTORY_UNITTEST
    friend class AttributeUpdaterFactoryTest;
    CreatorMap& GetMap() { return mUpdaterCreators;}
#endif

private:
    void Init();
    AttributeUpdater* CreateAttributeUpdater(
            util::BuildResourceMetrics* buildResourceMetrics,
            segmentid_t segId, 
            const config::AttributeConfigPtr& attrConfig,
            const CreatorMap& creators);

private:
    autil::RecursiveThreadMutex mLock;
    CreatorMap mUpdaterCreators;
    CreatorMap mMultiValueUpdaterCreators;

    IE_LOG_DECLARE();
};

typedef std::tr1::shared_ptr<AttributeUpdaterFactory> AttributeUpdaterFactoryPtr;

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_ATTRIBUTE_UPDATER_FACTORY_H
