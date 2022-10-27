#ifndef __INDEXLIB_ATTRIBUTE_MERGER_FACTORY_H
#define __INDEXLIB_ATTRIBUTE_MERGER_FACTORY_H

#include <map>
#include <tr1/memory>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/singleton.h"

DECLARE_REFERENCE_CLASS(index, PackAttributeMerger);
DECLARE_REFERENCE_CLASS(index, AttributeMerger);
DECLARE_REFERENCE_CLASS(index, AttributeMergerCreator);

IE_NAMESPACE_BEGIN(index);

class AttributeMergerFactory : public misc::Singleton<AttributeMergerFactory>
{
public:
    typedef std::map<FieldType, AttributeMergerCreatorPtr> CreatorMap;

protected:
    AttributeMergerFactory();
    friend class misc::LazyInstantiation;

public:
    ~AttributeMergerFactory();

public:
    AttributeMerger* CreateAttributeMerger(
        const config::AttributeConfigPtr& attrConfig,
        bool needMergePatch,
        const index_base::MergeItemHint& hint = index_base::MergeItemHint(),
        const index_base::MergeTaskResourceVector& taskResources =
        index_base::MergeTaskResourceVector());

    
    AttributeMerger* CreatePackAttributeMerger(
        const config::PackAttributeConfigPtr& packAttrConfig,
        bool needMergePatch, size_t patchBufferSize,
        const index_base::MergeItemHint& hint = index_base::MergeItemHint(),
        const index_base::MergeTaskResourceVector& taskResources =
        index_base::MergeTaskResourceVector());

    
    void RegisterCreator(const AttributeMergerCreatorPtr &creator);
    void RegisterMultiValueCreator(const AttributeMergerCreatorPtr &creator);

protected:
#ifdef ATTRIBUTE_MERGER_FACTORY_UNITTEST
    friend class AttributeMergerFactoryTest;
    CreatorMap& GetMap() { return mMergerCreators;}
#endif

private:
    void Init();
    bool IsDocIdJoinAttribute(const config::AttributeConfigPtr& attrConfig);

private:
    autil::RecursiveThreadMutex mLock;
    CreatorMap mMergerCreators;
    CreatorMap mMultiValueMergerCreators;
};

DEFINE_SHARED_PTR(AttributeMergerFactory);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_ATTRIBUTE_MERGER_FACTORY_H
