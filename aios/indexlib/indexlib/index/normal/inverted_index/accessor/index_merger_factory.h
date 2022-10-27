#ifndef __INDEXLIB_INDEX_MERGER_FACTORY_H
#define __INDEXLIB_INDEX_MERGER_FACTORY_H

#include <tr1/memory>
#include <autil/Lock.h>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/singleton.h"

DECLARE_REFERENCE_CLASS(plugin, PluginManager);
DECLARE_REFERENCE_CLASS(util, CounterMap);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(index, IndexMergerCreator);
DECLARE_REFERENCE_CLASS(index, IndexMerger);

IE_NAMESPACE_BEGIN(index);

class IndexMergerFactory : public misc::Singleton<IndexMergerFactory>
{
public:
    typedef std::map<IndexType, IndexMergerCreatorPtr> IndexCreatorMap;

protected:
    IndexMergerFactory();
    friend class misc::LazyInstantiation;

public:
    ~IndexMergerFactory();

public:
    IndexMerger* CreateIndexMerger(
            IndexType type, const plugin::PluginManagerPtr& pluginManager);
    
    void RegisterCreator(IndexMergerCreatorPtr creator);

protected:
    void Init();

protected:
#ifdef INDEX_MERGER_FACTORY_UNITTEST
    friend class IndexMergerFactoryTest;
    IndexCreatorMap& GetMap() { return mIndexMap;}
#endif

private:
    autil::RecursiveThreadMutex mLock;
    IndexCreatorMap mIndexMap;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexMergerFactory);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_INDEX_MERGER_FACTORY_H
