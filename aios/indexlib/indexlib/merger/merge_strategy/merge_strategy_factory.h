#ifndef __INDEXLIB_MERGE_STRATEGY_FACTORY_H
#define __INDEXLIB_MERGE_STRATEGY_FACTORY_H

#include <map>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include <tr1/memory>
#include "indexlib/merger/merge_strategy/merge_strategy.h"
#include "indexlib/misc/singleton.h"
#include "indexlib/merger/merge_strategy/merge_strategy_creator.h"
#include "autil/Lock.h"

IE_NAMESPACE_BEGIN(merger);

class MergeStrategyFactory : public misc::Singleton<MergeStrategyFactory>
{
public:
    typedef std::map<std::string, MergeStrategyCreatorPtr> CreatorMap;

protected:
    MergeStrategyFactory();
    friend class misc::LazyInstantiation;

public:
    ~MergeStrategyFactory();

public:
    MergeStrategyPtr CreateMergeStrategy(
            const std::string& identifier,
            const SegmentDirectoryPtr &segDir,
            const config::IndexPartitionSchemaPtr &schema);
    void RegisterCreator(MergeStrategyCreatorPtr creator);

protected:
    void Init();

protected:
#ifdef MERGE_STRATEGY_FACTORY_UNITTEST
    friend class MergeStrategyFactoryTest;
    CreatorMap& GetMap() { return mCreatorMap;}
#endif

private:
    autil::RecursiveThreadMutex mLock;
    CreatorMap mCreatorMap;
};

typedef std::tr1::shared_ptr<MergeStrategyFactory> MergeStrategyFactoryPtr;

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_MERGE_STRATEGY_FACTORY_H
