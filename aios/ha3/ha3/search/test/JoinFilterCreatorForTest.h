#ifndef ISEARCH_JOINFILTERCREATORFORTEST_H
#define ISEARCH_JOINFILTERCREATORFORTEST_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/JoinFilter.h>
#include <autil/mem_pool/Pool.h>
#include <suez/turing/expression/framework/JoinDocIdConverterCreator.h>
#include <ha3/common/Ha3MatchDocAllocator.h>

BEGIN_HA3_NAMESPACE(search);

class JoinFilterCreatorForTest
{
public:
    JoinFilterCreatorForTest(autil::mem_pool::Pool *pool,
                             common::Ha3MatchDocAllocator* allocator = NULL);
    ~JoinFilterCreatorForTest();
private:
    JoinFilterCreatorForTest(const JoinFilterCreatorForTest &);
    JoinFilterCreatorForTest& operator=(const JoinFilterCreatorForTest &);
public:
    JoinFilterPtr createJoinFilter(std::vector<std::vector<int> > converterDocIdMaps, std::vector<bool> isSubs, std::vector<bool> isStrongJoins, bool forceStrongJoin);
private:
    autil::mem_pool::Pool *_pool;
    common::Ha3MatchDocAllocator *_mdAllocator;
    suez::turing::JoinDocIdConverterCreator *_converterFactory;
    matchdoc::Reference<matchdoc::MatchDoc> *_subDocIdRef;
    bool _ownAllocator;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(JoinFilterCreatorForTest);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_JOINFILTERCREATORFORTEST_H
