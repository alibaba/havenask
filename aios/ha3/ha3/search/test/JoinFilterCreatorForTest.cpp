#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/JoinFilter.h>
#include <autil/mem_pool/Pool.h>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <ha3/search/test/FakeJoinDocidReader.h>
#include <ha3/search/test/JoinFilterCreatorForTest.h>

using namespace std;
using namespace suez::turing;

BEGIN_HA3_NAMESPACE(search);

HA3_LOG_SETUP(search, JoinFilterCreatorForTest);

JoinFilterCreatorForTest::JoinFilterCreatorForTest(autil::mem_pool::Pool *pool,
        common::Ha3MatchDocAllocator* allocator)
{
    _pool = pool;
    _ownAllocator = false;
    if (!allocator) {
        _mdAllocator = new common::Ha3MatchDocAllocator(_pool, true);
        _ownAllocator = true;
    }
    else {
        _mdAllocator = allocator;
    }

    _converterFactory = new JoinDocIdConverterCreator(_pool,
            _mdAllocator);
    _subDocIdRef = _mdAllocator->getSubDocRef();
}

JoinFilterCreatorForTest::~JoinFilterCreatorForTest() { 
    //DELETE_AND_SET_NULL(_subDocIdRef);
    DELETE_AND_SET_NULL(_converterFactory);
    if (_ownAllocator)
    {
        DELETE_AND_SET_NULL(_mdAllocator);
    }
}

JoinFilterPtr JoinFilterCreatorForTest::createJoinFilter(vector<vector<int> > converterDocIdMaps, vector<bool> isSubs, vector<bool> isStrongJoins, bool forceStrongJoin) {
    for (size_t i = 0; i < converterDocIdMaps.size(); i++) {
        map<docid_t, docid_t> docIdMap;
        vector<int> docIds = converterDocIdMaps[i];
        for (size_t j = 0; j < docIds.size(); j++) {
            docIdMap[j] = docIds[j];
        }
        string attrName = string("attr") + autil::StringUtil::toString(i);
        FakeJoinDocidReader *joinDocidReader = 
            POOL_NEW_CLASS(_pool, FakeJoinDocidReader);
        joinDocidReader->setDocIdMap(docIdMap);
        JoinDocIdConverterBase * joinDocidConverter = 
            POOL_NEW_CLASS(_pool, JoinDocIdConverterBase, attrName, 
                           joinDocidReader);
        joinDocidConverter->setStrongJoin(isStrongJoins[i]);
        if (isSubs[i]) {
            joinDocidConverter->setSubDocIdRef(_subDocIdRef);
        }
        _converterFactory->addConverter(attrName, joinDocidConverter);
    }
    return JoinFilterPtr(new JoinFilter(_converterFactory, forceStrongJoin));
}

END_HA3_NAMESPACE(search);

