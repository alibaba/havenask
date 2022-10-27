#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/JoinFilter.h>
#include <autil/mem_pool/Pool.h>
#include <suez/turing/expression/framework/JoinDocIdConverterCreator.h>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <ha3/search/test/FakeJoinDocidReader.h>

using namespace std;
using namespace autil::mem_pool;
using namespace suez::turing;
USE_HA3_NAMESPACE(common);
BEGIN_HA3_NAMESPACE(search);

class JoinFilterTest : public TESTBASE {
public:
    JoinFilterTest();
    ~JoinFilterTest();
public:
    void setUp();
    void tearDown();
protected:
    void innerTestFilter(bool forceStrongJoin,
                         bool subDocJoin,
                         const std::vector<docid_t> &docIds, 
                         const std::vector<bool> &expectedResult);
protected:
    autil::mem_pool::Pool *_pool;
    common::Ha3MatchDocAllocator *_mdAllocator;
    JoinDocIdConverterCreator *_converterFactory;
    matchdoc::Reference<matchdoc::MatchDoc> *_subDocIdRef;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(search, JoinFilterTest);


JoinFilterTest::JoinFilterTest() { 
    _pool = NULL;
    _mdAllocator = NULL;
    _converterFactory = NULL;
    _subDocIdRef = NULL;
}

JoinFilterTest::~JoinFilterTest() { 
}

void JoinFilterTest::setUp() { 
    _pool = new Pool();
    _mdAllocator = new common::Ha3MatchDocAllocator(_pool, true);
    _converterFactory = new JoinDocIdConverterCreator(_pool,
            _mdAllocator);
    _subDocIdRef = _mdAllocator->getSubDocRef();
    map<docid_t, docid_t> docIdMap;
    docIdMap[0] = 0;
    docIdMap[1] = 0;
    docIdMap[2] = 1;
    docIdMap[3] = -1;
    FakeJoinDocidReader *joinDocidReader = POOL_NEW_CLASS(_pool, FakeJoinDocidReader);
    joinDocidReader->setDocIdMap(docIdMap);
    JoinDocIdConverterBase *fakeConverter = POOL_NEW_CLASS(_pool, JoinDocIdConverterBase, "attr1", joinDocidReader);
    fakeConverter->setStrongJoin(false);
    _converterFactory->addConverter("attr1", fakeConverter);

    docIdMap[0] = 0;
    docIdMap[1] = -1;
    docIdMap[2] = -1;
    docIdMap[3] = 1;
    joinDocidReader = POOL_NEW_CLASS(_pool, FakeJoinDocidReader);
    joinDocidReader->setDocIdMap(docIdMap);
    fakeConverter = POOL_NEW_CLASS(_pool, JoinDocIdConverterBase, "attr2", joinDocidReader);
    fakeConverter->setStrongJoin(true);
    _converterFactory->addConverter("attr2", fakeConverter);

    docIdMap[0] = -1;
    docIdMap[1] = 0;
    docIdMap[2] = 1;
    docIdMap[3] = -1;
    joinDocidReader = POOL_NEW_CLASS(_pool, FakeJoinDocidReader);
    joinDocidReader->setDocIdMap(docIdMap);
    fakeConverter = POOL_NEW_CLASS(_pool, JoinDocIdConverterBase, "attr3", joinDocidReader);
    fakeConverter->setStrongJoin(false);
    fakeConverter->setSubDocIdRef(_subDocIdRef);
    _converterFactory->addConverter("attr3", fakeConverter);

    docIdMap[0] = 1;
    docIdMap[1] = -1;
    docIdMap[2] = 1;
    docIdMap[3] = 1;
    joinDocidReader = POOL_NEW_CLASS(_pool, FakeJoinDocidReader);
    joinDocidReader->setDocIdMap(docIdMap);
    fakeConverter = POOL_NEW_CLASS(_pool, JoinDocIdConverterBase, "attr4", joinDocidReader);
    fakeConverter->setStrongJoin(true);
    fakeConverter->setSubDocIdRef(_subDocIdRef);
    _converterFactory->addConverter("attr4", fakeConverter);
}

void JoinFilterTest::tearDown() { 
    DELETE_AND_SET_NULL(_converterFactory);
    DELETE_AND_SET_NULL(_mdAllocator);
    DELETE_AND_SET_NULL(_pool);
}

TEST_F(JoinFilterTest, testWithoutForceStrongJoin) { 
    vector<docid_t> docIds;
    docIds.push_back(0);
    docIds.push_back(1);
    docIds.push_back(2);
    docIds.push_back(3);

    vector<bool> expectedResults;
    expectedResults.push_back(true);
    expectedResults.push_back(false);
    expectedResults.push_back(false);
    expectedResults.push_back(true);

    innerTestFilter(false, false, docIds, expectedResults);
}

TEST_F(JoinFilterTest, testWithForceStrongJoin) { 
    vector<docid_t> docIds;
    docIds.push_back(0);
    docIds.push_back(1);
    docIds.push_back(2);
    docIds.push_back(3);

    vector<bool> expectedResults;
    expectedResults.push_back(true);
    expectedResults.push_back(false);
    expectedResults.push_back(false);
    expectedResults.push_back(false);

    innerTestFilter(true, false, docIds, expectedResults);
}

TEST_F(JoinFilterTest, testSubDocJoinWithoutForceStrongJoin) { 
    vector<docid_t> docIds;
    docIds.push_back(0);
    docIds.push_back(1);
    docIds.push_back(2);
    docIds.push_back(3);

    vector<bool> expectedResults;
    expectedResults.push_back(true);
    expectedResults.push_back(false);
    expectedResults.push_back(true);
    expectedResults.push_back(true);

    innerTestFilter(false, true, docIds, expectedResults);
}

TEST_F(JoinFilterTest, testSubDocJoinWithForceStrongJoin) { 
    vector<docid_t> docIds;
    docIds.push_back(0);
    docIds.push_back(1);
    docIds.push_back(2);
    docIds.push_back(3);

    vector<bool> expectedResults;
    expectedResults.push_back(false);
    expectedResults.push_back(false);
    expectedResults.push_back(true);
    expectedResults.push_back(false);

    innerTestFilter(true, true, docIds, expectedResults);
}

void JoinFilterTest::innerTestFilter(
        bool forceStrongJoin,
        bool subDocJoin,
        const vector<docid_t> &docIds, 
        const vector<bool> &expectedResult)
{
    JoinFilter joinFilter(_converterFactory, forceStrongJoin);
    for (size_t i = 0; i < docIds.size(); i++) {
        matchdoc::MatchDoc matchDoc = _mdAllocator->allocate(docIds[i]);
        if (!subDocJoin) {
            ASSERT_EQ(expectedResult[i], 
                    joinFilter.pass(matchDoc));
        } else {
            //common::DocIdentifier identifier;
            //identifier.setDocId(docIds[i]);
            matchdoc::MatchDoc subDoc;
            subDoc.setDocId(docIds[i]);
            _subDocIdRef->set(matchDoc, subDoc);
            ASSERT_EQ(expectedResult[i], 
                    joinFilter.passSubDoc(matchDoc));
        }
        _mdAllocator->deallocate(matchDoc);
    }
}

END_HA3_NAMESPACE(search);

