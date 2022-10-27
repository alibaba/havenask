#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <ha3/common/GlobalIdentifier.h>
#include <ha3/common/CommonDef.h>
#include <matchdoc/MatchDoc.h>
#include <ha3/common/test/FakeClass4VSAllocator.h>
#include <matchdoc/Reference.h>
#include <matchdoc/SubDocAccessor.h>
#include <ha3_sdk/testlib/common/MatchDocCreator.h>
#include <string>
#include <vector>
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
using namespace suez::turing;

USE_HA3_NAMESPACE(common);
BEGIN_HA3_NAMESPACE(search);

class MatchDocAllocatorTest : public TESTBASE {
public:
    MatchDocAllocatorTest();
    ~MatchDocAllocatorTest();
public:
    void setUp();
    void tearDown();

protected:
    autil::mem_pool::Pool _pool;
    common::Ha3MatchDocAllocator *_matchDocAllocator;
    matchdoc::MatchDoc _matchDoc;
    matchdoc::MatchDoc _matchDoc2;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(search, MatchDocAllocatorTest);


MatchDocAllocatorTest::MatchDocAllocatorTest() {
    _matchDocAllocator = new common::Ha3MatchDocAllocator(&_pool);
    _matchDoc = matchdoc::INVALID_MATCHDOC;
    _matchDoc2 = matchdoc::INVALID_MATCHDOC;
}

MatchDocAllocatorTest::~MatchDocAllocatorTest() {
}

void MatchDocAllocatorTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");
    _matchDoc = matchdoc::INVALID_MATCHDOC;
    _matchDoc2 = matchdoc::INVALID_MATCHDOC;
}

void MatchDocAllocatorTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");
    if (matchdoc::INVALID_MATCHDOC != _matchDoc) {
        _matchDocAllocator->deallocate(_matchDoc);
        _matchDoc = matchdoc::INVALID_MATCHDOC;
    }

    if (matchdoc::INVALID_MATCHDOC != _matchDoc2) {
        _matchDocAllocator->deallocate(_matchDoc2);
        _matchDoc2 = matchdoc::INVALID_MATCHDOC;
    }
    delete _matchDocAllocator;
}

TEST_F(MatchDocAllocatorTest, testAllocMatchDoc) {
    HA3_LOG(DEBUG, "Begin Test!");

    _matchDoc = _matchDocAllocator->allocate((docid_t)1);
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC != _matchDoc);
    ASSERT_EQ((docid_t)1, _matchDoc.getDocId());
}

TEST_F(MatchDocAllocatorTest, testAllocTwoMatchDoc) {
    HA3_LOG(DEBUG, "Begin Test!");

    _matchDoc = _matchDocAllocator->allocate((docid_t)1);
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC != _matchDoc);
    ASSERT_EQ((docid_t)1, _matchDoc.getDocId());

    _matchDoc2 = _matchDocAllocator->allocate((docid_t)2);
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC != _matchDoc2);
    ASSERT_EQ((docid_t)2, _matchDoc2.getDocId());
}

TEST_F(MatchDocAllocatorTest, testAllocMatchDocWithInt32Var) {
    HA3_LOG(DEBUG, "Begin Test!");

    matchdoc::Reference<int32_t> *ref
        = _matchDocAllocator->declare<int32_t>("int32");
    _matchDoc = _matchDocAllocator->allocate((docid_t)1);
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC != _matchDoc);

    ref->set(_matchDoc, 123);

    ASSERT_EQ((docid_t)1, _matchDoc.getDocId());

    int32_t expVal = 123;
    int32_t val = ref->get(_matchDoc);
    ASSERT_EQ(expVal, val);
}

TEST_F(MatchDocAllocatorTest, testAllocMatchDocWithStringVar) {
    HA3_LOG(DEBUG, "Begin Test!");

    matchdoc::Reference<string> *ref
        = _matchDocAllocator->declare<string>("string");
    _matchDoc = _matchDocAllocator->allocate((docid_t)1);
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC != _matchDoc);

    string val = "test string";
    ref->set(_matchDoc, val);

    ASSERT_EQ((docid_t)1, _matchDoc.getDocId());

    string expVal = "test string";
    string &valRef =  ref->getReference(_matchDoc);
    ASSERT_EQ(expVal, valRef);
}

TEST_F(MatchDocAllocatorTest, testAllocMatchDocWithMultiVar) {
    HA3_LOG(DEBUG, "Begin Test!");

    matchdoc::Reference<vector<int32_t> > *vectorRef
        = _matchDocAllocator->declare<vector<int32_t> >("vector");
    matchdoc::Reference<int32_t> *intRef
        = _matchDocAllocator->declare<int32_t >("int32");
    _matchDoc = _matchDocAllocator->allocate((docid_t)1);
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC != _matchDoc);

    vector<int32_t> vectorVal;
    vectorVal.push_back(1);
    vectorVal.push_back(2);
    vectorVal.push_back(3);
    vectorRef->set(_matchDoc, vectorVal);

    int32_t intVal = 123;
    intRef->set(_matchDoc, intVal);

    ASSERT_EQ((docid_t)1, _matchDoc.getDocId());

    vector<int32_t> &vectorValRef
        = vectorRef->getReference(_matchDoc);
    ASSERT_EQ((int32_t)1, vectorValRef[0]);
    ASSERT_EQ((int32_t)2, vectorValRef[1]);
    ASSERT_EQ((int32_t)3, vectorValRef[2]);

    int32_t actIntVal = intRef->get(_matchDoc);
    ASSERT_EQ(123, actIntVal);
}


TEST_F(MatchDocAllocatorTest, testFreeMatchDoc) {
    HA3_LOG(DEBUG, "Begin Test!");

    FakeClass4VSAllocator::resetCounts();
    matchdoc::Reference<FakeClass4VSAllocator> *fcVarRef = NULL;
    fcVarRef = _matchDocAllocator->declare<FakeClass4VSAllocator>("a");
    ASSERT_TRUE(fcVarRef);

    _matchDoc = _matchDocAllocator->allocate((docid_t)1);
    ASSERT_EQ(1, FakeClass4VSAllocator::getConstructCount());

    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC != _matchDoc);
    ASSERT_EQ((docid_t)1, _matchDoc.getDocId());

    _matchDocAllocator->deallocate(_matchDoc);
    _matchDoc = matchdoc::INVALID_MATCHDOC;
    ASSERT_EQ(1, FakeClass4VSAllocator::getDestructCount());
}

TEST_F(MatchDocAllocatorTest, testAllocateSubMatchDoc) {
    common::Ha3MatchDocAllocator allocator(&_pool, true);
    ASSERT_TRUE(allocator._firstSub != NULL);
    ASSERT_TRUE(allocator._nextSub != NULL);
    ASSERT_TRUE(allocator.getSubDocRef() != NULL);

    auto subDocIdRef = allocator.getSubDocRef();
    ASSERT_TRUE(subDocIdRef != NULL);

    matchdoc::MatchDoc matchDoc = allocator.allocate(1);
    allocator.allocateSub(matchDoc, 3);
    auto &docIdentifier =
        subDocIdRef->getReference(matchDoc);
    ASSERT_EQ(docid_t(3), docIdentifier.getDocId());

    matchdoc::Reference<matchdoc::MatchDoc> *firstSubSlabRef = allocator._firstSub;
    matchdoc::MatchDoc subSlab = firstSubSlabRef->getReference(matchDoc);
    matchdoc::MatchDoc subDoc = (matchdoc::MatchDoc)subSlab;
    ASSERT_EQ(docid_t(3), subDoc.getDocId());

    allocator.deallocate(matchDoc);
}

TEST_F(MatchDocAllocatorTest, testInitReference) {
    common::Ha3MatchDocAllocator allocator(&_pool, true);
    auto subDocIdRef = allocator.getSubDocRef();
    ASSERT_TRUE(subDocIdRef != NULL);

    uint8_t phaseOneBaseInfoMask = 0;
    phaseOneBaseInfoMask |= pob_indexversion_flag;
    phaseOneBaseInfoMask |= pob_fullversion_flag;
    phaseOneBaseInfoMask |= pob_ip_flag;
    phaseOneBaseInfoMask |= pob_primarykey64_flag;

    allocator.initPhaseOneInfoReferences(phaseOneBaseInfoMask);

    matchdoc::Reference<uint64_t> *pkRef =
        allocator.findReference<uint64_t>(PRIMARYKEY_REF);
    ASSERT_TRUE(pkRef != NULL);
    ASSERT_EQ(SL_CACHE, pkRef->getSerializeLevel());
}

class SubDocTester {
public:
    SubDocTester(const string& docStr, const matchdoc::Reference<matchdoc::MatchDoc>* subDocIdRef)
        : _cur(0),
          _subDocIdRef(subDocIdRef)
    {
        StringTokenizer st(docStr, ",", StringTokenizer::TOKEN_IGNORE_EMPTY |
                           StringTokenizer::TOKEN_TRIM);
        for (size_t i = 0; i < st.getNumTokens(); i++) {
            _subDocs.push_back(StringUtil::fromString<docid_t>(st[i]));
        }
    }
public:
    void operator()(matchdoc::MatchDoc slab) {
        ASSERT_TRUE(_cur < _subDocs.size());
        ASSERT_EQ(_subDocs[_cur], _subDocIdRef->getReference(slab).getDocId());
        _cur++;
    }
private:
    size_t _cur;
    const matchdoc::Reference<matchdoc::MatchDoc>* _subDocIdRef;
    vector<docid_t> _subDocs;
};

TEST_F(MatchDocAllocatorTest, testAllocatorSubDocIterator) {
    common::Ha3MatchDocAllocator allocator(&_pool, true);

    SimpleDocIdIterator docIdIterator =
        MatchDocCreator::createDocIterator("1:2,4,6; 5:10,13; 10:;");
    {
        matchdoc::MatchDoc matchDoc = allocator.allocateWithSubDoc(&docIdIterator);
        SubDocTester tester("2,4,6", allocator.getSubDocRef());
        allocator.getSubDocAccessor()->foreach(matchDoc, tester);
    }
    docIdIterator.next();
    {
        matchdoc::MatchDoc matchDoc = allocator.allocateWithSubDoc(&docIdIterator);
        SubDocTester tester("10,13", allocator.getSubDocRef());
        allocator.getSubDocAccessor()->foreach(matchDoc, tester);
    }
    docIdIterator.next();
    {
        matchdoc::MatchDoc matchDoc = allocator.allocateWithSubDoc(&docIdIterator);
        SubDocTester tester("", allocator.getSubDocRef());
        allocator.getSubDocAccessor()->foreach(matchDoc, tester);
    }
}

END_HA3_NAMESPACE(search);
