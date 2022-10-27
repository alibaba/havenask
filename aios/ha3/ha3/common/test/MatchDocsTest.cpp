#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/MatchDocs.h>
#include <ha3/common/test/ResultConstructor.h>
#include <matchdoc/MatchDoc.h>
#include <matchdoc/Reference.h>
#include <matchdoc/SubDocAccessor.h>
#include <ha3/common/GlobalIdentifier.h>
#include <autil/TimeUtility.h>
#include <autil/MultiValueType.h>
#include <autil/MultiValueCreator.h>
#include <string>

using namespace std;
using namespace autil;
using namespace suez::turing;

USE_HA3_NAMESPACE(util);
BEGIN_HA3_NAMESPACE(common);

class MatchDocsTest : public TESTBASE {
public:
    MatchDocsTest();
    ~MatchDocsTest();
public:
    void setUp();
    void tearDown();
protected:
    void prepareMatchDocs();
    void checkDeserialize(autil::DataBuffer &dataBuffer, uint8_t serializeLevel,
                          bool hasRef1, bool hasRef2, bool hasRef3,
                          docid_t expectedDocId, int32_t value1,
                          int32_t value2, int32_t value3);
protected:
    autil::mem_pool::Pool _pool;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(common, MatchDocsTest);


MatchDocsTest::MatchDocsTest() {
}

MatchDocsTest::~MatchDocsTest() {
}

void MatchDocsTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");
}

void MatchDocsTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(MatchDocsTest, testSerializeAndDeserializeWithoutJson) {
    HA3_LOG(DEBUG, "Begin Test!");

    ResultConstructor resultConstructor;
    MatchDocs *matchDocs = new MatchDocs();
    uint8_t phaseOneInfoFlag = 0;
    resultConstructor.prepareMatchDocs(matchDocs, &_pool, phaseOneInfoFlag);
    ASSERT_TRUE(matchDocs);
    autil::DataBuffer dataBuffer(autil::DataBuffer::DEFAUTL_DATA_BUFFER_SIZE, &_pool);
    matchDocs->serialize(dataBuffer);

    MatchDocs resultMatchDocs;
    resultMatchDocs.deserialize(dataBuffer, &_pool);

    ASSERT_EQ((uint32_t)10, resultMatchDocs.totalMatchDocs());
    ASSERT_EQ((uint32_t)9, resultMatchDocs.actualMatchDocs());
    ASSERT_EQ((uint32_t)2, resultMatchDocs.size());
    ASSERT_TRUE(!resultMatchDocs.hasPrimaryKey());

    matchdoc::MatchDoc resultMatchDoc1 = resultMatchDocs.getMatchDoc(0);
    ASSERT_EQ((docid_t)100, resultMatchDoc1.getDocId());
    matchdoc::MatchDoc resultMatchDoc2 = resultMatchDocs.getMatchDoc(1);
    ASSERT_EQ((docid_t)101, resultMatchDoc2.getDocId());

    const auto &resultVSAPtr = resultMatchDocs.getMatchDocAllocator();
    ASSERT_TRUE(resultVSAPtr != NULL);

    auto referenceVec = resultVSAPtr->getAllNeedSerializeReferences(SL_NONE + 1);
    ASSERT_EQ((uint32_t)13, referenceVec.size());

    auto resultRefer1 = resultVSAPtr->findReference<double>(
            PROVIDER_VAR_NAME_PREFIX + "ref2");
    ASSERT_TRUE(resultRefer1);
    auto resultRefer2 = resultVSAPtr->findReference<int32_t>(
            PROVIDER_VAR_NAME_PREFIX + "ref3");
    ASSERT_TRUE(resultRefer2);
    auto resultRefer3 = resultVSAPtr->findReference<string>(
            PROVIDER_VAR_NAME_PREFIX + "ref4");
    ASSERT_TRUE(resultRefer3);

    auto resultRefer4 = resultVSAPtr->findReference<MultiChar>("ref5");
    ASSERT_TRUE(resultRefer4);

    auto resultRefer5 = resultVSAPtr->findReference<double>(
            "SORT_FIELD_PREFIX_0");
    ASSERT_TRUE(resultRefer5);

    auto resultRefer6 = resultVSAPtr->findReference<int32_t>(
            "SORT_FIELD_PREFIX_1");
    ASSERT_TRUE(resultRefer6);

    auto resultRefer7 = resultVSAPtr->findReference<MultiDouble>(
            PROVIDER_VAR_NAME_PREFIX + "ref8");
    ASSERT_TRUE(resultRefer7);

    auto resultRefer8 = resultVSAPtr->findReference<MultiString>(
            PROVIDER_VAR_NAME_PREFIX + "ref9");
    ASSERT_TRUE(resultRefer8);


    ASSERT_EQ((double)1.5f,
              resultRefer1->getReference(resultMatchDoc1));
    ASSERT_EQ((int32_t)777,
              resultRefer2->getReference(resultMatchDoc1));

    ASSERT_EQ(string("abc"),
              resultRefer3->getReference(resultMatchDoc1));
    MultiValueType<char> tmpMultiChar;
    char buf[] = "abcd";
    tmpMultiChar.init(MultiValueCreator::createMultiValueBuffer<char>(buf, 4, &_pool));
    ASSERT_EQ(tmpMultiChar, resultRefer4->getReference(
                    resultMatchDoc1));
    ASSERT_EQ((double)11.1,
              resultRefer5->getReference(resultMatchDoc1));
    ASSERT_EQ((int32_t)233,
              resultRefer6->getReference(resultMatchDoc1));
    static double DOUBLE_DATA[] = {1.1, 2.2, 3.4};
    MultiDouble multiDouble_1;
    multiDouble_1.init(MultiValueCreator::createMultiValueBuffer(DOUBLE_DATA, 3, &_pool));
    MultiDouble multiDouble_2;
    multiDouble_2.init(MultiValueCreator::createMultiValueBuffer(DOUBLE_DATA, 2, &_pool));
    ASSERT_EQ(multiDouble_1,
              resultRefer7->getReference(resultMatchDoc1));
    vector<string> multiStrVec;
    multiStrVec.push_back("abc");
    multiStrVec.push_back("123456");
    MultiString multiString;
    multiString.init(MultiValueCreator::createMultiStringBuffer(multiStrVec, &_pool));

    ASSERT_EQ(multiString,
              resultRefer8->getReference(resultMatchDoc1));

    ASSERT_EQ((double)1.5f,
              resultRefer1->getReference(resultMatchDoc2));
    ASSERT_EQ((int32_t)777,
              resultRefer2->getReference(resultMatchDoc2));
    ASSERT_EQ(string("abc"),
              resultRefer3->getReference(resultMatchDoc2));
    ASSERT_EQ(tmpMultiChar, resultRefer4->getReference(
                    resultMatchDoc2));
    ASSERT_EQ((double)99.9,
              resultRefer5->getReference(resultMatchDoc2));
    ASSERT_EQ((int32_t)345,
              resultRefer6->getReference(resultMatchDoc2));
    ASSERT_EQ(multiDouble_2,
              resultRefer7->getReference(resultMatchDoc2));
    ASSERT_EQ(multiString,
              resultRefer8->getReference(resultMatchDoc2));

    delete matchDocs;
}

#define ASSERT_EXTRA_DOCID_HELPER(isExist, value, type, name)           \
    do {                                                                \
        matchdoc::Reference<type> *ref = matchDocs->get##name##Ref();   \
        ASSERT_EQ(bool(isExist), ref != NULL);                          \
        if (!ref) {                                                     \
            break;                                                      \
        }                                                               \
        ASSERT_EQ((value), ref->getReference(matchDoc));                \
    } while (0)

#define TEST_SET_GLOBALID_HELPER(phaseOneInfoFlag)                      \
    do {                                                                \
        MatchDocs *matchDocs = new MatchDocs();                         \
        uint8_t phaseOneInfo = phaseOneInfoFlag;                        \
        resultConstructor.prepareMatchDocs(matchDocs, &_pool, phaseOneInfo); \
        ASSERT_TRUE(matchDocs);                                         \
        matchDocs->setGlobalIdInfo(1, versionid_t(2), 3, 4, phaseOneInfo); \
        auto hashIdRef = matchDocs->getHashIdRef();                     \
        for(uint32_t i = 0; i < matchDocs->size(); ++i) {               \
            auto matchDoc = matchDocs->getMatchDoc(i);                  \
            ASSERT_EQ(hashid_t(1), hashIdRef->get(matchDocs->getMatchDoc(i))); \
            ASSERT_EXTRA_DOCID_HELPER(PHASE_ONE_HAS_FULL_VERSION(phaseOneInfo), \
                    FullIndexVersion(3), FullIndexVersion, FullIndexVersion); \
            ASSERT_EXTRA_DOCID_HELPER(PHASE_ONE_HAS_INDEX_VERSION(phaseOneInfo), \
                    versionid_t(2), versionid_t, IndexVersion);         \
            ASSERT_EXTRA_DOCID_HELPER(PHASE_ONE_HAS_IP(phaseOneInfo),   \
                    uint32_t(4), uint32_t, Ip);                         \
        }                                                               \
        delete matchDocs;                                               \
    } while (0)

TEST_F(MatchDocsTest, testSetGlobalIdInfo) {
    HA3_LOG(DEBUG, "Begin Test!");

    ResultConstructor resultConstructor;
    TEST_SET_GLOBALID_HELPER(pob_primarykey64_flag);
    TEST_SET_GLOBALID_HELPER(pob_indexversion_flag);
    TEST_SET_GLOBALID_HELPER(pob_fullversion_flag);
    TEST_SET_GLOBALID_HELPER(pob_ip_flag);
    TEST_SET_GLOBALID_HELPER(pob_primarykey64_flag | pob_primarykey128_flag | pob_fullversion_flag);
}

TEST_F(MatchDocsTest, testSetGlobalIdInfoNoExtraRef) {
    HA3_LOG(DEBUG, "Begin Test!");

    ResultConstructor resultConstructor;
    MatchDocs *matchDocs = new MatchDocs();
    resultConstructor.prepareMatchDocs(matchDocs, &_pool, 0);
    ASSERT_TRUE(matchDocs);
    matchDocs->setGlobalIdInfo(1, versionid_t(2), 3, 4, 0);
    auto hashIdRef = matchDocs->getHashIdRef();
    for(uint32_t i = 0; i < matchDocs->size(); ++i) {
        ASSERT_EQ(hashid_t(1), hashIdRef->get(matchDocs->getMatchDoc(i)));
    }
    delete matchDocs;
}

TEST_F(MatchDocsTest, testSerializeAndDeserializeVector) {
    MatchDocs *matchDocs = new MatchDocs();
    matchDocs->setTotalMatchDocs(2);
    matchDocs->setActualMatchDocs(1);
    common::Ha3MatchDocAllocator *mdAllocator = new common::Ha3MatchDocAllocator(&_pool);
    common::Ha3MatchDocAllocatorPtr vsa(mdAllocator);
    matchDocs->setMatchDocAllocator(vsa);

    typedef vector<int8_t> VI8;
    typedef vector<uint8_t> VUI8;
    typedef vector<int16_t> VI16;
    typedef vector<int32_t> VI32;
    typedef vector<int64_t> VI64;
    typedef vector<float> VF;
    typedef vector<double> VD;

    matchdoc::Reference<VI8> *ref1 = vsa->declare<VI8>("var1", SL_QRS);
    matchdoc::Reference<VUI8> *ref2 = vsa->declare<VUI8>("var2", SL_QRS);
    matchdoc::Reference<VI16> *ref3 = vsa->declare<VI16>("var3", SL_QRS);
    matchdoc::Reference<VI32> *ref4 = vsa->declare<VI32>("var4", SL_QRS);
    matchdoc::Reference<VI64> *ref5 = vsa->declare<VI64>("var5", SL_QRS);
    matchdoc::Reference<VF> *ref6 = vsa->declare<VF>("var6", SL_QRS);
    matchdoc::Reference<VD> *ref7 = vsa->declare<VD>("var7", SL_QRS);
    ASSERT_TRUE(ref1 && ref2 && ref3 && ref4 && ref6 && ref7);

#define setValue(type, ref, matchDoc) do {      \
        vector<type> v;                         \
        v.push_back((type)1.1);                 \
        v.push_back((type)2.2);                 \
        v.push_back((type)3.3);                 \
        ref->set(matchDoc, v);                  \
    } while(0)
    matchdoc::MatchDoc matchDoc = mdAllocator->allocate(0);
    setValue(int8_t, ref1, matchDoc);
    setValue(uint8_t, ref2, matchDoc);
    setValue(int16_t, ref3, matchDoc);
    setValue(int32_t, ref4, matchDoc);
    setValue(int64_t, ref5, matchDoc);
    setValue(float, ref6, matchDoc);
    setValue(double, ref7, matchDoc);
#undef setValue
    matchDocs->addMatchDoc(matchDoc);

    autil::DataBuffer dataBuffer(autil::DataBuffer::DEFAUTL_DATA_BUFFER_SIZE, &_pool);
    matchDocs->serialize(dataBuffer);

    MatchDocs resultMatchDocs;
    resultMatchDocs.deserialize(dataBuffer, &_pool);

    matchdoc::MatchDoc deserializedMatchDoc = resultMatchDocs.getMatchDoc(0);
    ASSERT_TRUE(deserializedMatchDoc != matchdoc::INVALID_MATCHDOC);
    ASSERT_EQ((docid_t)0, deserializedMatchDoc.getDocId());

    const auto &resultVSAPtr = resultMatchDocs.getMatchDocAllocator();
    ASSERT_TRUE(resultVSAPtr != NULL);

    auto resultRefVec = resultVSAPtr->getAllNeedSerializeReferences(SL_NONE + 1);
    ASSERT_EQ(uint32_t(7), resultRefVec.size());

#define checkValue(type, ref, matchDoc) do {                            \
        vector<type> expectedValue;                                     \
        expectedValue.push_back((type)1.1);                             \
        expectedValue.push_back((type)2.2);                             \
        expectedValue.push_back((type)3.3);                             \
        vector<type> actualValue;                                       \
        auto typedRef = dynamic_cast<matchdoc::Reference<vector<type> > *>(ref); \
        actualValue = typedRef->get(matchDoc);                          \
        ASSERT_TRUE(expectedValue == actualValue);                      \
    } while(0)

    checkValue(int8_t, resultRefVec[0], deserializedMatchDoc);
    checkValue(uint8_t, resultRefVec[1], deserializedMatchDoc);
    checkValue(int16_t, resultRefVec[2], deserializedMatchDoc);
    checkValue(int32_t, resultRefVec[3], deserializedMatchDoc);
    checkValue(int64_t, resultRefVec[4], deserializedMatchDoc);
    checkValue(float, resultRefVec[5], deserializedMatchDoc);
    checkValue(double, resultRefVec[6], deserializedMatchDoc);
#undef checkValue
    delete matchDocs;
}

TEST_F(MatchDocsTest, testSerializeAndDeserializeWithSubMatchDoc) {
    MatchDocs *matchDocs = new MatchDocs();
    matchDocs->setTotalMatchDocs(3);
    matchDocs->setActualMatchDocs(2);
    common::Ha3MatchDocAllocator *mdAllocator = new common::Ha3MatchDocAllocator(&_pool, true);
    common::Ha3MatchDocAllocatorPtr vsa(mdAllocator);
    matchDocs->setMatchDocAllocator(vsa);

    auto ref1 = vsa->declare<int32_t>("var1", SL_QRS);
    auto subRef1 = vsa->declareSub<int32_t>("sub_var1", SL_QRS);
    auto subRef2 = vsa->declareSub<double>("sub_var2", SL_NONE);
    auto matchDoc1 = mdAllocator->allocate(3);
    ref1->set(matchDoc1, 888);
    mdAllocator->allocateSub(matchDoc1, 0);
    subRef1->set(matchDoc1, 1234);
    subRef2->set(matchDoc1, 1.23);
    mdAllocator->allocateSub(matchDoc1, 2);
    subRef1->set(matchDoc1, 5678);
    subRef2->set(matchDoc1, 5.67);

    auto matchDoc2 = mdAllocator->allocate(7);
    ref1->set(matchDoc2, 999);
    matchDocs->addMatchDoc(matchDoc1);
    matchDocs->addMatchDoc(matchDoc2);

    autil::DataBuffer dataBuffer(autil::DataBuffer::DEFAUTL_DATA_BUFFER_SIZE, &_pool);
    matchDocs->serialize(dataBuffer);
    DELETE_AND_SET_NULL(matchDocs);

    MatchDocs resultMatchDocs;
    resultMatchDocs.deserialize(dataBuffer, &_pool);

    ASSERT_EQ((uint32_t)2, resultMatchDocs.size());
    matchdoc::MatchDoc resultMatchDoc1 = resultMatchDocs.getMatchDoc(0);
    ASSERT_EQ(3, resultMatchDoc1.getDocId());
    matchdoc::MatchDoc resultMatchDoc2 = resultMatchDocs.getMatchDoc(1);
    ASSERT_EQ(7, resultMatchDoc2.getDocId());

    const auto &resultVsa = resultMatchDocs.getMatchDocAllocator();
    auto refVec = resultVsa->getAllNeedSerializeReferences(SL_NONE + 1);
    ASSERT_EQ((uint32_t)1, refVec.size());
    typedef matchdoc::Reference<int32_t> Int32VariableReference;
    Int32VariableReference *resultRef1 = (Int32VariableReference*)refVec[0];
    ASSERT_EQ(888, resultRef1->getReference(resultMatchDoc1));
    ASSERT_EQ(999, resultRef1->getReference(resultMatchDoc2));

    ASSERT_TRUE(resultVsa->_subAllocator);
    auto subRefVec = resultVsa->_subAllocator->getAllNeedSerializeReferences(SL_NONE + 1);
    ASSERT_EQ((size_t)1, subRefVec.size());

    matchdoc::SubDocAccessor *accessor = resultVsa->getSubDocAccessor();
    matchdoc::SubCounter counter1;
    accessor->foreach(resultMatchDoc1, counter1);
    ASSERT_EQ((uint32_t)2, counter1.get());
    std::vector<matchdoc::MatchDoc> subDocs;
    vector<int32_t> values;
    matchdoc::Reference<int32_t> *valueRef = (matchdoc::Reference<int32_t>*)subRefVec[0];
    accessor->getSubDocs(resultMatchDoc1, subDocs);
    accessor->getSubDocValues(resultMatchDoc1, valueRef, values);

    ASSERT_EQ(size_t(2), subDocs.size());
    ASSERT_EQ(0, subDocs[0].getDocId());
    ASSERT_EQ(2, subDocs[1].getDocId());
    ASSERT_EQ(size_t(2), values.size());
    ASSERT_EQ(1234, values[0]);
    ASSERT_EQ(5678, values[1]);

    matchdoc::SubCounter counter2;
    accessor->foreach(resultMatchDoc2, counter2);
    ASSERT_EQ((uint32_t)0, counter2.get());
}

TEST_F(MatchDocsTest, testSerializeLevel) {
    MatchDocs *matchDocs = new MatchDocs();
    matchDocs->setTotalMatchDocs(2);
    matchDocs->setActualMatchDocs(1);
    common::Ha3MatchDocAllocator *mdAllocator = new common::Ha3MatchDocAllocator(&_pool);
    common::Ha3MatchDocAllocatorPtr vsa(mdAllocator);
    matchDocs->setMatchDocAllocator(vsa);

    autil::DataBuffer dataBuffer1(autil::DataBuffer::DEFAUTL_DATA_BUFFER_SIZE, &_pool);
    autil::DataBuffer dataBuffer2(autil::DataBuffer::DEFAUTL_DATA_BUFFER_SIZE, &_pool);
    autil::DataBuffer dataBuffer3(autil::DataBuffer::DEFAUTL_DATA_BUFFER_SIZE, &_pool);

    matchdoc::Reference<int32_t> *ref1 =
        vsa->declare<int32_t>("ref1", SL_NONE);
    ref1->setSerializeLevel(SL_QRS);
    matchdoc::Reference<int32_t> *ref2 =
        vsa->declare<int32_t>("ref2", SL_NONE);
    ref2->setSerializeLevel(SL_CACHE);
    matchdoc::Reference<int32_t> *ref3 =
        vsa->declare<int32_t>("ref3", SL_NONE);
    ref3->setSerializeLevel(SL_ATTRIBUTE);

    matchdoc::MatchDoc matchDoc = mdAllocator->allocate(1);
    ref1->set(matchDoc, 1);
    ref2->set(matchDoc, 2);
    ref3->set(matchDoc, 3);
    matchDocs->addMatchDoc(matchDoc);

    matchDocs->setSerializeLevel(SL_QRS);
    matchDocs->serialize(dataBuffer1);

    matchDocs->setSerializeLevel(SL_CACHE);
    matchDocs->serialize(dataBuffer2);

    matchDocs->setSerializeLevel(SL_ATTRIBUTE);
    matchDocs->serialize(dataBuffer3);

    DELETE_AND_SET_NULL(matchDocs);
    ASSERT_TRUE(dataBuffer1.getDataLen() > dataBuffer2.getDataLen());
    ASSERT_TRUE(dataBuffer2.getDataLen() > dataBuffer3.getDataLen());

    #define NO_USE 0
    checkDeserialize(dataBuffer1, SL_QRS, true, true, true, 1, 1, 2, 3);
    checkDeserialize(dataBuffer2, SL_CACHE, false, true, true, 1, NO_USE, 2, 3);
    checkDeserialize(dataBuffer3, SL_ATTRIBUTE, false, false, true, 1, NO_USE, NO_USE, 3);
    #undef NO_USE
}

void MatchDocsTest::checkDeserialize(autil::DataBuffer &dataBuffer, uint8_t serializeLevel,
                                     bool hasRef1, bool hasRef2, bool hasRef3,
                                     docid_t expectedDocId, int32_t value1,
                                     int32_t value2, int32_t value3)
{
    MatchDocs resultMatchDocs;
    resultMatchDocs.setSerializeLevel(serializeLevel);
    resultMatchDocs.deserialize(dataBuffer, &_pool);

    common::Ha3MatchDocAllocatorPtr vsa = resultMatchDocs.getMatchDocAllocator();
    matchdoc::Reference<int32_t> *ref1 =
        vsa->findReference<int32_t>("ref1");
    ASSERT_EQ(hasRef1, (bool)ref1);
    matchdoc::Reference<int32_t> *ref2 =
        vsa->findReference<int32_t>("ref2");
    ASSERT_EQ(hasRef2, (bool)ref2);
    matchdoc::Reference<int32_t> *ref3 =
        vsa->findReference<int32_t>("ref3");
    ASSERT_EQ(hasRef3, (bool)ref3);

    ASSERT_EQ(1u, resultMatchDocs.size());
    auto matchDoc = resultMatchDocs.getMatchDoc(0);
    ASSERT_EQ(expectedDocId, matchDoc.getDocId());

    if (ref1) {
        ASSERT_EQ(SL_QRS, ref1->getSerializeLevel());
        ASSERT_EQ(value1, ref1->getReference(matchDoc));
    }
    if (ref2) {
        ASSERT_EQ(SL_CACHE, ref2->getSerializeLevel());
        ASSERT_EQ(value2, ref2->getReference(matchDoc));
    }
    if (ref3) {
        ASSERT_EQ(SL_ATTRIBUTE, ref3->getSerializeLevel());
        ASSERT_EQ(value3, ref3->getReference(matchDoc));
    }
}

END_HA3_NAMESPACE(common);
