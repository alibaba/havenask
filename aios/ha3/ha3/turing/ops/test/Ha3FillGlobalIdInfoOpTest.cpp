#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include <tensorflow/core/kernels/ops_testutil.h>
#include <tensorflow/core/framework/fake_input.h>
#include <tensorflow/core/framework/node_def_builder.h>
#include <tensorflow/core/framework/variant.h>
#include <ha3/common/Request.h>
#include <ha3/common/Result.h>
#include <ha3/turing/variant/Ha3RequestVariant.h>
#include <ha3/turing/variant/Ha3ResultVariant.h>
#include <ha3/turing/ops/test/Ha3OpTestBase.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReader.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReaderCreator.h>

using namespace std;
using namespace tensorflow;
using namespace ::testing;
using namespace suez::turing;
using namespace matchdoc;

IE_NAMESPACE_USE(index);
USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(turing);

#define TEST_IP "10.101.193.224"

class Ha3FillGlobalIdInfoOpTest : public Ha3OpTestBase {
public:
    void SetUp() override {
        OpTestBase::SetUp();
    }
protected:
    void makeCacheHit(bool hit) {
        SearcherQueryResourcePtr searcherQueryResourcePtr = 
            dynamic_pointer_cast<SearcherQueryResource>(getQueryResource());
        ASSERT_TRUE(searcherQueryResourcePtr != NULL);
        search::SearcherCacheInfo *cacheInfo = new search::SearcherCacheInfo;
        cacheInfo->isHit = hit;
        searcherQueryResourcePtr->searcherCacheInfo.reset(cacheInfo);
    }
    void makeOp() {
        TF_ASSERT_OK(
                NodeDefBuilder("myop", "Ha3FillGlobalIdInfoOp")
                .Input(FakeInput(DT_VARIANT))
                .Input(FakeInput(DT_VARIANT))
                .Finalize(node_def()));
        TF_ASSERT_OK(InitOp());
    }

    Ha3RequestVariant fakeHa3Request(uint8_t phaseOneBasicInfoMask = 0) {
        RequestPtr requestPtr(new Request(&_pool));
        ConfigClause *configClause = new ConfigClause();
        configClause->setPhaseOneBasicInfoMask(phaseOneBasicInfoMask);
        requestPtr->setConfigClause(configClause);

        Ha3RequestVariant variant(requestPtr, &_pool);
        return variant;
    }

    Ha3ResultVariant fakeHa3Result() {
        ResultPtr resultPtr(new Result());
        Ha3ResultVariant variant(resultPtr, &_pool);
        return variant;
    }

    Ha3ResultVariant fakeHa3ResultWithMatchDocs() {
        ResultPtr resultPtr(new Result());
        MatchDocs *matchDocs = new MatchDocs();

        auto matchDocAllocator = new Ha3MatchDocAllocator(&_pool);
        Ha3MatchDocAllocatorPtr matchDocAllocatorPtr(matchDocAllocator);
        matchDocs->setMatchDocAllocator(matchDocAllocatorPtr);

        matchdoc::MatchDoc matchDoc1 = matchDocAllocator->allocate(100);
        matchdoc::MatchDoc matchDoc2 = matchDocAllocator->allocate(101);
        matchDocs->addMatchDoc(matchDoc1);
        matchDocs->addMatchDoc(matchDoc2);

        resultPtr->setMatchDocs(matchDocs);

        Ha3ResultVariant variant(resultPtr, &_pool);
        return variant;
    }

    void checkOutput(uint32_t expectPort) {
        auto pOutputTensor = GetOutput(expectPort);
        ASSERT_TRUE(pOutputTensor != nullptr);
    }

    void initSearcherSessionResource(proto::Range hashIdRange, FullIndexVersion fullIndexVersion) {
        auto searcherSessionResource = dynamic_pointer_cast<SearcherSessionResource>(
                _sessionResource);
        searcherSessionResource->searcherResource.reset(new HA3_NS(service)::SearcherResource);
        searcherSessionResource->ip = TEST_IP;
        searcherSessionResource->searcherResource->setHashIdRange(hashIdRange);
        searcherSessionResource->searcherResource->setFullIndexVersion(fullIndexVersion);
    }

    void initSearcherQueryResource(versionid_t versionId) {
        auto searcherQueryResource = dynamic_pointer_cast<SearcherQueryResource>(
                getQueryResource());

        FakeIndex fakeIndex;
        fakeIndex.versionId = versionId;
        fakeIndex.indexes["phrase"] = "with:0[1];1[1];2[2];3[1, 2];4[1];5[5];\n";
        fakeIndex.indexes["pk"] = "1:1,2:2,3:3,4:4,5:5,6:6;\n";
        fakeIndex.attributes = "int8 : int8_t : 1, 1, 1, 1, 1, 2;"
                               "type : int32_t : 1, 1, 1, 2, 2, 2;"
                               "price : int32_t : 1, 2, 6, 3, 4, 5;";
        searcherQueryResource->indexPartitionReaderWrapper = 
            FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    }

private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(turing, Ha3FillGlobalIdInfoOpTest);


TEST_F(Ha3FillGlobalIdInfoOpTest, testWithoutMatchDocs) {
    makeOp();
    proto::Range hashRange;
    hashRange.set_from(1024);
    hashRange.set_to(2047);
    FullIndexVersion fullIndexVersion = 123;
    initSearcherSessionResource(hashRange, fullIndexVersion);

    Variant variant1 = fakeHa3Request();
    Variant variant2 = fakeHa3Result();
    AddInput<Variant>(
            TensorShape({}),
            [&variant1](int x) -> Variant {
                return variant1;
            });

    AddInput<Variant>(
            TensorShape({}),
            [&variant2](int x) -> Variant {
                return variant2;
            });
    TF_ASSERT_OK(RunOpKernel());
    checkOutput(0);
}


TEST_F(Ha3FillGlobalIdInfoOpTest, testWithMatchDocsFlagAllTrue) {
    makeOp();
    proto::Range hashRange;

    uint32_t hashFrom = 1024;
    uint32_t hashTo = 2047;
    versionid_t indexVersionExpect = 10;
    hashRange.set_from(hashFrom);
    hashRange.set_to(hashTo);
    FullIndexVersion fullIndexVersionExpect = 123;
    initSearcherSessionResource(hashRange, fullIndexVersionExpect);
    initSearcherQueryResource(indexVersionExpect);

    uint8_t phaseOneInfoFlag = common::pob_indexversion_flag
                               | common::pob_fullversion_flag
                               | common::pob_ip_flag;

    Variant variant1 = fakeHa3Request(phaseOneInfoFlag);
    Variant variant2 = fakeHa3ResultWithMatchDocs();
    AddInput<Variant>(
            TensorShape({}),
            [&variant1](int x) -> Variant {
                return variant1;
            });

    AddInput<Variant>(
            TensorShape({}),
            [&variant2](int x) -> Variant {
                return variant2;
            });
    TF_ASSERT_OK(RunOpKernel());
    checkOutput(0);

    auto pOutputTensor = GetOutput(0);
    ASSERT_TRUE(pOutputTensor != nullptr);

    auto ha3ResultVariant = pOutputTensor->scalar<Variant>()().get<Ha3ResultVariant>();
    ResultPtr result = ha3ResultVariant->getResult();
    ASSERT_TRUE(result != nullptr);
    MatchDocs *matchDocs = result->getMatchDocs();
    ASSERT_TRUE(matchDocs != nullptr);
    ASSERT_EQ(2u, matchDocs->size());
    
    auto matchDocAllocator = matchDocs->getMatchDocAllocator();
    auto hashidRef = matchDocAllocator->findReference<hashid_t>(HASH_ID_REF);
    auto indexVersionRef = matchDocAllocator->findReference<versionid_t>(INDEXVERSION_REF);
    auto fullIndexVersionRef = matchDocAllocator->findReference<FullIndexVersion>(FULLVERSION_REF);
    auto ipRef = matchDocAllocator->findReference<uint32_t>(IP_REF);
    ASSERT_TRUE(hashidRef != nullptr);
    ASSERT_TRUE(indexVersionRef != nullptr);
    ASSERT_TRUE(fullIndexVersionRef != nullptr);
    ASSERT_TRUE(ipRef != nullptr);
    auto &matchdocVec = matchDocs->getMatchDocsVect();
    for (size_t i=0; i<matchdocVec.size(); i++) {
        MatchDoc doc = matchdocVec[i];
        auto hashId = hashidRef->get(doc);
        ASSERT_EQ(hashFrom, hashId);
        auto indexVersion = indexVersionRef->get(doc);
        ASSERT_EQ(indexVersionExpect, indexVersion);
        auto fullIndexVersion = fullIndexVersionRef->get(doc);
        ASSERT_EQ(fullIndexVersionExpect, fullIndexVersion);
        auto ip = ipRef->get(doc);
        ASSERT_EQ(util::NetFunction::encodeIp(TEST_IP), ip);
    }
}


TEST_F(Ha3FillGlobalIdInfoOpTest, testWithMatchDocsFlagDisableFullIndex) {
    makeOp();
    proto::Range hashRange;

    uint32_t hashFrom = 1024;
    uint32_t hashTo = 2047;
    versionid_t indexVersionExpect = 10;
    hashRange.set_from(hashFrom);
    hashRange.set_to(hashTo);
    FullIndexVersion fullIndexVersionExpect = 123;
    initSearcherSessionResource(hashRange, fullIndexVersionExpect);
    initSearcherQueryResource(indexVersionExpect);

    uint8_t phaseOneInfoFlag = common::pob_indexversion_flag
                               | common::pob_ip_flag;

    Variant variant1 = fakeHa3Request(phaseOneInfoFlag);
    Variant variant2 = fakeHa3ResultWithMatchDocs();
    AddInput<Variant>(
            TensorShape({}),
            [&variant1](int x) -> Variant {
                return variant1;
            });

    AddInput<Variant>(
            TensorShape({}),
            [&variant2](int x) -> Variant {
                return variant2;
            });
    TF_ASSERT_OK(RunOpKernel());
    checkOutput(0);

    auto pOutputTensor = GetOutput(0);
    ASSERT_TRUE(pOutputTensor != nullptr);

    auto ha3ResultVariant = pOutputTensor->scalar<Variant>()().get<Ha3ResultVariant>();
    ResultPtr result = ha3ResultVariant->getResult();
    ASSERT_TRUE(result != nullptr);
    MatchDocs *matchDocs = result->getMatchDocs();
    ASSERT_TRUE(matchDocs != nullptr);
    ASSERT_EQ(2u, matchDocs->size());
    
    auto matchDocAllocator = matchDocs->getMatchDocAllocator();
    auto hashidRef = matchDocAllocator->findReference<hashid_t>(HASH_ID_REF);
    auto indexVersionRef = matchDocAllocator->findReference<versionid_t>(INDEXVERSION_REF);
    auto fullIndexVersionRef = matchDocAllocator->findReference<FullIndexVersion>(FULLVERSION_REF);
    auto ipRef = matchDocAllocator->findReference<uint32_t>(IP_REF);
    ASSERT_TRUE(hashidRef != nullptr);
    ASSERT_TRUE(indexVersionRef != nullptr);
    ASSERT_TRUE(fullIndexVersionRef == nullptr);
    ASSERT_TRUE(ipRef != nullptr);
    auto &matchdocVec = matchDocs->getMatchDocsVect();
    for (size_t i=0; i<matchdocVec.size(); i++) {
        MatchDoc doc = matchdocVec[i];
        auto hashId = hashidRef->get(doc);
        ASSERT_EQ(hashFrom, hashId);
        auto indexVersion = indexVersionRef->get(doc);
        ASSERT_EQ(indexVersionExpect, indexVersion);
        auto ip = ipRef->get(doc);
        ASSERT_EQ(util::NetFunction::encodeIp(TEST_IP), ip);
    }
    
}

TEST_F(Ha3FillGlobalIdInfoOpTest, testWithMatchDocsFlagDisableIndexVersion) {
    makeOp();
    proto::Range hashRange;

    uint32_t hashFrom = 1024;
    uint32_t hashTo = 2047;
    versionid_t indexVersionExpect = 10;
    hashRange.set_from(hashFrom);
    hashRange.set_to(hashTo);
    FullIndexVersion fullIndexVersionExpect = 123;
    initSearcherSessionResource(hashRange, fullIndexVersionExpect);
    initSearcherQueryResource(indexVersionExpect);

    uint8_t phaseOneInfoFlag =   common::pob_fullversion_flag
                               | common::pob_ip_flag;

    Variant variant1 = fakeHa3Request(phaseOneInfoFlag);
    Variant variant2 = fakeHa3ResultWithMatchDocs();
    AddInput<Variant>(
            TensorShape({}),
            [&variant1](int x) -> Variant {
                return variant1;
            });

    AddInput<Variant>(
            TensorShape({}),
            [&variant2](int x) -> Variant {
                return variant2;
            });
    TF_ASSERT_OK(RunOpKernel());
    checkOutput(0);

    auto pOutputTensor = GetOutput(0);
    ASSERT_TRUE(pOutputTensor != nullptr);

    auto ha3ResultVariant = pOutputTensor->scalar<Variant>()().get<Ha3ResultVariant>();
    ResultPtr result = ha3ResultVariant->getResult();
    ASSERT_TRUE(result != nullptr);
    MatchDocs *matchDocs = result->getMatchDocs();
    ASSERT_TRUE(matchDocs != nullptr);
    ASSERT_EQ(2u, matchDocs->size());
    
    auto matchDocAllocator = matchDocs->getMatchDocAllocator();
    auto hashidRef = matchDocAllocator->findReference<hashid_t>(HASH_ID_REF);
    auto indexVersionRef = matchDocAllocator->findReference<versionid_t>(INDEXVERSION_REF);
    auto fullIndexVersionRef = matchDocAllocator->findReference<FullIndexVersion>(FULLVERSION_REF);
    auto ipRef = matchDocAllocator->findReference<uint32_t>(IP_REF);
    ASSERT_TRUE(hashidRef != nullptr);
    ASSERT_TRUE(indexVersionRef == nullptr);
    ASSERT_TRUE(fullIndexVersionRef != nullptr);
    ASSERT_TRUE(ipRef != nullptr);
    auto &matchdocVec = matchDocs->getMatchDocsVect();
    for (size_t i=0; i<matchdocVec.size(); i++) {
        MatchDoc doc = matchdocVec[i];
        auto hashId = hashidRef->get(doc);
        ASSERT_EQ(hashFrom, hashId);
        auto fullIndexVersion = fullIndexVersionRef->get(doc);
        ASSERT_EQ(fullIndexVersionExpect, fullIndexVersion);
        auto ip = ipRef->get(doc);
        ASSERT_EQ(util::NetFunction::encodeIp(TEST_IP), ip);
    }
}


TEST_F(Ha3FillGlobalIdInfoOpTest, testWithMatchDocsFlagDisableIp) {
    makeOp();
    proto::Range hashRange;

    uint32_t hashFrom = 1024;
    uint32_t hashTo = 2047;
    versionid_t indexVersionExpect = 10;
    hashRange.set_from(hashFrom);
    hashRange.set_to(hashTo);
    FullIndexVersion fullIndexVersionExpect = 123;
    initSearcherSessionResource(hashRange, fullIndexVersionExpect);
    initSearcherQueryResource(indexVersionExpect);

    uint8_t phaseOneInfoFlag = common::pob_indexversion_flag
                               | common::pob_fullversion_flag;


    Variant variant1 = fakeHa3Request(phaseOneInfoFlag);
    Variant variant2 = fakeHa3ResultWithMatchDocs();
    AddInput<Variant>(
            TensorShape({}),
            [&variant1](int x) -> Variant {
                return variant1;
            });

    AddInput<Variant>(
            TensorShape({}),
            [&variant2](int x) -> Variant {
                return variant2;
            });
    TF_ASSERT_OK(RunOpKernel());
    checkOutput(0);

    auto pOutputTensor = GetOutput(0);
    ASSERT_TRUE(pOutputTensor != nullptr);

    auto ha3ResultVariant = pOutputTensor->scalar<Variant>()().get<Ha3ResultVariant>();
    ResultPtr result = ha3ResultVariant->getResult();
    ASSERT_TRUE(result != nullptr);
    MatchDocs *matchDocs = result->getMatchDocs();
    ASSERT_TRUE(matchDocs != nullptr);
    ASSERT_EQ(2u, matchDocs->size());
    
    auto matchDocAllocator = matchDocs->getMatchDocAllocator();
    auto hashidRef = matchDocAllocator->findReference<hashid_t>(HASH_ID_REF);
    auto indexVersionRef = matchDocAllocator->findReference<versionid_t>(INDEXVERSION_REF);
    auto fullIndexVersionRef = matchDocAllocator->findReference<FullIndexVersion>(FULLVERSION_REF);
    auto ipRef = matchDocAllocator->findReference<uint32_t>(IP_REF);
    ASSERT_TRUE(hashidRef != nullptr);
    ASSERT_TRUE(indexVersionRef != nullptr);
    ASSERT_TRUE(fullIndexVersionRef != nullptr);
    ASSERT_TRUE(ipRef == nullptr);
    auto &matchdocVec = matchDocs->getMatchDocsVect();
    for (size_t i=0; i<matchdocVec.size(); i++) {
        MatchDoc doc = matchdocVec[i];
        auto hashId = hashidRef->get(doc);
        ASSERT_EQ(hashFrom, hashId);
        auto indexVersion = indexVersionRef->get(doc);
        ASSERT_EQ(indexVersionExpect, indexVersion);
        auto fullIndexVersion = fullIndexVersionRef->get(doc);
        ASSERT_EQ(fullIndexVersionExpect, fullIndexVersion);
    }
}

#undef TEST_IP

END_HA3_NAMESPACE();

