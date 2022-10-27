#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include <tensorflow/core/kernels/ops_testutil.h>
#include <tensorflow/core/framework/fake_input.h>
#include <tensorflow/core/framework/node_def_builder.h>
#include <tensorflow/core/framework/variant.h>
#include "ha3/turing/ops/test/Ha3OpTestBase.h"
#include <ha3/turing/ops/Ha3SeekParaSplitOp.h>
#include <ha3/turing/ops/SearcherQueryResource.h>
#include <ha3/common/test/ResultConstructor.h>
#include <ha3/turing/variant/AggregateResultsVariant.h>
#include <ha3_sdk/testlib/index/FakeIndexPartition.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReader.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReaderCreator.h>
#include "ha3/service/SearcherResource.h"
#include <ha3/common/QueryLayerClause.h>
#include <ha3/queryparser/ClauseParserContext.h>

using namespace std;
using namespace tensorflow;
using namespace ::testing;
using namespace suez::turing;
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(document);
USE_HA3_NAMESPACE(index);
USE_HA3_NAMESPACE(rank);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(search);
USE_HA3_NAMESPACE(service);
USE_HA3_NAMESPACE(queryparser);

BEGIN_HA3_NAMESPACE(turing);

class Ha3SeekParaSplitOpTest : public Ha3OpTestBase {
public:
    void setUp() override {
        // OpTestBase::SetUp();
    }
    void tearDown() override {
        // OpTestBase::TearDown();
    }
private:
    Ha3RequestVariant prepareRequestVariant(const string& layerStr = "");
    void prepareOp(const int32_t wayCount, Ha3SeekParaSplitOp *&outOp);
    void initSearcherQueryResource(const string &segmentDocCounts = "10000");
    void initSearcherSessionResource();
    QueryLayerClause* initQueryLayerClause(const string &layerStr);
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(ops, Ha3SeekParaSplitOpTest);

QueryLayerClause* Ha3SeekParaSplitOpTest::initQueryLayerClause(const string &layerStr) {
    ClauseParserContext ctx;
    if (!ctx.parseLayerClause(layerStr.c_str())) {
        HA3_LOG(ERROR, "msg is [%s]", layerStr.c_str());
        assert(false);
    }
    QueryLayerClause *layerClause = ctx.stealLayerClause();
    return layerClause;
}

Ha3RequestVariant Ha3SeekParaSplitOpTest::prepareRequestVariant(
        const string &layerStr)
{
    RequestPtr requestPtr(new Request);
    if (!layerStr.empty()) {
        QueryLayerClause *layerClause = initQueryLayerClause(layerStr);
        requestPtr->setQueryLayerClause(layerClause);
    }
    Ha3RequestVariant variant(requestPtr, &_pool);
    return variant;
}

void Ha3SeekParaSplitOpTest::prepareOp(const int32_t wayCount,
                                       Ha3SeekParaSplitOp *&outOp)
{
    TF_ASSERT_OK(
            NodeDefBuilder("myop", "Ha3SeekParaSplitOp")
            .Input(FakeInput(DT_VARIANT))
            .Attr("N", wayCount)
            .Finalize(node_def()));
    TF_ASSERT_OK(InitOp());
    outOp = dynamic_cast<Ha3SeekParaSplitOp *>(kernel_.get());
}

void Ha3SeekParaSplitOpTest::initSearcherQueryResource(
        const string &segmentDocCounts)
{
    auto searcherQueryResource = dynamic_pointer_cast<SearcherQueryResource>(
            getQueryResource());
    ASSERT_TRUE(searcherQueryResource != nullptr);
    FakeIndex fakeIndex;
    fakeIndex.versionId = 1;
    fakeIndex.segmentDocCounts = segmentDocCounts;
    IndexPartitionReaderWrapperPtr indexWraper =
        FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    searcherQueryResource->indexPartitionReaderWrapper = indexWraper;
}

void Ha3SeekParaSplitOpTest::initSearcherSessionResource() {
    auto searcherSessionResource =
        dynamic_pointer_cast<SearcherSessionResource>(_sessionResource);
    ASSERT_TRUE(searcherSessionResource != nullptr);
    SearcherResourcePtr searcherResource(new HA3_NS(service)::SearcherResource);
    searcherResource->setPartCount(1);
    searcherSessionResource->searcherResource = searcherResource;
}

#define GET_LAYER_METAS(index)                          \
        auto pOutLayerMetasTensor = GetOutput(index);   \
        ASSERT_TRUE(pOutLayerMetasTensor != nullptr);   \
        LayerMetasVariant *lyrVar = pOutLayerMetasTensor->scalar<Variant>()().get<LayerMetasVariant>(); \
        ASSERT_TRUE(lyrVar != nullptr);                  \
        LayerMetasPtr lyrMetas = lyrVar->getLayerMetas();\
        ASSERT_TRUE(lyrMetas != nullptr);

TEST_F(Ha3SeekParaSplitOpTest, testSimple) {
    Ha3SeekParaSplitOp *seekParaOp;
    prepareOp(2, seekParaOp);
    initSearcherQueryResource();
    initSearcherSessionResource();
    Variant variant = prepareRequestVariant();
    AddInputFromArray<Variant>(TensorShape({}), {variant});
    TF_ASSERT_OK(RunOpKernel());
    {
        GET_LAYER_METAS(0);
        string expectLyrStr("(quota: 4294967295 maxQuota: 4294967295 quotaMode: 0 "
                            "needAggregate: 1 quotaType: 0) begin: 0 end: 4999 "
                            "nextBegin: 0 quota: 0;");
        ASSERT_EQ(1, lyrMetas->size());
        ASSERT_EQ(expectLyrStr, (*lyrMetas)[0].toString());
    }
    {
        GET_LAYER_METAS(1);
        string expectLyrStr("(quota: 4294967295 maxQuota: 4294967295 quotaMode: 0 "
                            "needAggregate: 1 quotaType: 0) begin: 5000 end: 9999 "
                            "nextBegin: 5000 quota: 0;");
        ASSERT_EQ(1, lyrMetas->size());
        ASSERT_EQ(expectLyrStr, (*lyrMetas)[0].toString());
    }
}

TEST_F(Ha3SeekParaSplitOpTest, testHasLayerClause) {
    Ha3SeekParaSplitOp *seekParaOp;
    prepareOp(2, seekParaOp);
    initSearcherQueryResource("100,200");
    initSearcherSessionResource();
    Variant variant = prepareRequestVariant("quota:3000;quota:3000");
    AddInputFromArray<Variant>(TensorShape({}), {variant});
    TF_ASSERT_OK(RunOpKernel());
    {
        GET_LAYER_METAS(0);
        string expectLyrStr("(quota: 3000 maxQuota: 4294967295 "
                             "quotaMode: 0 needAggregate: 1 quotaType: 0) "
                             "begin: 0 end: 99 nextBegin: 0 quota: 0;"
                             "begin: 100 end: 149 nextBegin: 100 quota: 0;");
        ASSERT_EQ(2, lyrMetas->size());
        ASSERT_EQ(expectLyrStr, (*lyrMetas)[0].toString());
        ASSERT_EQ(expectLyrStr, (*lyrMetas)[1].toString());
    }
    {
        GET_LAYER_METAS(1);
        string expectLyrStr("(quota: 3000 maxQuota: 4294967295 "
                             "quotaMode: 0 needAggregate: 1 quotaType: 0) "
                             "begin: 150 end: 299 nextBegin: 150 quota: 0;");
        for (size_t idx=0; idx<lyrMetas->size(); ++idx) {
            cout << (*lyrMetas)[idx].toString() << endl;
        }
        ASSERT_EQ(2, lyrMetas->size());
        ASSERT_EQ(expectLyrStr, (*lyrMetas)[0].toString());
        ASSERT_EQ(expectLyrStr, (*lyrMetas)[1].toString());
    }
}

TEST_F(Ha3SeekParaSplitOpTest, testExceptionInvalidInput) {
    Ha3SeekParaSplitOp *seekParaOp;
    prepareOp(2, seekParaOp);
    initSearcherQueryResource("100,200");
    initSearcherSessionResource();
    LayerMetasVariant variant; //error variant
    AddInputFromArray<Variant>(TensorShape({}), {variant});
    auto status = RunOpKernel();
    ASSERT_TRUE(::tensorflow::Status::OK() != status);
}

TEST_F(Ha3SeekParaSplitOpTest, testExceptionInvalidResource) {
    Ha3SeekParaSplitOp *seekParaOp;
    prepareOp(2, seekParaOp);
    clearQueryResource();
    Variant variant = prepareRequestVariant("quota:3000;quota:3000");
    AddInputFromArray<Variant>(TensorShape({}), {variant});
    auto status = RunOpKernel();
    ASSERT_TRUE(::tensorflow::Status::OK() != status);
}

END_HA3_NAMESPACE();
