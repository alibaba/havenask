#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include <tensorflow/core/kernels/ops_testutil.h>
#include <tensorflow/core/framework/fake_input.h>
#include <tensorflow/core/framework/node_def_builder.h>
#include <tensorflow/core/framework/variant.h>
#include <ha3/common/Request.h>
#include <ha3/common/Result.h>
#include <ha3/turing/variant/Ha3RequestVariant.h>
#include <ha3/turing/variant/LayerMetasVariant.h>
#include <ha3/search/LayerMetas.h>
#include <basic_ops/ops/test/OpTestBase.h>

using namespace std;
using namespace tensorflow;
using namespace suez::turing;
using namespace matchdoc;

USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(search);

BEGIN_HA3_NAMESPACE(turing);


class LayerMetasCreateOpTest : public OpTestBase {
public:
    void SetUp() override {
        _needBuildIndex = true;
        OpTestBase::SetUp();        
    }

    void prepareUserIndex() override {
        prepareInvertedTable();
    }

public:
    
    void makeOp() {
        TF_ASSERT_OK(
                NodeDefBuilder("myLayerMetasCreateOp", "LayerMetasCreateOp")
                .Input(FakeInput(DT_VARIANT))
                .Finalize(node_def()));
        TF_ASSERT_OK(InitOp());
    }

    void checkOutput(int from, int to) {
        auto pOutputTensor = GetOutput(0);
        ASSERT_TRUE(pOutputTensor != nullptr);

        auto layerMetasVariant = pOutputTensor->scalar<Variant>()().get<LayerMetasVariant>();
        ASSERT_TRUE(layerMetasVariant != nullptr);        
        auto layerMetas = layerMetasVariant->getLayerMetas();
        ASSERT_TRUE(layerMetas != nullptr);
        ASSERT_EQ(1, layerMetas->size());
        
        LayerMeta layerMeta = (*layerMetas)[0];
        ASSERT_EQ(1, layerMeta.size());
        
        ASSERT_EQ(from, layerMeta[0].begin);
        ASSERT_EQ(to, layerMeta[0].end);
    }

private:
    Ha3RequestVariant prepareRequestVariant(bool hasLayerClause) {
        RequestPtr request(new Request(&_pool));
        if (hasLayerClause) {
            QueryLayerClause *layerClause = new QueryLayerClause();
            LayerDescription *layerDesc = new LayerDescription();
            LayerKeyRange *layerRange = new LayerKeyRange();
            layerRange->attrName = LAYERKEY_DOCID;
            layerRange->ranges.push_back(LayerAttrRange("1", "3"));
            layerDesc->addLayerRange(layerRange);
            layerClause->addLayerDescription(layerDesc);
            request->setQueryLayerClause(layerClause);
        }
        return Ha3RequestVariant(request, &_pool);
    }

private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(turing, LayerMetasCreateOpTest);

TEST_F(LayerMetasCreateOpTest, testDefaultRequest) {
    makeOp();
    Ha3RequestVariant variant = prepareRequestVariant(false);
    AddInputFromArray<Variant>(TensorShape({}), {variant});
    TF_ASSERT_OK(RunOpKernel());
    checkOutput(0,3);
}

TEST_F(LayerMetasCreateOpTest, testWithLayerClause) {
    makeOp();
    Ha3RequestVariant variant = prepareRequestVariant(true);
    AddInputFromArray<Variant>(TensorShape({}), {variant});
    TF_ASSERT_OK(RunOpKernel());
    checkOutput(1,2);
}

END_HA3_NAMESPACE();

