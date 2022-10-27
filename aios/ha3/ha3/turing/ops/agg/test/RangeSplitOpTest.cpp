#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include <tensorflow/core/kernels/ops_testutil.h>
#include <tensorflow/core/framework/fake_input.h>
#include <tensorflow/core/framework/node_def_builder.h>
#include <tensorflow/core/framework/variant.h>
#include <ha3/common/Request.h>
#include <ha3/common/Result.h>
#include <ha3/turing/variant/LayerMetasVariant.h>
#include <ha3/search/LayerMetas.h>
#include <basic_ops/ops/test/OpTestBase.h>
#include <ha3/turing/ops/agg/RangeSplitOp.h>

using namespace std;
using namespace tensorflow;
using namespace suez::turing;
using namespace matchdoc;

USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(search);

BEGIN_HA3_NAMESPACE(turing);


class RangeSplitOpTest : public OpTestBase {
public:
    void SetUp() override {
        OpTestBase::SetUp();
    }
    void TearDown() override {
        OpTestBase::TearDown();
        gtl::STLDeleteElements(&tensors_); // clear output for using pool
    }
public:
    void makeOp(int count = 1) {
        TF_ASSERT_OK(
                NodeDefBuilder("myRangeSplitOp", "RangeSplitOp")
                .Input(FakeInput(DT_VARIANT))
                .Attr("N", count)
                .Finalize(node_def()));
        TF_ASSERT_OK(InitOp());
    }

    void checkOutput(size_t pathNum, vector< vector<pair<int32_t, int32_t>>> &splitRanges) {
        ASSERT_TRUE(pathNum >= splitRanges.size());
        for (size_t idx = 0; idx < pathNum; idx++){
            auto pOutputTensor = GetOutput(idx);
            ASSERT_TRUE(pOutputTensor != nullptr);
            auto layerMetasVariant = pOutputTensor->scalar<Variant>()().get<LayerMetasVariant>();
            ASSERT_TRUE(layerMetasVariant != nullptr);
            auto layerMetas = layerMetasVariant->getLayerMetas();
            if (idx < splitRanges.size()) {
                ASSERT_TRUE(layerMetas != nullptr);
                LayerMeta &layerMeta = (*layerMetas)[0];
                ASSERT_EQ(splitRanges[idx].size(), layerMeta.size());
                for (size_t range_id = 0; range_id < layerMeta.size(); range_id++) {
                    ASSERT_EQ(splitRanges[idx][range_id].first, layerMeta[range_id].begin);
                    ASSERT_EQ(splitRanges[idx][range_id].second, layerMeta[range_id].end);                           }
            } else {
                ASSERT_TRUE(layerMetas == nullptr);
            }
        }
    }

    void checkLayerMeta(vector<pair<int32_t, int32_t>> expectRanges, LayerMeta &layerMeta) {
        ASSERT_EQ(expectRanges.size(), layerMeta.size());
        for (size_t range_id = 0; range_id < layerMeta.size(); range_id++) {
            ASSERT_EQ(expectRanges[range_id].first, layerMeta[range_id].begin);
            ASSERT_EQ(expectRanges[range_id].second, layerMeta[range_id].end);
        }
    }
    void checkLayerMeta(vector<pair<int32_t, int32_t>> expectRanges, LayerMetasPtr layerMetas) {
        if (expectRanges.size() == 0) {
            ASSERT_TRUE(layerMetas == nullptr);
        } else {
            ASSERT_TRUE(layerMetas != nullptr);
            ASSERT_EQ(1, layerMetas->size());
            LayerMeta &layerMeta = (*layerMetas)[0];
            ASSERT_EQ(expectRanges.size(), layerMeta.size());
            for (size_t range_id = 0; range_id < layerMeta.size(); range_id++) {
                ASSERT_EQ(expectRanges[range_id].first, layerMeta[range_id].begin);
                ASSERT_EQ(expectRanges[range_id].second, layerMeta[range_id].end);
            }
        }
    }
private:
    LayerMetasVariant prepareInput(vector<pair<int32_t, int32_t> > &ranges ) {
        LayerMeta layerMeta(&_pool);
        for (size_t idx = 0; idx < ranges.size(); idx++) {
            layerMeta.push_back(DocIdRange(ranges[idx].first, ranges[idx].second));
        }
        LayerMetasPtr layerMetas(new LayerMetas(&_pool));
        layerMetas->push_back(layerMeta);
        return LayerMetasVariant(layerMetas);
    }

    LayerMetasVariant prepareInput(vector<vector<pair<int32_t, int32_t> > > &rangesVec ) {
        LayerMetasPtr layerMetas(new LayerMetas(&_pool));
        for (size_t i = 0; i < rangesVec.size(); i++) {
            auto &ranges = rangesVec[i];
            LayerMeta layerMeta(&_pool);
            for (size_t idx = 0; idx < ranges.size(); idx++) {
                layerMeta.push_back(DocIdRange(ranges[idx].first, ranges[idx].second));
            }
            layerMetas->push_back(layerMeta);
        }
        return LayerMetasVariant(layerMetas);
    }

};


TEST_F(RangeSplitOpTest, testOnePath) {
    makeOp(1);
    vector<pair<int32_t, int32_t> > ranges = {{0, 1}, {2, 3}, {5, 8}};
    LayerMetasVariant variant = prepareInput(ranges);
    AddInputFromArray<Variant>(TensorShape({}), {variant});
    TF_ASSERT_OK(RunOpKernel());
    vector< vector<pair<int32_t, int32_t>>> output_ranges;
    output_ranges.push_back(ranges);
    checkOutput(1, output_ranges);
}

TEST_F(RangeSplitOpTest, testTwoPath) {
    makeOp(2);
    vector<pair<int32_t, int32_t> > ranges = {{0, 1}, {2, 3}, {5, 8}};
    LayerMetasVariant variant = prepareInput(ranges);
    AddInputFromArray<Variant>(TensorShape({}), {variant});
    TF_ASSERT_OK(RunOpKernel());
    vector< vector<pair<int32_t, int32_t>>> output_ranges = {{{0, 1}, {2, 3}}, {{5, 8}}};
    checkOutput(2, output_ranges);
}

TEST_F(RangeSplitOpTest, testTwoPath_across_range) {
    makeOp(2);
    vector<pair<int32_t, int32_t> > ranges = {{0, 1}, {2, 3}, {5, 9}};
    LayerMetasVariant variant = prepareInput(ranges);
    AddInputFromArray<Variant>(TensorShape({}), {variant});
    TF_ASSERT_OK(RunOpKernel());
    vector< vector<pair<int32_t, int32_t>>> output_ranges = {{{0, 1}, {2, 3}, {5, 5}}, {{6, 9}}};
    checkOutput(2, output_ranges);
}

TEST_F(RangeSplitOpTest, testTwoPath_empty_range) {
    makeOp(2);
    vector<pair<int32_t, int32_t> > ranges = {{0, 0}};
    LayerMetasVariant variant = prepareInput(ranges);
    AddInputFromArray<Variant>(TensorShape({}), {variant});
    TF_ASSERT_OK(RunOpKernel());
    vector< vector<pair<int32_t, int32_t>>> output_ranges = {{{0, 0}}};
    checkOutput(2, output_ranges);
}

TEST_F(RangeSplitOpTest, testFivePath) {
    makeOp(5);
    vector<pair<int32_t, int32_t> > ranges = {{0, 1}, {2, 3}, {5, 9}};
    LayerMetasVariant variant = prepareInput(ranges);
    AddInputFromArray<Variant>(TensorShape({}), {variant});
    TF_ASSERT_OK(RunOpKernel());
    vector< vector<pair<int32_t, int32_t>>> output_ranges = {{{0, 1}}, {{2, 3}}, {{5, 6}}, {{7, 8}}, {{9, 9}}};
    checkOutput(5, output_ranges);
}

TEST_F(RangeSplitOpTest, testFivePathWithTwoLayer) {
    makeOp(5);
    vector<vector<pair<int32_t, int32_t> > > ranges = {{{0, 1}, {2, 3}}, {{5, 9}}};
    LayerMetasVariant variant = prepareInput(ranges);
    AddInputFromArray<Variant>(TensorShape({}), {variant});
    TF_ASSERT_OK(RunOpKernel());
    vector< vector<pair<int32_t, int32_t>>> output_ranges = {{{0, 1}}, {{2, 3}}, {{5, 6}}, {{7, 8}}, {{9, 9}}};
    ASSERT_NO_FATAL_FAILURE(checkOutput(5, output_ranges));
}

TEST_F(RangeSplitOpTest, testFivePathWithTwoLayerDupRange) {
    makeOp(5);
    vector<vector<pair<int32_t, int32_t> > > ranges = {{{0, 2}, {1, 3}}, {{5, 8},{8, 9}}};
    LayerMetasVariant variant = prepareInput(ranges);
    AddInputFromArray<Variant>(TensorShape({}), {variant});
    TF_ASSERT_OK(RunOpKernel());
    vector< vector<pair<int32_t, int32_t>>> output_ranges = {{{0, 1}}, {{2, 2},{3, 3}}, {{5, 6}}, {{7, 8}}, {{9, 9}}};
    ASSERT_NO_FATAL_FAILURE(checkOutput(5, output_ranges));
}

TEST_F(RangeSplitOpTest, testFivePath_empty_range) {
    makeOp(5);
    vector<pair<int32_t, int32_t> > ranges = {{0, 1}, {2, 2}};
    LayerMetasVariant variant = prepareInput(ranges);
    AddInputFromArray<Variant>(TensorShape({}), {variant});
    TF_ASSERT_OK(RunOpKernel());
    vector< vector<pair<int32_t, int32_t>>> output_ranges = {{{0, 0}}, {{1, 1}}, {{2, 2}}};
    checkOutput(5, output_ranges);
}

TEST_F(RangeSplitOpTest, testPath_illeagal_range) {
    makeOp(1);
    vector<pair<int32_t, int32_t> > ranges = {{2, 1}};
    LayerMetasVariant variant = prepareInput(ranges);
    AddInputFromArray<Variant>(TensorShape({}), {variant});
    TF_ASSERT_OK(RunOpKernel());
    vector< vector<pair<int32_t, int32_t>>> output_ranges = {};
    checkOutput(1, output_ranges);
}

TEST_F(RangeSplitOpTest, testPath_has_illeagal_range) {
    makeOp(1);
    vector<pair<int32_t, int32_t> > ranges = {{2, 1}, {4, 5}};
    LayerMetasVariant variant = prepareInput(ranges);
    AddInputFromArray<Variant>(TensorShape({}), {variant});
    TF_ASSERT_OK(RunOpKernel());
    vector< vector<pair<int32_t, int32_t>>> output_ranges = {{{4, 5}}};
    checkOutput(1, output_ranges);
}

TEST_F(RangeSplitOpTest, testSplitLayerMeta) {
    LayerMetasPtr layerMetas;
    {
       deque<pair<int32_t, int32_t> > ranges = {};
       layerMetas = RangeSplitOp::splitLayerMeta(ranges, 2, &_pool);
       checkLayerMeta({}, layerMetas);
    }
    {
       deque<pair<int32_t, int32_t> > ranges = {{1, 2}, {4, 5}};
       layerMetas = RangeSplitOp::splitLayerMeta(ranges, 2, &_pool);
       checkLayerMeta({{1,2}}, layerMetas);
       layerMetas = RangeSplitOp::splitLayerMeta(ranges, 2, &_pool);
       checkLayerMeta({{4, 5}}, layerMetas);
    }
    {
       deque<pair<int32_t, int32_t> > ranges = {{1, 2}, {4, 5}};
       layerMetas = RangeSplitOp::splitLayerMeta(ranges, 3, &_pool);
       checkLayerMeta({{1,2}, {4, 4}}, layerMetas);
       layerMetas = RangeSplitOp::splitLayerMeta(ranges, 3, &_pool);
       checkLayerMeta({{5, 5}}, layerMetas);
       layerMetas = RangeSplitOp::splitLayerMeta(ranges, 3, &_pool);
       checkLayerMeta({}, layerMetas);
    }
    {
       deque<pair<int32_t, int32_t> > ranges = {{1, 2}, {4, 5}};
       layerMetas = RangeSplitOp::splitLayerMeta(ranges, 4, &_pool);
       checkLayerMeta({{1,2}, {4, 5}}, layerMetas);
       layerMetas = RangeSplitOp::splitLayerMeta(ranges, 4, &_pool);
       checkLayerMeta({}, layerMetas);
    }

}

TEST_F(RangeSplitOpTest, testMergeLayerMeta) {
    {
        LayerMeta layerMeta(&_pool);
        ASSERT_FALSE(RangeSplitOp::mergeLayerMeta(LayerMetasPtr(), layerMeta));
    }
    {
        LayerMetasPtr layerMetas(new LayerMetas(&_pool));
        LayerMeta layerMeta(&_pool);
        ASSERT_FALSE(RangeSplitOp::mergeLayerMeta(layerMetas, layerMeta));
    }
    {
        vector<pair<int32_t, int32_t> > ranges = {{1, 2}, {4, 5}};
        LayerMetasVariant variant = prepareInput(ranges);
        LayerMetasPtr layerMetas = variant.getLayerMetas();
        LayerMeta layerMeta(&_pool);
        ASSERT_TRUE(RangeSplitOp::mergeLayerMeta(layerMetas, layerMeta));
        checkLayerMeta(ranges, layerMeta);
    }
    {
        vector<pair<int32_t, int32_t> > ranges = {{4, 5}, {1, 2}};
        LayerMetasVariant variant = prepareInput(ranges);
        LayerMetasPtr layerMetas = variant.getLayerMetas();
        LayerMeta layerMeta(&_pool);
        ASSERT_TRUE(RangeSplitOp::mergeLayerMeta(layerMetas, layerMeta));
        checkLayerMeta({{1, 2}, {4, 5}}, layerMeta);
    }
    {
        vector<pair<int32_t, int32_t> > ranges = {{1, 3}, {1, 5}, {2, 4}, {5,5}};
        LayerMetasVariant variant = prepareInput(ranges);
        LayerMetasPtr layerMetas = variant.getLayerMetas();
        LayerMeta layerMeta(&_pool);
        ASSERT_TRUE(RangeSplitOp::mergeLayerMeta(layerMetas, layerMeta));
        checkLayerMeta({{1, 3}, {4, 5}, {6, 4}, {6, 5}}, layerMeta);
    }
    {
        vector<vector<pair<int32_t, int32_t> > > ranges = {{{4, 5}}, {{1, 2}}};
        LayerMetasVariant variant = prepareInput(ranges);
        LayerMetasPtr layerMetas = variant.getLayerMetas();
        LayerMeta layerMeta(&_pool);
        ASSERT_TRUE(RangeSplitOp::mergeLayerMeta(layerMetas, layerMeta));
        checkLayerMeta({{1, 2}, {4, 5}}, layerMeta);
    }
    {
        vector<vector<pair<int32_t, int32_t> > > ranges = {{{1, 3}, {1, 5}}, {{2, 4}, {5,5}}};
        LayerMetasVariant variant = prepareInput(ranges);
        LayerMetasPtr layerMetas = variant.getLayerMetas();
        LayerMeta layerMeta(&_pool);
        ASSERT_TRUE(RangeSplitOp::mergeLayerMeta(layerMetas, layerMeta));
        checkLayerMeta({{1, 3}, {4, 5}, {6, 4}, {6, 5}}, layerMeta);
    }


}


END_HA3_NAMESPACE();
