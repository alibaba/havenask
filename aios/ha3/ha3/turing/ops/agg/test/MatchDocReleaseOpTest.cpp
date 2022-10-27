#include <ha3/common.h>
#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include <tensorflow/core/kernels/ops_testutil.h>
#include <tensorflow/core/framework/fake_input.h>
#include <tensorflow/core/framework/node_def_builder.h>
#include <tensorflow/core/framework/variant.h>
#include <basic_ops/ops/test/OpTestBase.h>
#include <ha3/common/ha3_op_common.h>

using namespace std;
using namespace tensorflow;
using namespace suez::turing;
using namespace matchdoc;

BEGIN_HA3_NAMESPACE(turing);


class MatchDocReleaseOpTest : public OpTestBase {
public:
    void makeOp() {
        TF_ASSERT_OK(
                NodeDefBuilder("myMatchDocReleaseOp", "MatchDocReleaseOp")
                .Input(FakeInput(DT_BOOL))
                .Input(FakeInput(DT_VARIANT))
                .Finalize(node_def()));
        TF_ASSERT_OK(InitOp());
    }

    void checkOutput() {
        auto out0 = GetOutput(0);
        ASSERT_TRUE(out0 != nullptr);
        ASSERT_TRUE(out0->scalar<bool>()());
    }

private:
    MatchDocsVariant prepareInput( ) {
        MatchDocAllocatorPtr allocator(new MatchDocAllocator(&_pool));
        MatchDocsVariant variant(allocator, &_pool);
        variant.allocate(1);
        return variant;
    }

};

TEST_F(MatchDocReleaseOpTest, testRelaseMatchDocs) {
    makeOp();
    MatchDocsVariant variant = prepareInput();
    AddInputFromArray<bool>(TensorShape({}), {true});
    AddInputFromArray<Variant>(TensorShape({}), {variant});
    TF_ASSERT_OK(RunOpKernel());
    checkOutput();
}


END_HA3_NAMESPACE();
