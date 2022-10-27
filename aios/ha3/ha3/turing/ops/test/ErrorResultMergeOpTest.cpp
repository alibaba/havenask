#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include <tensorflow/core/kernels/ops_testutil.h>
#include <tensorflow/core/framework/fake_input.h>
#include <tensorflow/core/framework/node_def_builder.h>
#include <tensorflow/core/framework/variant.h>
#include <ha3/common/Request.h>
#include <ha3/turing/variant/Ha3RequestVariant.h>
#include <ha3/turing/ops/test/Ha3OpTestBase.h>
#include <basic_variant/variant/KvPairVariant.h>


using namespace std;
using namespace testing;
using namespace tensorflow;
using namespace suez::turing;
using namespace ::testing;
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(search);

BEGIN_HA3_NAMESPACE(turing);

class ErrorResultMergeOpTest : public Ha3OpTestBase {
private:
    void prepareOp() {
        TF_ASSERT_OK(
                NodeDefBuilder("myop", "ErrorResultMergeOp")
                .Input(FakeInput(DT_STRING))
                .Attr("error_code", 30030)
                .Finalize(node_def()));
        TF_ASSERT_OK(InitOp());
    }

    void prepareDefaultAttrOp() {
        TF_ASSERT_OK(
                NodeDefBuilder("myop", "ErrorResultMergeOp")
                .Input(FakeInput(DT_STRING))
                .Finalize(node_def()));
        TF_ASSERT_OK(InitOp());
    }

    void prepareSimpleInput(const std::string &warnInfo) {
        AddInputFromArray<string>(TensorShape({}), {warnInfo});
    }

    void prepareCommonResource() {
        auto searcherQueryResource = dynamic_pointer_cast<SearcherQueryResource>(
                getQueryResource());
        _commonResource.reset(new SearchCommonResource(&_pool, TableInfoPtr(),
                        nullptr, nullptr, nullptr, nullptr,
                        CavaPluginManagerPtr(), nullptr, nullptr, {}));
        searcherQueryResource->commonResource = _commonResource;
    }
private:
    autil::mem_pool::Pool _pool;
    SearchCommonResourcePtr _commonResource;

private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(ops, ErrorResultMergeOpTest);

TEST_F(ErrorResultMergeOpTest, testSimple) {
    ASSERT_NO_FATAL_FAILURE(prepareOp());
    std::string warnInfo("scorer warn");
    ASSERT_NO_FATAL_FAILURE(prepareSimpleInput(warnInfo));
    ASSERT_NO_FATAL_FAILURE(prepareCommonResource());
    ASSERT_EQ(0, _commonResource->errorResult->getErrorCount());
    TF_ASSERT_OK(RunOpKernel());

    ASSERT_EQ(1, _commonResource->errorResult->getErrorCount());
    auto &errorResults = _commonResource->errorResult->getErrorResults();
    ASSERT_EQ(ERROR_OTHER_SCORER_WARNING, errorResults[0].getErrorCode());
    ASSERT_EQ(warnInfo, errorResults[0].getErrorMsg());
}

TEST_F(ErrorResultMergeOpTest, testInputEmpty) {
    ASSERT_NO_FATAL_FAILURE(prepareOp());
    std::string warnInfo("");
    ASSERT_NO_FATAL_FAILURE(prepareSimpleInput(warnInfo));
    ASSERT_NO_FATAL_FAILURE(prepareCommonResource());
    ASSERT_EQ(0, _commonResource->errorResult->getErrorCount());
    TF_ASSERT_OK(RunOpKernel());

    ASSERT_EQ(0, _commonResource->errorResult->getErrorCount());
}

TEST_F(ErrorResultMergeOpTest, testNoAttr) {
    ASSERT_NO_FATAL_FAILURE(prepareDefaultAttrOp());
    std::string warnInfo("scorer warn");
    ASSERT_NO_FATAL_FAILURE(prepareSimpleInput(warnInfo));
    ASSERT_NO_FATAL_FAILURE(prepareCommonResource());
    ASSERT_EQ(0, _commonResource->errorResult->getErrorCount());
    TF_ASSERT_OK(RunOpKernel());

    ASSERT_EQ(1, _commonResource->errorResult->getErrorCount());
    auto &errorResults = _commonResource->errorResult->getErrorResults();
    ASSERT_EQ(ERROR_UNKNOWN, errorResults[0].getErrorCode());
    ASSERT_EQ(warnInfo, errorResults[0].getErrorMsg());
}

END_HA3_NAMESPACE();
