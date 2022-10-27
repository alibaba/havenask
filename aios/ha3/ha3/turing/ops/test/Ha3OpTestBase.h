#ifndef ISEARCH_OP_TEST_BASE_H
#define ISEARCH_OP_TEST_BASE_H
#include <basic_ops/ops/test/OpTestBase.h>
#include <ha3/turing/ops/SearcherQueryResource.h>
#include <ha3/turing/ops/SearcherSessionResource.h>
#include <unittest/unittest.h>
#include <ha3/test/test.h>

BEGIN_HA3_NAMESPACE(turing);
class Ha3OpTestBase : public suez::turing::OpTestBase {
public:
    virtual ~Ha3OpTestBase() {
	tensorflow::gtl::STLDeleteElements(&tensors_);
    }
    virtual tensorflow::QueryResourcePtr createQueryResource() override {
        SearcherQueryResource *searcherQueryResource = new SearcherQueryResource();
        HA3_NS(monitor)::SessionMetricsCollectorPtr sessionMetricsCollectorPtr(new HA3_NS(monitor)::SessionMetricsCollector());
        searcherQueryResource->sessionMetricsCollector = sessionMetricsCollectorPtr.get();
        tensorflow::QueryResourcePtr queryResource(searcherQueryResource);
        queryResource->setBasicMetricsCollector(sessionMetricsCollectorPtr);
        return queryResource;
    }

    virtual tensorflow::SessionResourcePtr createSessionResource(uint32_t count) override {
        SearcherSessionResource *searcherSessionResource = new SearcherSessionResource(count);
        tensorflow::SessionResourcePtr sessionResource(searcherSessionResource);
        return sessionResource;
    }

    template <typename T>
    T checkOutputWithExpectedValue(uint32_t expectPort, T expectedValue) {
        auto pOutputTensor = GetOutput(expectPort);
        EXPECT_TRUE(pOutputTensor != nullptr);

        auto basicValue = pOutputTensor->scalar<T>()();
        EXPECT_EQ(expectedValue, basicValue);

        return basicValue;
    }

};

END_HA3_NAMESPACE(turing);

#endif //ISEARCH_OP_TEST_BASE_H
