#include "build_service/test/unittest.h"
#include "build_service/builder/BuilderMetrics.h"

using namespace std;
using namespace testing;

namespace build_service {
namespace builder {

class BuilderMetricsTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
};

void BuilderMetricsTest::setUp() {
}

void BuilderMetricsTest::tearDown() {
}

TEST_F(BuilderMetricsTest, testReportMetrics) {
    BuilderMetrics builderMetrics;

#define CHECK_DOC_COUNT(add, update, del, delsub, total)        \
    do {                                                        \
        EXPECT_EQ(add, builderMetrics._addDocCount);            \
        EXPECT_EQ(update, builderMetrics._updateDocCount);      \
        EXPECT_EQ(del, builderMetrics._deleteDocCount);         \
        EXPECT_EQ(delsub, builderMetrics._deleteSubDocCount);   \
        EXPECT_EQ(total, builderMetrics._totalDocCount);        \
    } while (0)
    
    CHECK_DOC_COUNT(0u, 0u, 0u, 0u, 0u);

    builderMetrics.reportMetrics(false, ADD_DOC);
    EXPECT_EQ(1u, builderMetrics._addFailedDocCount);

    builderMetrics.reportMetrics(false, DELETE_SUB_DOC);
    EXPECT_EQ(1u, builderMetrics._deleteSubFailedDocCount);

    CHECK_DOC_COUNT(0u, 0u, 0u, 0u, 2u);

    builderMetrics.reportMetrics(true, ADD_DOC);
    CHECK_DOC_COUNT(1u, 0u, 0u, 0u, 3u);

    builderMetrics.reportMetrics(true, UPDATE_FIELD);
    CHECK_DOC_COUNT(1u, 1u, 0u, 0u, 4u);

    builderMetrics.reportMetrics(true, DELETE_DOC);
    CHECK_DOC_COUNT(1u, 1u, 1u, 0u, 5u);

    builderMetrics.reportMetrics(true, DELETE_SUB_DOC);
    CHECK_DOC_COUNT(1u, 1u, 1u, 1u, 6u);

}

}
}
