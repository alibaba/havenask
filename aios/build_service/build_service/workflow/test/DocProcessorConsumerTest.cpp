#include "build_service/test/unittest.h"
#include "build_service/workflow/DocProcessorConsumer.h"

using namespace std;

namespace build_service {
namespace workflow {

class DocProcessorConsumerTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
};

void DocProcessorConsumerTest::setUp() {
}

void DocProcessorConsumerTest::tearDown() {
}

TEST_F(DocProcessorConsumerTest, testGetLocator) {
    DocProcessorConsumer consumer(NULL);
    common::Locator locator;
    EXPECT_TRUE(consumer.getLocator(locator));
    EXPECT_EQ(common::INVALID_LOCATOR, locator);
}


}
}
