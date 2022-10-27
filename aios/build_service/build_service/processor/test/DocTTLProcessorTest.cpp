#include "build_service/test/unittest.h"
#include "build_service/processor/DocTTLProcessor.h"

using namespace std;
using namespace testing;

namespace build_service {
namespace processor {

class DocTTLProcessorTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
};

void DocTTLProcessorTest::setUp() {
}

void DocTTLProcessorTest::tearDown() {
}

TEST_F(DocTTLProcessorTest, testProcessType) {
    DocTTLProcessorPtr processor(new DocTTLProcessor);
    DocumentProcessorPtr clonedProcessor(processor->clone());
    EXPECT_TRUE(clonedProcessor->needProcess(ADD_DOC));
    EXPECT_TRUE(clonedProcessor->needProcess(DELETE_DOC));
    EXPECT_TRUE(clonedProcessor->needProcess(DELETE_SUB_DOC));
    EXPECT_TRUE(clonedProcessor->needProcess(UPDATE_FIELD));
    EXPECT_TRUE(!clonedProcessor->needProcess(SKIP_DOC));

}


}
}
