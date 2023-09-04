#include "unittest/unittest.h"
#include "navi/common.h"
#include "navi/example/TestData.h"
#include "navi/tester/KernelTesterBuilder.h"
#include "navi/engine/AsyncPipe.h"
#include "navi/engine/GraphParam.h"
#include "navi/engine/LocalSubGraph.h"
#include "navi/engine/Node.h"

using namespace std;
using namespace testing;

namespace navi {

class AsyncPipeTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void AsyncPipeTest::setUp() {
}

void AsyncPipeTest::tearDown() {
}

TEST_F(AsyncPipeTest, testAsyncPipeSimple) {

}

}
