#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/DataBuffer.h>

using namespace std;
using namespace autil;

BEGIN_HA3_NAMESPACE(util);

class DataBufferTest : public TESTBASE {
public:
    DataBufferTest();
    ~DataBufferTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(util, DataBufferTest);


DataBufferTest::DataBufferTest() {
}

DataBufferTest::~DataBufferTest() {
}

void DataBufferTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");
}

void DataBufferTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");
}


END_HA3_NAMESPACE(util);
