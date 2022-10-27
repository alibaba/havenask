#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include <ha3/common/LayerDescription.h>

using namespace std;
using namespace testing;

BEGIN_HA3_NAMESPACE(common);

class LayerDescriptionTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(common, LayerDescriptionTest);

void LayerDescriptionTest::setUp() {
}

void LayerDescriptionTest::tearDown() {
}

TEST_F(LayerDescriptionTest, testSerilizeAndDeserilize) {
    LayerDescription desc;
    ASSERT_EQ(desc._quotaMode, QM_PER_DOC);
    ASSERT_EQ(desc._quotaType, QT_PROPOTION);
    ASSERT_EQ(desc._quota, 0);
    autil::DataBuffer dataBuffer;
    desc.serialize(dataBuffer);
    LayerDescription  newdesc;
    newdesc._quota = 1;
    newdesc._quotaMode = QM_PER_LAYER;
    newdesc._quotaType = QT_AVERAGE;
    newdesc.deserialize(dataBuffer);
    ASSERT_EQ(newdesc._quotaMode, QM_PER_DOC);
    ASSERT_EQ(newdesc._quotaType, QT_PROPOTION);
    ASSERT_EQ(newdesc._quota, 0);

    desc._quota = 1;
    desc._quotaMode = QM_PER_LAYER;
    desc._quotaType = QT_AVERAGE;
    desc.serialize(dataBuffer);
    newdesc.deserialize(dataBuffer);
    ASSERT_EQ(newdesc._quotaMode, QM_PER_LAYER);
    ASSERT_EQ(newdesc._quotaType, QT_AVERAGE);
    ASSERT_EQ(newdesc._quota, 1);
}

END_HA3_NAMESPACE();

