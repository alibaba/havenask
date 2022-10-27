#include <ha3_sdk/testlib/index/FakeIndexPartitionReader.h>
#include <indexlib/index/normal/attribute/accessor/attribute_iterator_typed.h>
#include <unittest/unittest.h>
#include <ha3/util/Log.h>

using namespace std::tr1;

IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(partition);

IE_NAMESPACE_BEGIN(index);

class FakeIndexPartitionReaderTest : public TESTBASE {
public:
    void setUp() {}
    void tearDown() {}
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(index, FakeIndexPartitionReaderTest);

TEST_F(FakeIndexPartitionReaderTest, testSimpleProcess) {
    HA3_LOG(DEBUG, "Begin Test!");
    FakeIndexPartitionReader fakeIndexPartitionReader;
    fakeIndexPartitionReader.createFakeAttributeReader<int32_t>("price",
            "0, -1, -2, -3, -4, -5");

    AttributeReaderPtr attributeReader =
        fakeIndexPartitionReader.GetAttributeReader("price");
    ASSERT_EQ(AT_INT32, attributeReader->GetType());
}

IE_NAMESPACE_END(index);
