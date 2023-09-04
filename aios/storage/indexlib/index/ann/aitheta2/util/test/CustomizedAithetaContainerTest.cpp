#include "indexlib/index/ann/aitheta2/util/CustomizedAithetaContainer.h"

#include "indexlib/util/testutil/unittest.h"

using namespace indexlib::file_system;
using namespace std;
using namespace aitheta2;

namespace indexlibv2::index::ann {

class MockContainer : public CustomizedAiThetaContainer
{
public:
    MOCK_METHOD(IndexContainer::Segment::Pointer, get, (const std::string& id), (const, override));
};

class CustomizedAiThetaContainerTest : public TESTBASE
{
public:
    CustomizedAiThetaContainerTest() = default;
    ~CustomizedAiThetaContainerTest() = default;

public:
    void TestFetch();

private:
    AUTIL_LOG_DECLARE();
};

TEST_F(CustomizedAiThetaContainerTest, TestFetch)
{
    size_t data_size = 12;
    size_t padding_size = 4;
    uint32_t data_crc = 1;
    size_t region_size = data_size + padding_size;
    shared_ptr<uint8_t> data(new (std::nothrow) uint8_t[region_size], [](uint8_t* p) { delete[] p; });
    for (size_t i = 0; i < region_size; ++i) {
        data.get()[i] = i;
    }

    MockContainer container;
    auto mockSegment = std::make_shared<CustomizedAiThetaContainer::Segment>(data, data_size, padding_size, data_crc);
    EXPECT_CALL(container, get(_)).WillRepeatedly(Return(mockSegment));
    {
        auto segment = container.fetch("test", 0);
        EXPECT_NE(nullptr, segment);
        EXPECT_NE(segment.get(), mockSegment.get());
        EXPECT_EQ(12, segment->data_size());
        EXPECT_EQ(4, segment->padding_size());
        EXPECT_EQ(1, segment->data_crc());
        const void* actualData {nullptr};
        EXPECT_EQ(region_size, segment->read(0, &actualData, region_size));
        for (size_t i = 0; i < region_size; ++i) {
            EXPECT_EQ(((const int8_t*)actualData)[i], data.get()[i]);
        }
    }
    {
        auto segment = container.fetch("test", 1);
        EXPECT_NE(nullptr, segment);
        EXPECT_EQ(segment.get(), mockSegment.get());
    }
}

AUTIL_LOG_SETUP(indexlib.index, CustomizedAiThetaContainerTest);
} // namespace indexlibv2::index::ann
