#pragma once

#include "unittest/unittest.h"
#include "worker_framework/DataClient.h"
#include "worker_framework/DataItem.h"

using namespace worker_framework;

namespace suez {

class MockDataClient : public DataClient {
public:
    MockDataClient() {}
    ~MockDataClient() {}

public:
    MOCK_METHOD4(
        getData,
        DataItemPtr(const std::string &, const std::vector<std::string> &, const std::string &, const DataOption &));
};

class MockDataItem : public DataItem {
public:
    MockDataItem() : DataItem("", "", DataOption()) {}
    MOCK_METHOD0(doCancel, void());
};

typedef ::testing::StrictMock<MockDataClient> StrictMockDataClient;
typedef ::testing::NiceMock<MockDataClient> NiceMockDataClient;

using MockDataClientPtr = std::shared_ptr<MockDataClient>;

} // namespace suez
