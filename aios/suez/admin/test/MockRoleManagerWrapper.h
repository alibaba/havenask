#pragma once

#include <cstdint>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <map>
#include <string>
#include <vector>

#include "carbon/RoleManagerWrapper.h"
#include "carbon/legacy/RoleManagerWrapperInternal.h"
#include "hippo/proto/Common.pb.h"

namespace google {
namespace protobuf {
class Service;
}
} // namespace google
namespace worker_framework {
namespace worker_base {
class LeaderElector;
}
} // namespace worker_framework

namespace suez {

class MockRoleManagerWrapper : public carbon::RoleManagerWrapper {
public:
    MockRoleManagerWrapper() {}
    ~MockRoleManagerWrapper() {}

public:
    MOCK_METHOD6(init,
                 bool(const std::string &,
                      const std::string &,
                      const std::string &,
                      bool,
                      worker_framework::LeaderElector *,
                      uint32_t));
    MOCK_METHOD0(start, bool());
    MOCK_METHOD0(stop, bool());
    MOCK_METHOD1(setGroups, bool(const std::map<std::string, carbon::GroupDesc> &));
    MOCK_METHOD2(getGroupStatuses,
                 void(const std::vector<std::string> &, std::map<std::string, carbon::GroupStatus> &));
    MOCK_METHOD1(getGroupStatuses, void(std::map<std::string, carbon::GroupStatus> &));
    MOCK_METHOD3(cancelRollingUpdate, bool(const std::string &, bool, bool));
    MOCK_METHOD2(releseSlots,
                 bool(const std::vector<hippo::proto::SlotId> &, const hippo::proto::PreferenceDescription &));
    MOCK_CONST_METHOD0(getService, ::google::protobuf::Service *());
    MOCK_CONST_METHOD0(getOpsService, ::google::protobuf::Service *());
};

typedef ::testing::StrictMock<MockRoleManagerWrapper> StrictMockRoleManagerWrapper;
typedef ::testing::NiceMock<MockRoleManagerWrapper> NiceMockRoleManagerWrapper;

} // namespace suez
