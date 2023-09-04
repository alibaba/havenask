#pragma once

#include <gmock/gmock-function-mocker.h>
#include <gmock/gmock-spec-builders.h>

#include "swift/admin/AdminStatusManager.h"
#include "swift/admin/modules/BaseModule.h"

namespace swift {
namespace admin {

class MockBaseModule : public BaseModule {
public:
    MockBaseModule() {}
    ~MockBaseModule() {}

public:
    MOCK_METHOD0(doInit, bool());
    MOCK_METHOD0(doLoad, bool());
    MOCK_METHOD0(doUnload, bool());
    MOCK_METHOD1(doUpdate, bool(Status stauts));
};

} // namespace admin
} // namespace swift
