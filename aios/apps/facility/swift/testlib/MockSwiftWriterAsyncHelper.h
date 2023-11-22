#pragma once

#include <functional>
#include <gmock/gmock-function-mocker.h>
#include <stddef.h>
#include <stdint.h>

#include "swift/client/helper/SwiftWriterAsyncHelper.h"
#include "swift/client/helper/FixedTimeoutSwiftWriterAsyncHelper.h"
#include "unittest/unittest.h"

namespace swift {
namespace common {
class MessageInfo;
} // namespace common

namespace testlib {

class MockSwiftWriterAsyncHelper : public swift::client::SwiftWriterAsyncHelper {
public:
    MOCK_METHOD4(write,
                 void(common::MessageInfo *msgInfos,
                      size_t count,
                      swift::client::SwiftWriteCallbackItem::WriteCallbackFunc callback,
                      int64_t timeout));
};

class MockFixedTimeoutSwiftWriterAsyncHelper : public swift::client::FixedTimeoutSwiftWriterAsyncHelper {
public:
    MOCK_METHOD3(write,
                 void(common::MessageInfo *msgInfos,
                      size_t count,
                      swift::client::SwiftWriteCallbackItem::WriteCallbackFunc callback));
};

} // namespace testlib
} // namespace swift
