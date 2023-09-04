#pragma once

#include <functional>
#include <gmock/gmock-function-mocker.h>
#include <stddef.h>
#include <stdint.h>

#include "swift/client/helper/SwiftWriterAsyncHelper.h"
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

} // namespace testlib
} // namespace swift
