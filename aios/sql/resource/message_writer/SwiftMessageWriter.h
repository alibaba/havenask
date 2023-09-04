/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <memory>

#include "sql/resource/MessageWriter.h"
#include "swift/client/helper/SwiftWriterAsyncHelper.h"

namespace suez {
struct MessageWriteParam;
} // namespace suez

namespace sql {

class SwiftMessageWriter : public MessageWriter {
public:
    SwiftMessageWriter(swift::client::SwiftWriterAsyncHelperPtr swiftWriter);
    ~SwiftMessageWriter();
    SwiftMessageWriter(const SwiftMessageWriter &) = delete;
    SwiftMessageWriter &operator=(const SwiftMessageWriter &) = delete;

public:
    void write(suez::MessageWriteParam param) override;

private:
    swift::client::SwiftWriterAsyncHelperPtr _swiftWriter;
};

typedef std::shared_ptr<SwiftMessageWriter> SwiftMessageWriterPtr;

} // namespace sql
