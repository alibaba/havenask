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

#include <stddef.h>
#include <string>

#include "autil/Log.h"
#include "swift/common/Common.h"

namespace flatbuffers {
template <typename T>
class Vector;
template <typename T>
struct Offset;
} // namespace flatbuffers

namespace swift {
namespace protocol {
namespace flat {
struct Message;
} // namespace flat

class FBMessageReader {
public:
    FBMessageReader() { reset(); }
    ~FBMessageReader() {}

private:
    FBMessageReader(const FBMessageReader &);
    FBMessageReader &operator=(const FBMessageReader &);

public:
    bool init(const std::string &data, bool needValidate = true);
    bool init(const char *data, size_t dataLen, bool needValidate = true);
    void reset();
    size_t size() const;
    const flat::Message *read(size_t offset) const;

private:
    const flatbuffers::Vector<flatbuffers::Offset<flat::Message>> *_msgs;
    size_t _size;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(FBMessageReader);

} // namespace protocol
} // namespace swift
