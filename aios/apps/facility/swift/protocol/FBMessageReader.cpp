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
#include "swift/protocol/FBMessageReader.h"

#include <cstddef>
#include <stdint.h>

#include "flatbuffers/flatbuffers.h"
#include "swift/protocol/SwiftMessage_generated.h"

using namespace std;
namespace swift {
namespace protocol {
AUTIL_LOG_SETUP(swift, FBMessageReader);

void FBMessageReader::reset() {
    _msgs = NULL;
    _size = 0;
}

bool FBMessageReader::init(const std::string &data, bool needValidate) {
    return init(data.c_str(), data.size(), needValidate);
}

bool FBMessageReader::init(const char *data, size_t dataLen, bool needValidate) {
    reset();
    if (needValidate) {
        flatbuffers::Verifier verifier((const uint8_t *)data, dataLen);
        if (!flat::VerifyMessagesBuffer(verifier)) {
            return false;
        }
    }

    const flat::Messages *msgs = flatbuffers::GetRoot<flat::Messages>(data);
    if (msgs == NULL) {
        return false;
    }
    _msgs = msgs->msgs();
    if (_msgs == NULL) {
        return false;
    }
    _size = _msgs->size();
    return true;
}

const swift::protocol::flat::Message *FBMessageReader::read(size_t offset) const {
    if (offset < _size) {
        return _msgs->Get(offset);
    } else {
        return NULL;
    }
}
size_t FBMessageReader::size() const { return _size; }

} // namespace protocol
} // namespace swift
