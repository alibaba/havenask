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
#include "swift/protocol/FBMessageWriter.h"

#include <cstddef>

#include "swift/protocol/SwiftMessage_generated.h"

using namespace std;
namespace swift {
namespace protocol {
AUTIL_LOG_SETUP(swift, FBMessageWriter);
int64_t FBMessageWriter::FB_RECYCLE_BUFFER_SIZE = 16 * 1024 * 1024; // 16M

FBMessageWriter::FBMessageWriter() : _isFinish(false) {
    _fbAllocator = new SimpleFbAllocator();
    _fbBuilder = new flatbuffers::FlatBufferBuilder(256 * 1024, _fbAllocator, false);
}

FBMessageWriter::~FBMessageWriter() {
    if (_fbBuilder) {
        delete _fbBuilder;
        _fbBuilder = nullptr;
    }
    if (_fbAllocator) {
        delete _fbAllocator;
        _fbAllocator = nullptr;
    }
}

void FBMessageWriter::reset() {
    _fbBuilder->Clear();
    _offsetVec.clear();
    _isFinish = false;
    if (_fbAllocator->getAllocatedSize() > FB_RECYCLE_BUFFER_SIZE) {
        AUTIL_INTERVAL_LOG(100,
                           INFO,
                           "flat buffer writer size is  "
                           "[%ld] > [%ld], release.",
                           _fbAllocator->getAllocatedSize(),
                           FB_RECYCLE_BUFFER_SIZE);
        delete _fbBuilder;
        delete _fbAllocator;
        _fbAllocator = new SimpleFbAllocator();
        _fbBuilder = new flatbuffers::FlatBufferBuilder(256 * 1024);
    }
}

const char *FBMessageWriter::data() {
    if (!isFinish()) {
        finish();
    }
    return reinterpret_cast<const char *>(_fbBuilder->GetBufferPointer());
}

size_t FBMessageWriter::size() {
    if (!isFinish()) {
        finish();
    }
    return _fbBuilder->GetSize();
}

void FBMessageWriter::finish() {
    if (isFinish()) {
        return;
    }
    flatbuffers::Offset<flat::Messages> msgs = flat::CreateMessages(*_fbBuilder, _fbBuilder->CreateVector(_offsetVec));
    _fbBuilder->Finish(msgs);
    _isFinish = true;
}

} // namespace protocol
} // namespace swift
