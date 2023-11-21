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

#include "indexlib/file_system/file/FileReader.h"
#include "unittest/unittest.h"

namespace indexlib { namespace file_system {

class MockFileReader : public FileReader
{
public:
    MOCK_METHOD(FSResult<void>, Open, (), (noexcept, override));
    MOCK_METHOD(FSResult<void>, Close, (), (noexcept, override));
    MOCK_METHOD(FSResult<size_t>, Read, (void* buffer, size_t length, size_t offset, ReadOption option),
                (noexcept, override));
    MOCK_METHOD(FSResult<size_t>, Read, (void* buffer, size_t length, ReadOption option), (noexcept, override));
    MOCK_METHOD(util::ByteSliceList*, ReadToByteSliceList, (size_t length, size_t offset, ReadOption option),
                (noexcept, override));
    MOCK_METHOD(void*, GetBaseAddress, (), (const, noexcept, override));
    MOCK_METHOD(size_t, GetLength, (), (const, noexcept, override));

    // logic length match with offset, which is equal to uncompress file length for compress file
    MOCK_METHOD(size_t, GetLogicLength, (), (const, noexcept, override));
    MOCK_METHOD(const std::string&, GetLogicalPath, (), (const, noexcept, override));
    MOCK_METHOD(const std::string&, GetPhysicalPath, (), (const, noexcept, override));
    MOCK_METHOD(FSOpenType, GetOpenType, (), (const, noexcept, override));
    MOCK_METHOD(FSFileType, GetType, (), (const, noexcept, override));
    MOCK_METHOD(FSResult<size_t>, Prefetch, (size_t length, size_t offset, ReadOption option), (noexcept, override));
    MOCK_METHOD(std::shared_ptr<FileNode>, GetFileNode, (), (const, noexcept, override));

    // future interface
    MOCK_METHOD(future_lite::Future<FSResult<size_t>>, ReadAsync, (void*, size_t, size_t, ReadOption),
                (noexcept, override));
    MOCK_METHOD(future_lite::Future<FSResult<uint32_t>>, ReadUInt32Async, (size_t offset, ReadOption option),
                (noexcept, override));
    MOCK_METHOD(future_lite::Future<FSResult<uint32_t>>, ReadVUInt32Async, (ReadOption option), (noexcept, override));
    MOCK_METHOD(future_lite::Future<FSResult<uint32_t>>, ReadVUInt32Async, (size_t offset, ReadOption option),
                (noexcept, override));
    MOCK_METHOD(future_lite::Future<FSResult<size_t>>, PrefetchAsync, (size_t length, size_t offset, ReadOption option),
                (noexcept, override));

    // lazy
    MOCK_METHOD(FL_LAZY(FSResult<size_t>), ReadAsyncCoro,
                (void* buffer, size_t length, size_t offset, ReadOption option), (noexcept, override));
    MOCK_METHOD(FL_LAZY(FSResult<size_t>), PrefetchAsyncCoro, (size_t length, size_t offset, ReadOption option),
                (noexcept, override));
};

typedef ::testing::StrictMock<MockFileReader> StrictMockFileReader;
typedef ::testing::NiceMock<MockFileReader> NiceMockFileReader;

}} // namespace indexlib::file_system
