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
#include "indexlib/index/attribute/UniqEncodeSequenceIteratorTyped.h"

namespace indexlibv2::index {
namespace {
class MemoryFileStream : public indexlib::file_system::FileStream
{
public:
    MemoryFileStream(const autil::StringView& content) : FileStream(content.length(), false), _content(content) {}
    ~MemoryFileStream() = default;

public:
    indexlib::file_system::FSResult<size_t> Read(void* buffer, size_t length, size_t offset,
                                                 indexlib::file_system::ReadOption option) override
    {
        if (offset >= _content.size()) {
            return indexlib::file_system::FSResult<size_t>(indexlib::file_system::ErrorCode::FSEC_OK, 0);
        }
        length = std::min(length, _content.size() - offset);
        memcpy(buffer, _content.data() + offset, length);
        return indexlib::file_system::FSResult<size_t>(indexlib::file_system::ErrorCode::FSEC_OK, length);
    }
    std::string DebugString() const override { return "InMemFile"; }
    size_t GetLockedMemoryUse() const override { return _content.size(); }

private:
    future_lite::coro::Lazy<std::vector<indexlib::file_system::FSResult<size_t>>>
    BatchRead(indexlib::file_system::BatchIO& batchIO, indexlib::file_system::ReadOption option) noexcept override
    {
        assert(false);
        std::vector<indexlib::file_system::FSResult<size_t>> ret;
        co_return ret;
    }

    future_lite::Future<indexlib::file_system::FSResult<size_t>>
    ReadAsync(void* buffer, size_t length, size_t offset, indexlib::file_system::ReadOption option) override
    {
        assert(false);
        using namespace indexlib::file_system;
        return future_lite::makeReadyFuture(FSResult<size_t> {FSEC_ERROR, 0});
    }
    std::shared_ptr<indexlib::file_system::FileStream> CreateSessionStream(autil::mem_pool::Pool* pool) const override
    {
        assert(false);
        return nullptr;
    }

private:
    autil::StringView _content;
};
} // namespace

std::shared_ptr<indexlib::file_system::FileStream>
MemoryFileStreamCreator::CreateMemoryFileStream(const autil::StringView& stringView)
{
    return std::make_shared<MemoryFileStream>(stringView);
}

} // namespace indexlibv2::index
