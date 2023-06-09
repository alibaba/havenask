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

#include "orc/OrcFile.hh"

namespace indexlib { namespace file_system {
class FileReader;
}} // namespace indexlib::file_system

namespace indexlibv2::index {

class InputStreamImpl : public orc::InputStream
{
public:
    InputStreamImpl(const std::shared_ptr<indexlib::file_system::FileReader>& fileReader);
    ~InputStreamImpl();

public:
    uint64_t getLength() const override;
    uint64_t getNaturalReadSize() const override;
    void read(void* buf, uint64_t length, uint64_t offset) override;
    void readAsync(void* buf, uint64_t length, uint64_t offset) override;
    void loadCache(uint64_t offset, uint64_t length) override;
    orc::AsyncState waitForAsyncComplete(uint64_t offset, int64_t timeOut) override;
    const std::string& getName() const override;
    const std::string& getUniqueId() const override;
    uint64_t GetReadBytes() const override;
    bool isAsyncReadSupported() const override;
    uint64_t getPendingRequestCount() const override;

private:
    std::shared_ptr<indexlib::file_system::FileReader> _fileReader;
    uint64_t _readBytes;
    std::unordered_set<uint64_t> _failedRequests;
};

} // namespace indexlibv2::index
