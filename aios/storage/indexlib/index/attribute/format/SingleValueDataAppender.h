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

#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/index/attribute/format/SingleValueAttributeUpdatableFormatter.h"
#include "indexlib/index/common/data_structure/EqualValueCompressDumper.h"

namespace indexlibv2::index {

class SingleValueDataAppender
{
public:
    SingleValueDataAppender(AttributeFormatter* formatter);
    ~SingleValueDataAppender() = default;

public:
    void Init(uint32_t capacity, const std::shared_ptr<indexlib::file_system::FileWriter>& dataFile);
    bool IsFull() const { return _inBufferCount == _capacity; }
    Status Flush();
    template <typename T>
    void FlushCompressBuffer(EqualValueCompressDumper<T>* dumper);

    template <typename T>
    void Append(const T& value, bool isNull);

    void Close();

    uint32_t GetTotalCount() const { return _totalCount; }
    uint32_t GetInBufferCount() const { return _inBufferCount; }
    uint8_t* GetDataBuffer() const { return _dataBuffer.get(); }
    const std::shared_ptr<indexlib::file_system::FileWriter>& GetDataFileWriter() const { return _dataFileWriter; }

private:
    uint32_t _capacity;
    uint32_t _inBufferCount;
    uint32_t _totalCount;
    AttributeFormatter* _formatter;
    std::unique_ptr<uint8_t[]> _dataBuffer;
    std::shared_ptr<indexlib::file_system::FileWriter> _dataFileWriter;

private:
    AUTIL_LOG_DECLARE();
};

template <typename T>
void SingleValueDataAppender::FlushCompressBuffer(EqualValueCompressDumper<T>* dumper)
{
    assert(dumper != nullptr);
    if (_inBufferCount == 0) {
        return;
    }
    dumper->CompressData((T*)_dataBuffer.get(), _inBufferCount);
    _inBufferCount = 0;
}

template <typename T>
void SingleValueDataAppender::Append(const T& value, bool isNull)
{
    assert(_dataBuffer != nullptr);
    auto updatableFormatter = static_cast<SingleValueAttributeUpdatableFormatter<T>*>(_formatter);
    assert(updatableFormatter != nullptr);
    updatableFormatter->Set(_inBufferCount, _dataBuffer.get(), value, isNull);
    ++_inBufferCount;
    ++_totalCount;
}

} // namespace indexlibv2::index
