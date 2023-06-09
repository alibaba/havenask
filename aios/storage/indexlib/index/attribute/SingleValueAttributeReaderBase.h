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

#include "autil/NoCopyable.h"
#include "indexlib/base/Define.h"

namespace indexlib::file_system {
class FileStream;
}
namespace indexlibv2::index {

template <typename T>
class SingleValueAttributeReaderBase : private autil::NoCopyable
{
public:
    SingleValueAttributeReaderBase() = default;
    ~SingleValueAttributeReaderBase() = default;

    bool Updatable() const { return _updatable; }
    uint64_t GetDocCount() const __ALWAYS_INLINE { return _docCount; }
    uint8_t* GetDataBaseAddr() { return _data; }

public:
    uint32_t TEST_GetDataLength() const { return _dataSize; }

protected:
    std::shared_ptr<indexlib::file_system::FileStream> _fileStream;
    uint8_t* _data;
    uint64_t _docCount;
    uint8_t _dataSize;
    bool _updatable;
};

} // namespace indexlibv2::index
