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

#include "indexlib/index/kv/ValueReader.h"

namespace indexlib::file_system {
class FileReader;
}

namespace indexlibv2::index {

class VarLenValueReader final : public ValueReader
{
public:
    explicit VarLenValueReader(std::shared_ptr<indexlib::file_system::FileReader> file);
    ~VarLenValueReader();

public:
    Status Read(uint64_t offset, autil::mem_pool::Pool* pool, autil::StringView& data) override;

private:
    std::shared_ptr<indexlib::file_system::FileReader> _file;
};

} // namespace indexlibv2::index
