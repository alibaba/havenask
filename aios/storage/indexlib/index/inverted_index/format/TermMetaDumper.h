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

#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/index/inverted_index/format/PostingFormatOption.h"
#include "indexlib/index/inverted_index/format/TermMeta.h"

namespace indexlib::index {

class TermMetaDumper
{
public:
    explicit TermMetaDumper(const PostingFormatOption& option);
    TermMetaDumper() = default;
    ~TermMetaDumper() = default;

    uint32_t CalculateStoreSize(const TermMeta& termMeta) const;
    void Dump(const std::shared_ptr<file_system::FileWriter>& file, const TermMeta& termMeta) const;

    // ReadVUInt32 + ReadVUInt32 + sizeof(payload)
    static size_t MaxStoreSize() { return sizeof(uint32_t) + 1 + sizeof(uint32_t) + 1 + sizeof(termpayload_t); }

private:
    PostingFormatOption _option;
};

} // namespace indexlib::index
