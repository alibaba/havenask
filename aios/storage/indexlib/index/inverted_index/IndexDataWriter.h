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
#include "indexlib/index/inverted_index/format/dictionary/TieredDictionaryWriter.h"

namespace indexlib::index {

struct IndexDataWriter {
public:
    bool IsValid() const { return dictWriter.get() != NULL; }
    void Reset()
    {
        dictWriter.reset();
        postingWriter.reset();
    }
    std::shared_ptr<DictionaryWriter> dictWriter;
    file_system::FileWriterPtr postingWriter;
};

} // namespace indexlib::index
