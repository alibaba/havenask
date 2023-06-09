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

#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/index/inverted_index/format/ComplexDocid.h"

namespace indexlib::index {

class PatchFormat
{
public:
    struct PatchMeta {
        size_t nonNullTermCount = 0; // 8b
        uint8_t hasNullTerm = 0;     // 1b
        uint8_t unused[7];           // 7b;
    };

    PatchFormat() { static_assert(sizeof(PatchMeta) == 16, "PatchMeta size modified"); }
    ~PatchFormat();

    template <typename DocVector>
    static void WriteDocListForTermToPatchFile(const std::shared_ptr<file_system::FileWriter>& fileWriter,
                                               uint64_t termKey, const DocVector& docList);
};

template <typename DocVector>
inline void PatchFormat::WriteDocListForTermToPatchFile(const std::shared_ptr<file_system::FileWriter>& fileWriter,
                                                        uint64_t termKey, const DocVector& docList)
{
    assert(fileWriter != nullptr);
    uint32_t docCount = docList.size();
    fileWriter->Write(&termKey, sizeof(termKey)).GetOrThrow();
    fileWriter->Write(&docCount, sizeof(docCount)).GetOrThrow();
    for (uint32_t idx = 0; idx < docCount; ++idx) {
        assert(idx == 0 || docList[idx] > docList[idx - 1]);
        fileWriter->Write(&docList[idx], sizeof(ComplexDocId)).GetOrThrow();
    }
}

} // namespace indexlib::index
