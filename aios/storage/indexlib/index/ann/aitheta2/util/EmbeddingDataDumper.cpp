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
#include "indexlib/index/ann/aitheta2/util/EmbeddingDataDumper.h"
using namespace indexlib::file_system;

namespace indexlibv2::index::ann {

bool EmbeddingDataDumper::Dump(const EmbeddingDataHolder& holder, Segment& segment)
{
    auto segDataWriter = segment.GetSegmentDataWriter();
    ANN_CHECK(segDataWriter, "get segment data writer failed");
    auto writer = segDataWriter->GetEmbeddingDataWriter();
    ANN_CHECK(writer, "get embedding data writer failed");

    AUTIL_LOG(INFO, "dumping embedding data");
    for (auto& [indexId, embData] : holder.GetEmbeddingDataMap()) {
        uint64_t count = embData->count();
        if (count == 0) {
            continue;
        }
        EmbeddingDataHeader header = {indexId, count};
        ANN_CHECK(sizeof(header) == writer->Write(&header, sizeof(header)).GetOrThrow(), "write header failed");
        const size_t docIdSize = sizeof(docid_t);
        const size_t dimension = embData->dimension();
        const size_t embeddingSize = dimension * sizeof(float);
        auto iter = embData->create_iterator();
        while (iter->is_valid()) {
            docid_t docId = iter->key();
            ANN_CHECK(docIdSize == writer->Write(&docId, docIdSize).GetOrThrow(), "write docId failed");
            ANN_CHECK(embeddingSize == writer->Write(iter->data(), embeddingSize).GetOrThrow(),
                      "write embedding failed");
            iter->next();
        }
    }
    AUTIL_LOG(INFO, "dump embedding data success");
    return true;
}

AUTIL_LOG_SETUP(indexlib.index, EmbeddingDataDumper);
} // namespace indexlibv2::index::ann
