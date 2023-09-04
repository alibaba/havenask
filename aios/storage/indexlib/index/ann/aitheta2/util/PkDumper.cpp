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
#include "indexlib/index/ann/aitheta2/util/PkDumper.h"

namespace indexlibv2::index::ann {

bool PkDumper::Dump(const PkDataHolder& pkDataHolder, Segment& segment)
{
    auto segDataWriter = segment.GetSegmentDataWriter();
    ANN_CHECK(segDataWriter, "get segment data pkWriter failed");
    auto pkWriter = segDataWriter->GetPrimaryKeyWriter();
    ANN_CHECK(pkWriter, "get primary key pkWriter failed");

    AUTIL_LOG(INFO, "dumping primary key");
    const auto& pkDataMap = pkDataHolder.GetPkDataMap();
    for (auto& [indexId, pkData] : pkDataMap) {
        uint64_t count = pkData.Size();
        if (count == 0) {
            continue;
        }
        PkDataHeader header = {indexId, count};
        ANN_CHECK(sizeof(header) == pkWriter->Write(&header, sizeof(header)).GetOrThrow(), "write header failed");
        size_t pkSize = sizeof(primary_key_t);
        size_t docIdSize = sizeof(docid_t);
        auto iter = pkData.CreateIterator();
        while (iter->isValid()) {
            ANN_CHECK(pkSize == pkWriter->Write(iter->pk(), pkSize).GetOrThrow(), "write pk failed");
            ANN_CHECK(docIdSize == pkWriter->Write(iter->docId(), docIdSize).GetOrThrow(), "write docId failed");
            iter->next();
        }
    }
    AUTIL_LOG(INFO, "dump primary key success");
    return true;
}

AUTIL_LOG_SETUP(indexlib.index, PkDumper);
} // namespace indexlibv2::index::ann
