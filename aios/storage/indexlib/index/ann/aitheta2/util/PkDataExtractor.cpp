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
#include "indexlib/index/ann/aitheta2/util/PkDataExtractor.h"

using namespace indexlib::file_system;
namespace indexlibv2::index::ann {

bool PkDataExtractor::Extract(const std::vector<NormalSegmentPtr>& segments, PkDataHolder& result)
{
    for (auto segment : segments) {
        auto segDataReader = segment->GetSegmentDataReader();
        ANN_CHECK(segDataReader, "get segment data pkReader failed");
        auto pkReader = segDataReader->GetPrimaryKeyReader();
        if (!pkReader) {
            continue;
        }
        pkReader->Seek(0ul).GetOrThrow();

        auto& docIdMap = segment->GetDocMapWrapper();
        while (pkReader->Tell() < pkReader->GetLength()) {
            PkDataHeader header {};
            ANN_CHECK(sizeof(header) == pkReader->Read(&header, sizeof(header)).GetOrThrow(),
                      "read pk data header failed");

            if (!_mergeTask.empty() && _mergeTask.find(header.indexId) == _mergeTask.end()) {
                int64_t dataSize = (sizeof(primary_key_t) + sizeof(docid_t)) * header.count;
                int64_t offset = pkReader->Tell() + dataSize;
                pkReader->Seek(offset).GetOrThrow();
                continue;
            }

            for (uint64_t i = 0; i < header.count; ++i) {
                primary_key_t pk = 0;
                docid_t docId = INVALID_DOCID;
                ANN_CHECK(sizeof(pk) == pkReader->Read(&pk, sizeof(pk)).GetOrThrow(), "read pk failed");
                ANN_CHECK(sizeof(docId) == pkReader->Read(&docId, sizeof(docId)).GetOrThrow(), "read docId failed");

                docid_t updatedDocId = docIdMap->GetNewDocId(docId);
                if (updatedDocId != INVALID_DOCID) {
                    result.Add(pk, updatedDocId, {_compactIndex ? kDefaultIndexId : header.indexId});
                }
            }
        }
    }

    return true;
}

bool PkDataExtractor::ExtractWithoutDocIdMap(const NormalSegmentPtr& segment, PkDataHolder& result)
{
    auto segDataReader = segment->GetSegmentDataReader();
    ANN_CHECK(segDataReader, "get segment data pkReader failed");
    auto pkReader = segDataReader->GetPrimaryKeyReader();
    if (!pkReader) {
        return false;
    }
    pkReader->Seek(0ul).GetOrThrow();

    while (pkReader->Tell() < pkReader->GetLength()) {
        PkDataHeader header {};
        ANN_CHECK(sizeof(header) == pkReader->Read(&header, sizeof(header)).GetOrThrow(), "read pk data header failed");

        if (!_mergeTask.empty() && _mergeTask.find(header.indexId) == _mergeTask.end()) {
            int64_t dataSize = (sizeof(primary_key_t) + sizeof(docid_t)) * header.count;
            int64_t offset = pkReader->Tell() + dataSize;
            pkReader->Seek(offset).GetOrThrow();
            continue;
        }

        for (uint64_t i = 0; i < header.count; ++i) {
            primary_key_t pk = 0;
            docid_t docId = INVALID_DOCID;
            ANN_CHECK(sizeof(pk) == pkReader->Read(&pk, sizeof(pk)).GetOrThrow(), "read pk failed");
            ANN_CHECK(sizeof(docId) == pkReader->Read(&docId, sizeof(docId)).GetOrThrow(), "read docId failed");
            result.Add(pk, docId, {_compactIndex ? kDefaultIndexId : header.indexId});
        }
    }
    return true;
}

AUTIL_LOG_SETUP(indexlib.index, PkDataExtractor);

} // namespace indexlibv2::index::ann
