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
#include "indexlib/index/normal/inverted_index/accessor/index_patch_merger.h"

#include "indexlib/config/index_config.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/file/BufferedFileWriter.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/index/inverted_index/format/PatchFormat.h"
#include "indexlib/index/inverted_index/patch/SingleFieldIndexSegmentPatchIterator.h"
#include "indexlib/index/inverted_index/patch/SingleTermIndexSegmentPatchIterator.h"
#include "indexlib/index_define.h"
#include "indexlib/util/Algorithm.h"

namespace indexlib { namespace index {
IE_LOG_SETUP(index, IndexPatchMerger);

IndexPatchMerger::IndexPatchMerger(const config::IndexConfigPtr& indexConfig) : _indexConfig(indexConfig) {}

IndexPatchMerger::~IndexPatchMerger() {}

void IndexPatchMerger::Merge(const index_base::PatchFileInfoVec& patchFileInfoVec,
                             const file_system::FileWriterPtr& destPatchFile)
{
    if (patchFileInfoVec.empty()) {
        return;
    }
    if (patchFileInfoVec.size() == 1) {
        if (patchFileInfoVec[0].srcSegment == patchFileInfoVec[0].destSegment) {
            return;
        }
        PatchMerger::CopyFile(patchFileInfoVec[0].patchDirectory, patchFileInfoVec[0].patchFileName, destPatchFile);
        return;
    }

    auto patchIter =
        std::make_unique<SingleFieldIndexSegmentPatchIterator>(_indexConfig, patchFileInfoVec[0].destSegment);
    for (size_t i = 0; i < patchFileInfoVec.size(); i++) {
        if (patchFileInfoVec[i].srcSegment == patchFileInfoVec[i].destSegment) {
            continue;
        }
        auto status = patchIter->AddPatchFile(patchFileInfoVec[i].patchDirectory->GetIDirectory(),
                                              patchFileInfoVec[i].patchFileName, patchFileInfoVec[i].srcSegment,
                                              patchFileInfoVec[i].destSegment);
        if (!status.IsOK()) {
            IE_LOG(ERROR, "Add patch file %s failed.", patchFileInfoVec[i].patchFileName.c_str());
        }
    }
    DoMerge(std::move(patchIter), destPatchFile);
}

void IndexPatchMerger::DoMerge(std::unique_ptr<SingleFieldIndexSegmentPatchIterator> patchIter,
                               const file_system::FileWriterPtr& destPatchFile)
{
    file_system::FileWriterPtr patchFile = CreatePatchFileWriter(destPatchFile, _indexConfig->IsPatchCompressed());
    assert(patchFile);

    std::vector<ComplexDocId> docList;
    size_t nonNullTermCount = 0;
    bool hasNullTerm = false;
    while (true) {
        std::unique_ptr<SingleTermIndexSegmentPatchIterator> termIter = patchIter->NextTerm();
        if (termIter == nullptr) {
            break;
        }
        assert(!hasNullTerm);
        docList.clear();
        ComplexDocId docId;
        while (termIter->Next(&docId)) {
            docList.push_back(docId);
        }
        docList.erase(util::Algorithm::SortAndUnique(docList.begin(), docList.end()), docList.end());
        index::DictKeyInfo termKey = termIter->GetTermKey();
        if (!termKey.IsNull()) {
            nonNullTermCount++;
        } else {
            assert(!hasNullTerm);
            hasNullTerm = true;
        }
        PatchFormat::WriteDocListForTermToPatchFile(patchFile, termKey.GetKey(), docList);
    }
    PatchFormat::PatchMeta meta;
    meta.nonNullTermCount = nonNullTermCount;
    meta.hasNullTerm = hasNullTerm;
    patchFile->Write(&meta, sizeof(meta)).GetOrThrow();

    patchFile->Close().GetOrThrow();
}
}} // namespace indexlib::index
