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
#include "indexlib/index/inverted_index/patch/InvertedIndexPatchMerger.h"

#include "indexlib/index/inverted_index/Common.h"
#include "indexlib/index/inverted_index/config/InvertedIndexConfig.h"
#include "indexlib/index/inverted_index/patch/SingleFieldIndexSegmentPatchIterator.h"
#include "indexlib/index/inverted_index/patch/SingleTermIndexSegmentPatchIterator.h"
#include "indexlib/util/Algorithm.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, InvertedIndexPatchMerger);

InvertedIndexPatchMerger::InvertedIndexPatchMerger(
    const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& invertedIndexConfig)
    : _invertedIndexConfig(invertedIndexConfig)
{
}
std::pair<Status, std::shared_ptr<file_system::FileWriter>>
InvertedIndexPatchMerger::CreateTargetPatchFileWriter(std::shared_ptr<file_system::IDirectory> targetSegmentDir,
                                                      segmentid_t srcSegmentId, segmentid_t targetSegmentId)
{
    auto [status1, targetInvertedIndexDir] = targetSegmentDir->GetDirectory(INVERTED_INDEX_PATH).StatusWith();
    RETURN2_IF_STATUS_ERROR(status1, nullptr, "get inverted index dir in [%s] failed, error[%s]",
                            targetSegmentDir->GetLogicalPath().c_str(), status1.ToString().c_str());
    assert(targetInvertedIndexDir != nullptr);

    const std::string& indexName = _invertedIndexConfig->GetIndexName();
    auto [status2, targetIndexDir] = targetInvertedIndexDir->GetDirectory(indexName).StatusWith();
    RETURN2_IF_STATUS_ERROR(status2, nullptr, "get field dir failed, inverted index [%s]", indexName.c_str());

    std::string patchFileName = autil::StringUtil::toString(srcSegmentId) + "_" +
                                autil::StringUtil::toString(targetSegmentId) + "." + PATCH_FILE_NAME;
    auto [status3, targetPatchFileWriter] =
        targetIndexDir->CreateFileWriter(patchFileName, indexlib::file_system::WriterOption()).StatusWith();
    RETURN2_IF_STATUS_ERROR(status3, nullptr, "create patch file writer failed for patch file: %s",
                            patchFileName.c_str());
    return {Status::OK(), targetPatchFileWriter};
}

Status InvertedIndexPatchMerger::Merge(const indexlibv2::index::PatchFileInfos& patchFileInfos,
                                       const std::shared_ptr<file_system::FileWriter>& targetPatchFileWriter)
{
    if (patchFileInfos.Size() == 0) {
        return Status::OK();
    }
    if (patchFileInfos.Size() == 1) {
        if (patchFileInfos[0].srcSegment == patchFileInfos[0].destSegment) {
            return Status::OK();
        }
        AUTIL_LOG(INFO, "only find one patch file [%s], just copy it", patchFileInfos[0].patchFileName.c_str());
        return PatchMerger::CopyFile(patchFileInfos[0].patchDirectory, patchFileInfos[0].patchFileName,
                                     targetPatchFileWriter);
    }

    auto patchIter =
        std::make_unique<SingleFieldIndexSegmentPatchIterator>(_invertedIndexConfig, patchFileInfos[0].destSegment);
    for (size_t i = 0; i < patchFileInfos.Size(); i++) {
        if (patchFileInfos[i].srcSegment == patchFileInfos[i].destSegment) {
            continue;
        }
        auto status = patchIter->AddPatchFile(patchFileInfos[i].patchDirectory, patchFileInfos[i].patchFileName,
                                              patchFileInfos[i].srcSegment, patchFileInfos[i].destSegment);
        RETURN_IF_STATUS_ERROR(status, "load patch file [%s] failed", patchFileInfos[i].patchFileName.c_str());
    }
    return DoMerge(std::move(patchIter), targetPatchFileWriter);
}

Status InvertedIndexPatchMerger::DoMerge(std::unique_ptr<SingleFieldIndexSegmentPatchIterator> patchIter,
                                         const std::shared_ptr<file_system::FileWriter>& targetPatchFileWriter)
{
    file_system::FileWriterPtr outputPatchFileWriter =
        ConvertToCompressFileWriter(targetPatchFileWriter, _invertedIndexConfig->IsPatchCompressed());
    assert(outputPatchFileWriter);

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
        PatchFormat::WriteDocListForTermToPatchFile(outputPatchFileWriter, termKey.GetKey(), docList);
    }
    PatchFormat::PatchMeta meta;
    meta.nonNullTermCount = nonNullTermCount;
    meta.hasNullTerm = hasNullTerm;
    auto [status, writeLength] = outputPatchFileWriter->Write(&meta, sizeof(meta)).StatusWith();
    assert(writeLength == sizeof(meta));
    RETURN_IF_STATUS_ERROR(status, "inverted index merge write patch meta to [%s] failed",
                           outputPatchFileWriter->GetPhysicalPath().c_str());
    status = outputPatchFileWriter->Close().Status();
    RETURN_IF_STATUS_ERROR(status, "inverted index merge close patch writer [%s] failed",
                           outputPatchFileWriter->GetPhysicalPath().c_str());
    return Status::OK();
}

} // namespace indexlib::index
