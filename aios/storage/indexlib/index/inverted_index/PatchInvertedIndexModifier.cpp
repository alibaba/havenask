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
#include "indexlib/index/inverted_index/PatchInvertedIndexModifier.h"

#include "indexlib/framework/Segment.h"
#include "indexlib/index/inverted_index/Common.h"
#include "indexlib/index/inverted_index/config/InvertedIndexConfig.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, PatchInvertedIndexModifier);

PatchInvertedIndexModifier::PatchInvertedIndexModifier(
    const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema,
    const std::shared_ptr<indexlib::file_system::IDirectory>& workDir)
    : InvertedIndexModifier(schema)
    , _workDir(workDir)
{
}

Status PatchInvertedIndexModifier::Init(const indexlibv2::framework::TabletData& tabletData)
{
    if (tabletData.GetSegmentCount() == 0) {
        return Status::OK();
    }
    auto slice = tabletData.CreateSlice();
    auto lastSegmentId = (*slice.rbegin())->GetSegmentId();
    docid_t baseDocId = 0;
    for (const auto& segment : slice) {
        if (segment->GetSegmentInfo()->GetDocCount() == 0) {
            continue;
        }
        _segmentInfos.push_back(
            {segment->GetSegmentId(), baseDocId, baseDocId + segment->GetSegmentInfo()->GetDocCount()});
        baseDocId += segment->GetSegmentInfo()->GetDocCount();
    }
    _maxDocCount = baseDocId;

    file_system::DirectoryOption directoryOption;
    auto [status, invertedIndexDir] = _workDir->MakeDirectory(INVERTED_INDEX_PATH, directoryOption).StatusWith();
    RETURN_IF_STATUS_ERROR(status, "create inverted index patch dir [%s] failed", _workDir->DebugString().c_str());

    for (const auto& indexConfig : _schema->GetIndexConfigs(INVERTED_INDEX_TYPE_STR)) {
        auto invertedIndexConfig = std::dynamic_pointer_cast<indexlibv2::config::InvertedIndexConfig>(indexConfig);
        assert(invertedIndexConfig);
        if (invertedIndexConfig->IsIndexUpdatable()) {
            auto patchWriter = std::make_unique<InvertedIndexPatchWriter>(invertedIndexDir, lastSegmentId, indexConfig);
            for (auto fieldConfig : invertedIndexConfig->GetFieldConfigs()) {
                _fieldId2PatchWriters[fieldConfig->GetFieldId()].push_back(patchWriter.get());
            }
            _patchWriters.push_back(std::move(patchWriter));
        }
    }
    return Status::OK();
}

Status PatchInvertedIndexModifier::UpdateOneFieldTokens(docid_t docId, const document::ModifiedTokens& modifiedTokens,
                                                        bool)
{
    assert(docId < _maxDocCount);
    for (auto [segmentId, baseDocId, endDocId] : _segmentInfos) {
        if (docId >= baseDocId && docId < endDocId) {
            for (auto invertedPatchWriter : _fieldId2PatchWriters[modifiedTokens.FieldId()]) {
                for (size_t i = 0; i < modifiedTokens.NonNullTermSize(); ++i) {
                    uint64_t termKey = modifiedTokens[i].first;
                    indexlib::document::ModifiedTokens::Operation op = modifiedTokens[i].second;
                    auto status =
                        invertedPatchWriter->Write(segmentId, docId - baseDocId, indexlib::index::DictKeyInfo(termKey),
                                                   op == indexlib::document::ModifiedTokens::Operation::REMOVE);
                    RETURN_IF_STATUS_ERROR(status, "update inverted index termKey [%lu], segment [%d]", termKey,
                                           segmentId);
                }
                indexlib::document::ModifiedTokens::Operation nullTermOp;
                if (modifiedTokens.GetNullTermOperation(&nullTermOp)) {
                    if (nullTermOp != indexlib::document::ModifiedTokens::Operation::NONE) {
                        auto status = invertedPatchWriter->Write(
                            segmentId, docId - baseDocId, indexlib::index::DictKeyInfo::NULL_TERM,
                            nullTermOp == indexlib::document::ModifiedTokens::Operation::REMOVE);
                        RETURN_IF_STATUS_ERROR(status,
                                               "write null token to patch writer failed, segment [%d] docId [%d]",
                                               segmentId, docId - baseDocId);
                    }
                }
            }
            return Status::OK();
        }
    }
    RETURN_STATUS_ERROR(Corruption, "invalid docid %d for update inverted index", docId);
}

Status PatchInvertedIndexModifier::Close()
{
    for (auto& patchWriter : _patchWriters) {
        auto status = patchWriter->Close();
        RETURN_STATUS_DIRECTLY_IF_ERROR(status);
    }
    return Status::OK();
}

} // namespace indexlib::index
