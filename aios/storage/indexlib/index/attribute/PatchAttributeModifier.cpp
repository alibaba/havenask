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
#include "indexlib/index/attribute/PatchAttributeModifier.h"

#include "indexlib/framework/Segment.h"
#include "indexlib/index/attribute/Common.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, PatchAttributeModifier);

PatchAttributeModifier::PatchAttributeModifier(const std::shared_ptr<config::ITabletSchema>& schema,
                                               const std::shared_ptr<indexlib::file_system::IDirectory>& workDir)
    : AttributeModifier(schema)
    , _workDir(workDir)
{
}

Status PatchAttributeModifier::Init(const framework::TabletData& tabletData)
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

    auto [status, attributeWorkDir] =
        _workDir->MakeDirectory(ATTRIBUTE_INDEX_PATH, indexlib::file_system::DirectoryOption()).StatusWith();

    RETURN_IF_STATUS_ERROR(status, "create attribute patch dir [%s] failed", _workDir->DebugString().c_str());

    for (const auto& indexConfig : _schema->GetIndexConfigs(ATTRIBUTE_INDEX_TYPE_STR)) {
        auto attrConfig = std::dynamic_pointer_cast<AttributeConfig>(indexConfig);
        assert(attrConfig);
        if (attrConfig->IsAttributeUpdatable()) {
            auto patchWriter = std::make_shared<AttributePatchWriter>(attributeWorkDir, lastSegmentId, indexConfig);
            _patchWriters[attrConfig->GetFieldId()] = patchWriter;
        }
    }
    return Status::OK();
}

bool PatchAttributeModifier::UpdateField(docid_t docId, fieldid_t fieldId, const autil::StringView& value, bool isNull)
{
    assert(docId < _maxDocCount);
    auto iter = _patchWriters.find(fieldId);
    if (iter == _patchWriters.end()) {
        return false;
    }
    auto patchWriter = iter->second;
    auto fieldConfig = _schema->GetFieldConfig(fieldId);
    for (auto [segmentId, baseDocId, endDocId] : _segmentInfos) {
        if (docId >= baseDocId && docId < endDocId) {
            auto status = patchWriter->Write(segmentId, docId - baseDocId, value, isNull);
            if (!status.IsOK()) {
                AUTIL_LOG(ERROR, "update attribute fieldId [%d] failed", fieldId);
                return false;
            }
            return true;
        }
    }
    return false;
}

Status PatchAttributeModifier::Close()
{
    for (auto& [fieldId, patchWriter] : _patchWriters) {
        auto status = patchWriter->Close();
        RETURN_IF_STATUS_ERROR(status, "field [%d] patch writer close failed", fieldId);
    }
    return Status::OK();
}

} // namespace indexlibv2::index
