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
#include "indexlib/table/normal_table/index_task/SingleSegmentDocumentGroupSelector.h"

#include "indexlib/config/ITabletSchema.h"
#include "indexlib/config/MutableJson.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/index_task/IndexTaskResourceManager.h"
#include "indexlib/index/attribute/expression/DocumentEvaluator.h"
#include "indexlib/index/attribute/expression/DocumentEvaluatorMaintainer.h"
#include "indexlib/table/normal_table/Common.h"
#include "indexlib/table/normal_table/config/SegmentGroupConfig.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, SingleSegmentDocumentGroupSelector);
AUTIL_LOG_SETUP(indexlib.table, GroupSelectResource);

static std::string GetResourceName(segmentid_t segmentId)
{
    return "group_select_resource_segment_" + std::to_string(segmentId);
}

SingleSegmentDocumentGroupSelector::SingleSegmentDocumentGroupSelector() : _segmentId(INVALID_SEGMENTID) {}

SingleSegmentDocumentGroupSelector::~SingleSegmentDocumentGroupSelector() {}

Status
SingleSegmentDocumentGroupSelector::Init(segmentid_t segmentId,
                                         const std::shared_ptr<framework::IndexTaskResourceManager>& resourceManager)
{
    _segmentId = segmentId;
    auto resourceName = GetResourceName(_segmentId);
    std::shared_ptr<GroupSelectResource> resource;
    RETURN_IF_STATUS_ERROR(resourceManager->LoadResource(resourceName, GroupSelectResource::RESOURCE_TYPE, resource),
                           "load resource [%s] failed", resourceName.c_str());
    _selectInfo = resource->GetSelectInfo();
    return Status::OK();
}
Status
SingleSegmentDocumentGroupSelector::Dump(const std::shared_ptr<framework::IndexTaskResourceManager>& resourceManager)
{
    if (_segmentId == INVALID_SEGMENTID) {
        AUTIL_LOG(ERROR, "need init before dump");
        return Status::InvalidArgs("need init before dump");
    }
    auto resourceName = GetResourceName(_segmentId);
    std::shared_ptr<GroupSelectResource> resource;
    RETURN_IF_STATUS_ERROR(resourceManager->CreateResource(resourceName, GroupSelectResource::RESOURCE_TYPE, resource),
                           "create resource [%s] failed", resourceName.c_str());
    resource->SetSelectInfo(_selectInfo);
    RETURN_IF_STATUS_ERROR(resourceManager->CommitResource(resourceName), "commit resource [%s] failed",
                           resourceName.c_str());
    AUTIL_LOG(INFO, "commit resource [%s] success", resourceName.c_str());
    return Status::OK();
}

Status SingleSegmentDocumentGroupSelector::Init(const std::shared_ptr<framework::Segment>& segment,
                                                const std::shared_ptr<config::ITabletSchema>& schema)
{
    AUTIL_LOG(INFO, "init SingleSegmentDocumentGroupSelector for segment [%d] begin", segment->GetSegmentId());
    _segmentId = segment->GetSegmentId();
    _selectInfo.reset(new std::vector<int32_t>(segment->GetSegmentInfo()->docCount, -1));
    _evaluatorMatainer.reset(new index::DocumentEvaluatorMaintainer);
    std::vector<std::shared_ptr<framework::Segment>> segments({segment});

    auto [status, segmentGroupConfig] =
        schema->GetRuntimeSettings().GetValue<SegmentGroupConfig>(NORMAL_TABLE_GROUP_CONFIG_KEY);
    RETURN_IF_STATUS_ERROR(status, "get group config failed");
    auto functionNames = segmentGroupConfig.GetFunctionNames();
    assert(!functionNames.empty());
    RETURN_IF_STATUS_ERROR(_evaluatorMatainer->Init(segments, schema, functionNames), "init evaluator matainer failed");
    auto evaluators = _evaluatorMatainer->GetAllEvaluators();
    assert(evaluators.size() == functionNames.size());
    for (size_t i = 0; i < functionNames.size(); ++i) {
        auto evaluator = evaluators[i];
        auto evaluatorTyped = std::dynamic_pointer_cast<index::DocumentEvaluator<bool>>(evaluator);
        if (!evaluatorTyped) {
            AUTIL_LOG(ERROR, "cast document evaluator for function name [%s] failed", functionNames[i].c_str());
            return Status::InvalidArgs("create evaluator failed");
        }
        _evaluators.push_back(evaluatorTyped);
    }
    AUTIL_LOG(INFO, "init SingleSegmentDocumentGroupSelector for segment [%d] success", segment->GetSegmentId());
    return Status::OK();
}

std::pair<Status, int32_t> SingleSegmentDocumentGroupSelector::Select(docid_t inSegmentDocId)
{
    if (_selectInfo->size() <= inSegmentDocId) {
        AUTIL_LOG(ERROR, "doc count is [%lu] but select doc id [%d]", _selectInfo->size(), inSegmentDocId);
        return {Status::InvalidArgs("invalid read"), -1};
    }
    int32_t cachedResult = (*_selectInfo)[inSegmentDocId];
    if (cachedResult != -1) {
        return {Status::OK(), cachedResult};
    }
    for (int32_t i = 0; i < (int32_t)_evaluators.size(); ++i) {
        auto& evaluator = _evaluators[i];
        auto [status, match] = evaluator->Evaluate(inSegmentDocId);
        RETURN2_IF_STATUS_ERROR(status, -1, "select group for local doc id [%d] failed", inSegmentDocId);
        if (match) {
            (*_selectInfo)[inSegmentDocId] = i;
            return {Status::OK(), i};
        }
    }
    AUTIL_LOG(ERROR, "no match group for document, but expected default group");
    assert(false);
    return {Status::Corruption(), -1};
}

const framework::IndexTaskResourceType GroupSelectResource::RESOURCE_TYPE = "normal_table_group_select_resource_name";

GroupSelectResource::GroupSelectResource(std::string name, framework::IndexTaskResourceType type)
    : IndexTaskResource(name, type)
{
}
GroupSelectResource::~GroupSelectResource() {}
Status GroupSelectResource::Store(const std::shared_ptr<indexlib::file_system::Directory>& resourceDirectory)
{
    auto [writerStatus, writer] = resourceDirectory->GetIDirectory()
                                      ->CreateFileWriter(_name, indexlib::file_system::WriterOption::BufferAtomicDump())
                                      .StatusWith();
    RETURN_IF_STATUS_ERROR(writerStatus, "create file writer [%s] failed", _name.c_str());
    int32_t docCount = _selectInfo->size();
    auto [status, _] = writer->Write(&docCount, sizeof(docCount)).StatusWith();
    RETURN_IF_STATUS_ERROR(status, "write size failed");
    if (docCount == 0) {
        return Status::OK();
    }
    auto [status2, __] = writer->Write(_selectInfo->data(), sizeof(int32_t) * docCount).StatusWith();
    RETURN_IF_STATUS_ERROR(status2, "write select info failed");
    RETURN_IF_STATUS_ERROR(writer->Close().Status(), "close file writer failed");
    return Status::OK();
}
Status GroupSelectResource::Load(const std::shared_ptr<indexlib::file_system::Directory>& resourceDirectory)
{
    auto [readerStatus, reader] = resourceDirectory->GetIDirectory()
                                      ->CreateFileReader(_name, indexlib::file_system::FSOpenType::FSOT_BUFFERED)
                                      .StatusWith();
    RETURN_IF_STATUS_ERROR(readerStatus, "create filereader [%s] failed", _name.c_str());
    int32_t docCount = 0;
    auto [status, _] = reader->Read(&docCount, sizeof(docCount)).StatusWith();
    RETURN_IF_STATUS_ERROR(status, "read doc count failed");
    _selectInfo.reset(new std::vector<int32_t>());
    if (docCount == 0) {
        return Status::OK();
    }
    _selectInfo->resize(docCount);
    auto [status2, __] = reader->Read(_selectInfo->data(), docCount * sizeof(int32_t)).StatusWith();
    RETURN_IF_STATUS_ERROR(status2, "read select info failed");
    return Status::OK();
}

void GroupSelectResource::SetSelectInfo(const std::shared_ptr<std::vector<int32_t>>& selectInfo)
{
    _selectInfo = selectInfo;
}
const std::shared_ptr<std::vector<int32_t>>& GroupSelectResource::GetSelectInfo() const { return _selectInfo; }

} // namespace indexlibv2::table
