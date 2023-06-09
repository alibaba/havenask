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
#include "indexlib/table/normal_table/index_task/AttributeIndexMergeOperation.h"

#include "indexlib/base/PathUtil.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/index/IIndexMerger.h"
#include "indexlib/index/attribute/Common.h"
#include "indexlib/index/attribute/Constant.h"
#include "indexlib/index/attribute/merger/AttributeMerger.h"
#include "indexlib/index/attribute/patch/AttributePatchFileFinder.h"
#include "indexlib/table/index_task/IndexTaskConstant.h"
#include "indexlib/table/normal_table/index_task/Common.h"
#include "indexlib/table/normal_table/index_task/PatchFileFinderUtil.h"

namespace indexlibv2 { namespace table {
AUTIL_LOG_SETUP(indexlib.table, AttributeIndexMergeOperation);

AttributeIndexMergeOperation::AttributeIndexMergeOperation(const framework::IndexOperationDescription& opDesc)
    : IndexMergeOperation(opDesc)
{
}

std::pair<Status, std::shared_ptr<config::IIndexConfig>>
AttributeIndexMergeOperation::GetIndexConfig(const framework::IndexOperationDescription& opDesc,
                                             const std::string& indexType, const std::string& indexName,
                                             const std::shared_ptr<config::TabletSchema>& schema)
{
    auto indexConfig = schema->GetIndexConfig(indexType, indexName);
    if (!indexConfig) {
        AUTIL_LOG(ERROR, "get index config failed. index type [%s], index name [%s]", indexType.c_str(),
                  indexName.c_str());
        assert(false);
        return std::make_pair(Status::Corruption("get index config failed"), nullptr);
    }

    std::shared_ptr<indexlibv2::config::AttributeConfig> attributeConfig =
        std::dynamic_pointer_cast<indexlibv2::config::AttributeConfig>(indexConfig);

    if (attributeConfig == nullptr) {
        auto status = Status::Corruption("Invalid attribute config encountered [%s]", indexName.c_str());
        AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
        return std::make_pair(status, nullptr);
    }
    int64_t sliceCount = 0;
    int64_t sliceIdx = 0;
    if (opDesc.GetParameter(index::ATTRIBUTE_SLICE_COUNT, sliceCount) &&
        opDesc.GetParameter(index::ATTRIBUTE_SLICE_IDX, sliceIdx)) {
        assert(sliceCount > 0);
        assert(sliceIdx >= 0);
        assert(sliceIdx < sliceCount);
        auto sliceAttributeConfigs = attributeConfig->CreateSliceAttributeConfigs(sliceCount);
        return std::make_pair(Status::OK(), sliceAttributeConfigs[sliceIdx]);
    }
    return std::make_pair(Status::OK(), indexConfig);
}

Status AttributeIndexMergeOperation::Execute(const framework::IndexTaskContext& context)
{
    try {
        auto [status1, segmentMergeInfos] = PrepareSegmentMergeInfos(context, true);
        RETURN_STATUS_DIRECTLY_IF_ERROR(status1);

        auto resourceManager = context.GetResourceManager();

        auto [status2, opLog2PatchOpRootDir] = PatchFileFinderUtil::GetOpLog2PatchRootDir(_desc, context);
        RETURN_STATUS_DIRECTLY_IF_ERROR(status2);
        index::AttributePatchFileFinder attrPatchFinder;
        RETURN_STATUS_DIRECTLY_IF_ERROR(PatchFileFinderUtil::FindPatchInfos(
            &attrPatchFinder, context.GetTabletData(), _indexConfig, opLog2PatchOpRootDir, &_allPatchInfos));
        auto attributeMerger = std::dynamic_pointer_cast<index::AttributeMerger>(_indexMerger);
        assert(attributeMerger != nullptr);
        attributeMerger->SetPatchInfos(_allPatchInfos);

        return _indexMerger->Merge(segmentMergeInfos, resourceManager);
    } catch (const autil::legacy::ExceptionBase& e) {
        auto status = Status::InternalError("attribute merge failed, exception [%s]", e.what());
        AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
        return status;
    }
}

}} // namespace indexlibv2::table
