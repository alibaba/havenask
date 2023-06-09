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
#include "indexlib/table/normal_table/index_task/NormalTableResourceCreator.h"

#include "indexlib/index/DocMapper.h"
#include "indexlib/index/inverted_index/truncate/BucketMap.h"
#include "indexlib/table/index_task/IndexTaskConstant.h"
#include "indexlib/table/index_task/merger/MergePlan.h"
#include "indexlib/table/normal_table/index_task/ReclaimMap.h"
#include "indexlib/table/normal_table/index_task/SingleSegmentDocumentGroupSelector.h"
#include "indexlib/table/normal_table/index_task/SortedReclaimMap.h"

using namespace std;

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, NormalTableResourceCreator);

NormalTableResourceCreator::NormalTableResourceCreator() {}

NormalTableResourceCreator::~NormalTableResourceCreator() {}

std::unique_ptr<framework::IndexTaskResource>
NormalTableResourceCreator::CreateResource(const std::string& name, const framework::IndexTaskResourceType& type)
{
    if (type == MERGE_PLAN) {
        return std::make_unique<MergePlan>(name, type);
    } else if (type == index::DocMapper::GetDocMapperType()) {
        if (IsSortedReclaimMapName(name)) {
            return std::make_unique<SortedReclaimMap>(name, type);
        } else {
            return std::make_unique<ReclaimMap>(name, type);
        }
    } else if (type == GroupSelectResource::RESOURCE_TYPE) {
        return std::make_unique<GroupSelectResource>(name, type);
    } else if (type == indexlib::index::BucketMap::GetBucketMapType()) {
        return std::make_unique<indexlib::index::BucketMap>(name, type);
    } else {
        return nullptr;
    }
}

std::string NormalTableResourceCreator::GetReclaimMapName(size_t segmentMergePlanIdx, bool isSorted)
{
    std::string result;
    if (isSorted) {
        result = "Sorted_";
    }
    return result + std::string("ReclaimMap_") + autil::StringUtil::toString(segmentMergePlanIdx);
}

bool NormalTableResourceCreator::IsSortedReclaimMapName(const std::string& reclaimMapName)
{
    std::vector<std::string> splitValues;
    autil::StringUtil::fromString(reclaimMapName, splitValues, "_");
    if (splitValues.size() == 3 && splitValues[0] == "Sorted") {
        return true;
    }
    return false;
}

} // namespace indexlibv2::table
