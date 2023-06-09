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
#include "indexlib/index/normal/attribute/attribute_update_info.h"

#include "indexlib/index_define.h"
#include "indexlib/util/Exception.h"

using namespace autil::legacy::json;
using namespace std;

using namespace indexlib::file_system;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, AttributeUpdateInfo);

void AttributeUpdateInfo::Jsonize(Jsonizable::JsonWrapper& json)
{
    SegmentUpdateInfoVec segUpdateInfos;
    if (json.GetMode() == TO_JSON) {
        Iterator iter = CreateIterator();
        while (iter.HasNext()) {
            segUpdateInfos.push_back(iter.Next());
        }
        json.Jsonize("attribute_update_info", segUpdateInfos);
    } else {
        json.Jsonize("attribute_update_info", segUpdateInfos, segUpdateInfos);
        for (size_t i = 0; i < segUpdateInfos.size(); i++) {
            if (!Add(segUpdateInfos[i])) {
                INDEXLIB_FATAL_ERROR(Duplicate,
                                     "duplicated updateSegId[%d] "
                                     "in attribute update info",
                                     segUpdateInfos[i].updateSegId);
            }
        }
    }
}

void AttributeUpdateInfo::Load(const DirectoryPtr& directory)
{
    assert(directory);
    string content;
    directory->Load(ATTRIBUTE_UPDATE_INFO_FILE_NAME, content);
    FromJsonString(*this, content);
}

bool AttributeUpdateInfo::operator==(const AttributeUpdateInfo& other) const
{
    if (mUpdateMap.size() != other.mUpdateMap.size()) {
        return false;
    }

    Iterator iter1 = CreateIterator();
    Iterator iter2 = other.CreateIterator();

    while (iter1.HasNext()) {
        assert(iter2.HasNext());
        if (iter1.Next() != iter2.Next()) {
            return false;
        }
    }
    return true;
}
}} // namespace indexlib::index
