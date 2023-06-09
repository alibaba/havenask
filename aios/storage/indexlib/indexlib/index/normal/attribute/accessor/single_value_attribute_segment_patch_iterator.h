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
#ifndef __INDEXLIB_SINGLE_VALUE_ATTRIBUTE_SEGMENT_PATCH_ITERATOR_H
#define __INDEXLIB_SINGLE_VALUE_ATTRIBUTE_SEGMENT_PATCH_ITERATOR_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_patch_reader.h"
#include "indexlib/index_base/patch/patch_file_finder.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

template <typename T>
class SingleValueAttributeSegmentPatchIterator : public SingleValueAttributePatchReader<T>
{
private:
    using SingleValueAttributePatchReader<T>::Next;

public:
    SingleValueAttributeSegmentPatchIterator(const config::AttributeConfigPtr& config)
        : SingleValueAttributePatchReader<T>(config)
    {
    }
    ~SingleValueAttributeSegmentPatchIterator() {}

public:
    void TEST_Init(const file_system::DirectoryPtr& directory,
                   const std::vector<std::pair<std::string, segmentid_t>>& patchFileList);
    bool Next(docid_t& docId, T& value, bool& isNull);

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP_TEMPLATE(index, SingleValueAttributeSegmentPatchIterator);

template <typename T>
void SingleValueAttributeSegmentPatchIterator<T>::TEST_Init(
    const file_system::DirectoryPtr& directory, const std::vector<std::pair<std::string, segmentid_t>>& patchFileList)
{
    for (size_t i = 0; i < patchFileList.size(); ++i) {
        this->AddPatchFile(directory, patchFileList[i].first, patchFileList[i].second);
    }
}

template <typename T>
bool SingleValueAttributeSegmentPatchIterator<T>::Next(docid_t& docId, T& value, bool& isNull)
{
    if (!this->HasNext()) {
        return false;
    }

    SinglePatchFile* patchFile = this->mPatchHeap.top();
    docId = patchFile->GetCurDocId();
    return this->InnerReadPatch(docId, value, isNull);
}
}} // namespace indexlib::index

#endif //__INDEXLIB_SINGLE_VALUE_ATTRIBUTE_SEGMENT_PATCH_ITERATOR_H
