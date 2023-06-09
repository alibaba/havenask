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
#ifndef __INDEXLIB_PATCH_ITERATOR_H
#define __INDEXLIB_PATCH_ITERATOR_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(index_base, Version);
DECLARE_REFERENCE_CLASS(index, AttrFieldValue);
DECLARE_REFERENCE_CLASS(index, PatchWorkItem);

namespace indexlib { namespace index {

class PatchIterator
{
public:
    PatchIterator() {}
    virtual ~PatchIterator() {}

public:
    virtual bool HasNext() const = 0;
    virtual void Next(AttrFieldValue& value) = 0;
    virtual void Reserve(AttrFieldValue& value) = 0;
    virtual size_t GetPatchLoadExpandSize() const = 0;
    virtual void CreateIndependentPatchWorkItems(std::vector<PatchWorkItem*>& workItems) = 0;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PatchIterator);
}} // namespace indexlib::index

#endif //__INDEXLIB_PATCH_ITERATOR_H
