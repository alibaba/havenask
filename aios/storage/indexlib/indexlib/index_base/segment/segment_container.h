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
#pragma once

#include <memory>

#include "autil/WorkItem.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(common, DumpItem);

namespace indexlib { namespace index_base {

class SegmentContainer
{
public:
    SegmentContainer() {}

    virtual ~SegmentContainer() {}

public:
    virtual void CreateDumpItems(const file_system::DirectoryPtr& directory,
                                 std::vector<std::unique_ptr<common::DumpItem>>& dumpItems) = 0;

    virtual size_t GetTotalMemoryUse() const = 0;
};

DEFINE_SHARED_PTR(SegmentContainer);
}} // namespace indexlib::index_base
