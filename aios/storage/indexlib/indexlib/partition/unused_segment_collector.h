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

#include "fslib/common/common_type.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/reader_container.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);

namespace indexlib { namespace partition {

class UnusedSegmentCollector
{
public:
    UnusedSegmentCollector();
    ~UnusedSegmentCollector();

public:
    static void Collect(const ReaderContainerPtr& readerContainer, const file_system::DirectoryPtr& directory,
                        fslib::FileList& segments);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(UnusedSegmentCollector);
}} // namespace indexlib::partition
