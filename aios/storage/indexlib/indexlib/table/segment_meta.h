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

#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/segment/segment_data_base.h"
#include "indexlib/misc/common.h"
#include "indexlib/misc/log.h"

// TODO: move SegmentInfo to common namespace
namespace indexlib { namespace table {

class SegmentMeta
{
public:
    enum class SegmentSourceType { INC_BUILD = 0, RT_BUILD = 1, UNKNOWN = -1 };

public:
    SegmentMeta();
    ~SegmentMeta();

public:
    index_base::SegmentDataBase segmentDataBase;
    index_base::SegmentInfo segmentInfo;
    file_system::DirectoryPtr segmentDataDirectory;
    SegmentSourceType type;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SegmentMeta);
}} // namespace indexlib::table
