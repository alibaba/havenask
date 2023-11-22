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

#include <map>
#include <memory>
#include <string>

#include "indexlib/common_define.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(file_system, Directory);

namespace indexlib { namespace index {

// for merger interface
class SegmentDirectoryBase
{
public:
    SegmentDirectoryBase() {}
    SegmentDirectoryBase(const index_base::Version& version) : mVersion(version) {}
    virtual ~SegmentDirectoryBase() {}

public:
    const index_base::Version& GetVersion() const { return mVersion; }

    const index_base::PartitionDataPtr& GetPartitionData() { return mPartitionData; }

    void SetPartitionData(const index_base::PartitionDataPtr& partitionData) { mPartitionData = partitionData; }

protected:
    index_base::Version mVersion;
    index_base::PartitionDataPtr mPartitionData;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SegmentDirectoryBase);
}} // namespace indexlib::index
