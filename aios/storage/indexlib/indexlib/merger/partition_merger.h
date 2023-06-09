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
#ifndef __INDEXLIB_PARTITION_MERGER_H
#define __INDEXLIB_PARTITION_MERGER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace merger {

class PartitionMerger
{
public:
    PartitionMerger();
    virtual ~PartitionMerger();

public:
    virtual bool Merge(bool optimize = false, int64_t currentTs = 0) = 0;
    virtual index_base::Version GetMergedVersion() const = 0;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PartitionMerger);
}} // namespace indexlib::merger

#endif //__INDEXLIB_PARTITION_MERGER_H
