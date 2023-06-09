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
#ifndef __INDEXLIB_MERGE_META_H
#define __INDEXLIB_MERGE_META_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/file_system/fslib/FenceContext.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace merger {

class MergeMeta
{
public:
    MergeMeta();
    virtual ~MergeMeta();

public:
    virtual void Store(const std::string& mergeMetaPath,
                       file_system::FenceContext* fenceContext = file_system::FenceContext::NoFence()) const = 0;
    virtual bool Load(const std::string& mergeMetaPath) = 0;
    virtual bool LoadBasicInfo(const std::string& mergeMetaPath) = 0;
    virtual size_t GetMergePlanCount() const = 0;
    virtual std::vector<segmentid_t> GetTargetSegmentIds(size_t planIdx) const = 0;
    virtual int64_t EstimateMemoryUse() const = 0;

public:
    const index_base::Version& GetTargetVersion() const { return mTargetVersion; }
    void SetTargetVersion(const index_base::Version& targetVersion) { mTargetVersion = targetVersion; }

protected:
    index_base::Version mTargetVersion;
};

DEFINE_SHARED_PTR(MergeMeta);
}} // namespace indexlib::merger

#endif //__INDEXLIB_MERGE_META_H
