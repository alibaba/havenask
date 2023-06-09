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

#include "indexlib/common_define.h"
#include "indexlib/index/normal/accessor/patch_merger.h"
#include "indexlib/index_base/patch/patch_file_finder.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(file_system, FileWriter);
DECLARE_REFERENCE_CLASS(index, SingleFieldIndexSegmentPatchIterator);

namespace indexlib { namespace index {

class IndexPatchMerger : public PatchMerger
{
public:
    IndexPatchMerger(const config::IndexConfigPtr& indexConfig);
    virtual ~IndexPatchMerger();

public:
    void Merge(const index_base::PatchFileInfoVec& patchFileInfoVec,
               const file_system::FileWriterPtr& destPatchFile) override;

private:
    void DoMerge(std::unique_ptr<SingleFieldIndexSegmentPatchIterator> patchIter,
                 const file_system::FileWriterPtr& destPatchFile);

private:
    config::IndexConfigPtr _indexConfig;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexPatchMerger);
}} // namespace indexlib::index
