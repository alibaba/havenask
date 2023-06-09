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
#ifndef __INDEXLIB_PROGRESS_SYNCHRONIZER_H
#define __INDEXLIB_PROGRESS_SYNCHRONIZER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/document/locator.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(index_base, Version);
DECLARE_REFERENCE_STRUCT(index_base, SegmentInfo);

namespace indexlib { namespace index_base {

class ProgressSynchronizer
{
public:
    ProgressSynchronizer();
    ~ProgressSynchronizer();

public:
    // max ts & latest src with min offset locator
    void Init(const std::vector<Version>& versions);
    void Init(const std::vector<SegmentInfo>& segInfos);

    int64_t GetTimestamp() const { return mTimestamp; }
    const document::Locator GetLocator() const { return mLocator; }
    uint32_t GetFormatVersion() const { return mFormatVersion; }

private:
    int64_t mTimestamp;
    document::Locator mLocator;
    uint32_t mFormatVersion;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ProgressSynchronizer);
}} // namespace indexlib::index_base

#endif //__INDEXLIB_PROGRESS_SYNCHRONIZER_H
