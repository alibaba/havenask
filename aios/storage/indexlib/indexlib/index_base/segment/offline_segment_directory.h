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
#include "indexlib/config/configurator_define.h"
#include "indexlib/index_base/segment/segment_directory.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index_base {

class OfflineSegmentDirectory : public SegmentDirectory
{
public:
    OfflineSegmentDirectory(bool enableRecover = false,
                            config::RecoverStrategyType recoverType = config::RecoverStrategyType::RST_SEGMENT_LEVEL);
    OfflineSegmentDirectory(const OfflineSegmentDirectory& other);
    ~OfflineSegmentDirectory();

public:
protected:
    void DoInit(const file_system::DirectoryPtr& directory) override;

protected:
    bool mEnableRecover;
    config::RecoverStrategyType mRecoverType;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OfflineSegmentDirectory);
}} // namespace indexlib::index_base
