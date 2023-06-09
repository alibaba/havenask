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
#ifndef __INDEXLIB_TRUNCATE_COLLECT_INFO_PROCESSOR_H
#define __INDEXLIB_TRUNCATE_COLLECT_INFO_PROCESSOR_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/kkv_index_config.h"
#include "indexlib/index/kkv/collect_info_comparator.h"
#include "indexlib/index/kkv/collect_info_processor.h"
#include "indexlib/index/kkv/kkv_comparator.h"
#include "indexlib/index/kkv/kkv_define.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class TruncateCollectInfoProcessor : public CollectInfoProcessor
{
public:
    TruncateCollectInfoProcessor() : mSKeyCountLimits(0), mSkeyFieldType(ft_unknown) {}

    ~TruncateCollectInfoProcessor() {}

public:
    bool Init(const config::KKVIndexConfigPtr& kkvConfig) override;
    uint32_t Process(SKeyCollectInfos& collectInfos, uint32_t validSkeyCount) override;

    void SetSKeyFieldType(FieldType ft) { mSkeyFieldType = ft; }

protected:
    uint32_t mSKeyCountLimits;
    CollectInfoValueComp mCollectComp;
    FieldType mSkeyFieldType;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TruncateCollectInfoProcessor);
}} // namespace indexlib::index

#endif //__INDEXLIB_TRUNCATE_COLLECT_INFO_PROCESSOR_H
