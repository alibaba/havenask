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
#ifndef __INDEXLIB_TABLE_MERGE_PLAN_META_H
#define __INDEXLIB_TABLE_MERGE_PLAN_META_H

#include <memory>

#include "autil/legacy/jsonizable.h"
#include "indexlib/common_define.h"
#include "indexlib/document/locator.h"
#include "indexlib/indexlib.h"
#include "indexlib/table/table_common.h"

namespace indexlib { namespace table {

class TableMergePlanMeta : public autil::legacy::Jsonizable
{
public:
    TableMergePlanMeta();
    ~TableMergePlanMeta();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

public:
    document::Locator locator;
    int64_t timestamp;
    uint32_t maxTTL;
    segmentid_t targetSegmentId;
    MergeSegmentDescription segmentDescription;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TableMergePlanMeta);
}} // namespace indexlib::table

#endif //__INDEXLIB_TABLE_MERGE_PLAN_META_H
