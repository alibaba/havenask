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

#include "autil/legacy/jsonizable.h"
#include "indexlib/base/Types.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/common.h"
#include "indexlib/misc/log.h"

namespace indexlib { namespace table {

class TableMergePlan : public autil::legacy::Jsonizable
{
public:
    TableMergePlan();
    virtual ~TableMergePlan();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void AddSegment(segmentid_t segmentId, bool reserveInNewVersion = false);
    const std::map<segmentid_t, bool>& GetInPlanSegments() const;

public:
    typedef std::map<segmentid_t, bool> InPlanSegmentAttributes;
    typedef InPlanSegmentAttributes::value_type InPlanSegmentAttribute;

private:
    InPlanSegmentAttributes mInPlanSegAttrs;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TableMergePlan);
}} // namespace indexlib::table
