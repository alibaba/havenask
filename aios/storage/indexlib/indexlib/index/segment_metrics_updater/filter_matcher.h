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
#ifndef __INDEXLIB_FILTER_MATCHER_H
#define __INDEXLIB_FILTER_MATCHER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/attribute_schema.h"
#include "indexlib/config/condition_filter.h"
#include "indexlib/document/document.h"
#include "indexlib/index/normal/attribute/accessor/offline_attribute_segment_reader_container.h"
#include "indexlib/index/segment_metrics_updater/match_function.h"

namespace indexlib { namespace index {

class FilterMatcher
{
public:
    FilterMatcher() {};
    virtual ~FilterMatcher() {};

public:
    virtual bool Init(const config::AttributeSchemaPtr& attrSchema, const config::ConditionFilterPtr& conditionFilter,
                      int32_t idx) = 0;
    virtual bool InitForMerge(const config::AttributeSchemaPtr& attrSchema,
                              const config::ConditionFilterPtr& conditionFilter,
                              const index::OfflineAttributeSegmentReaderContainerPtr& attrReaders, int32_t idx) = 0;
    virtual void Match(const document::DocumentPtr& doc, std::vector<int32_t>& matchIds) = 0;
    virtual void Match(segmentid_t segId, docid_t localId, std::vector<int32_t>& matchIds) = 0;
    virtual bool AddValue(const std::string& value, int32_t idx) = 0;

public:
    static constexpr double EPS = 1e-8;

protected:
    std::string mType;
    std::string mFieldName;
    std::unique_ptr<MatchFunction> mFunction;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(FilterMatcher);
}} // namespace indexlib::index

#endif //__INDEXLIB_FILTER_MATCHER_H
