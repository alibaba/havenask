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
#ifndef __INDEXLIB_TEMPERATURE_EVALUATOR_H
#define __INDEXLIB_TEMPERATURE_EVALUATOR_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/attribute_schema.h"
#include "indexlib/config/temperature_layer_config.h"
#include "indexlib/document/document.h"
#include "indexlib/index/normal/attribute/accessor/offline_attribute_segment_reader_container.h"
#include "indexlib/index/segment_metrics_updater/filter_matcher.h"
#include "indexlib/index_define.h"

namespace indexlib { namespace index {

class TemperatureEvaluator
{
public:
    TemperatureEvaluator();
    ~TemperatureEvaluator();

public:
    bool Init(const config::AttributeSchemaPtr& attrSchema, const config::TemperatureLayerConfigPtr& config);
    bool InitForMerge(const config::AttributeSchemaPtr& attrSchema, const config::TemperatureLayerConfigPtr& config,
                      const index::OfflineAttributeSegmentReaderContainerPtr& attrReaders);
    TemperatureProperty Evaluate(const document::DocumentPtr& doc);
    TemperatureProperty Evaluate(segmentid_t oldSegId, docid_t oldLocalId);

private:
    std::map<std::string, FilterMatcherPtr> mMatchers;
    std::map<int32_t, std::pair<TemperatureProperty, int32_t>> mMatchInfos; // config idx -> <temperature,conditionNum>
    TemperatureProperty mDefaultProperty;
    std::map<int32_t, int32_t> mCurrentMatchInfo;
    std::vector<int32_t> mMatchIds;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TemperatureEvaluator);
}} // namespace indexlib::index

#endif //__INDEXLIB_TEMPERATURE_EVALUATOR_H
