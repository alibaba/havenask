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
#ifndef ISEARCH_BS_CUSTOMMERGER_H
#define ISEARCH_BS_CUSTOMMERGER_H

#include "build_service/common_define.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/custom_merger/CustomMergeMeta.h"
#include "build_service/custom_merger/CustomMergerTaskItem.h"
#include "build_service/util/Log.h"
#include "indexlib/util/metrics/MetricProvider.h"

namespace build_service { namespace custom_merger {

class MergeResourceProvider;
BS_TYPEDEF_PTR(MergeResourceProvider);

class CustomMerger
{
public:
    CustomMerger() {}
    virtual ~CustomMerger() {}

private:
    CustomMerger(const CustomMerger&);
    CustomMerger& operator=(const CustomMerger&);

public:
    struct CustomMergerInitParam {
        KeyValueMap parameters;
        MergeResourceProviderPtr resourceProvider;
        indexlib::util::MetricProviderPtr metricProvider;
        config::ResourceReader* resourceReader;
    };

    virtual bool init(CustomMergerInitParam& param) = 0;
    virtual bool merge(const CustomMergeMeta& mergeMeta, size_t instanceId, const std::string& path) = 0;
    virtual bool endMerge(const CustomMergeMeta& mergeMeta, const std::string& path, int32_t targetVersionId) = 0;
    virtual bool estimateCost(std::vector<CustomMergerTaskItem>& taskItems) = 0;

    virtual MergeResourceProviderPtr getResourceProvider() const = 0;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(CustomMerger);
//////////////////////////////////////////////////////

}} // namespace build_service::custom_merger

#endif // ISEARCH_BS_CUSTOMMERGER_H
