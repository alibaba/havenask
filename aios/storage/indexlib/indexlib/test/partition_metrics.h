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
#ifndef __INDEXLIB_METRICS_RESULT_H
#define __INDEXLIB_METRICS_RESULT_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/online_partition.h"

namespace indexlib { namespace test {

class PartitionMetrics
{
public:
    PartitionMetrics();
    ~PartitionMetrics();

public:
    static const std::string RECLAIMED_SLICE_COUNT;
    static const std::string CURRENT_READER_RECLAIMED_BYTES;
    static const std::string VAR_ATTRIBUTE_WASTED_BYTES;
    static const std::string PARTITION_DOC_COUNT;

public:
    void Init(const partition::IndexPartitionPtr& indexPartition);
    // metrics_name:value,metrics_name:value.....
    std::string ToString();
    bool CheckMetrics(const std::string& expectMetrics);

private:
    typedef std::map<std::string, std::string> StringMap;
    StringMap ToMap(const std::string& metricsStr);
    void CollectAttributeMetrics(std::stringstream& ss);
    void CollectPartitionMetrics(std::stringstream& ss);

private:
    partition::OnlinePartitionPtr mOnlinePartition;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PartitionMetrics);
}} // namespace indexlib::test

#endif //__INDEXLIB_METRICS_RESULT_H
