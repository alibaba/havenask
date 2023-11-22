#pragma once

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
