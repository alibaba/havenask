#include "indexlib/test/partition_metrics.h"

#include "autil/StringUtil.h"
#include "indexlib/file_system/FileSystemMetrics.h"
#include "indexlib/index/partition_info.h"

using namespace std;
using namespace autil;
using namespace indexlib::index;
using namespace indexlib::file_system;
using namespace indexlib::partition;

namespace indexlib { namespace test {
IE_LOG_SETUP(test, PartitionMetrics);

const string PartitionMetrics::RECLAIMED_SLICE_COUNT = "ReclaimedSliceCount";
const string PartitionMetrics::CURRENT_READER_RECLAIMED_BYTES = "CurReaderReclaimableBytes";
const string PartitionMetrics::VAR_ATTRIBUTE_WASTED_BYTES = "VarAttributeWastedBytes";
const string PartitionMetrics::PARTITION_DOC_COUNT = "PartitionDocCount";

PartitionMetrics::PartitionMetrics() {}

PartitionMetrics::~PartitionMetrics() {}

void PartitionMetrics::Init(const IndexPartitionPtr& indexPartition)
{
    mOnlinePartition = DYNAMIC_POINTER_CAST(OnlinePartition, indexPartition);
    assert(mOnlinePartition);
}

string PartitionMetrics::ToString()
{
    stringstream ss;
    CollectAttributeMetrics(ss);
    CollectPartitionMetrics(ss);
    return ss.str();
}

void PartitionMetrics::CollectPartitionMetrics(stringstream& ss)
{
    PartitionInfoPtr partitionInfo = mOnlinePartition->GetReader()->GetPartitionInfo();
    ss << "," << PARTITION_DOC_COUNT << ":" << partitionInfo->GetTotalDocCount();
}

void PartitionMetrics::CollectAttributeMetrics(stringstream& ss)
{
    auto metrics = mOnlinePartition->GetPartitionMetrics();
    ss << RECLAIMED_SLICE_COUNT << ":" << metrics->GetAttributeMetrics()->GetReclaimedSliceCountValue() << ","
       << CURRENT_READER_RECLAIMED_BYTES << ":" << metrics->GetAttributeMetrics()->GetCurReaderReclaimableBytesValue()
       << "," << VAR_ATTRIBUTE_WASTED_BYTES << ":" << metrics->GetAttributeMetrics()->GetVarAttributeWastedBytesValue();
}

PartitionMetrics::StringMap PartitionMetrics::ToMap(const string& metricsString)
{
    StringMap map;
    vector<vector<string>> metricsKVPair;
    StringUtil::fromString(metricsString, metricsKVPair, ":", ",");
    for (size_t i = 0; i < metricsKVPair.size(); i++) {
        assert(metricsKVPair[i].size() == 2);
        map[metricsKVPair[i][0]] = metricsKVPair[i][1];
    }
    return map;
}

bool PartitionMetrics::CheckMetrics(const std::string& expectMetricsStr)
{
    string currentMetricsStr = ToString();
    StringMap currentMetrics = ToMap(currentMetricsStr);
    StringMap expectMetrics = ToMap(expectMetricsStr);

    for (StringMap::iterator it = expectMetrics.begin(); it != expectMetrics.end(); it++) {
        StringMap::iterator curIt = currentMetrics.find(it->first);
        if (curIt == currentMetrics.end()) {
            IE_LOG(ERROR, "can not find metrics [%s]", it->first.c_str());
            return false;
        }

        if (curIt->second != it->second) {
            IE_LOG(ERROR, "metrics [%s] value not equal, expect [%s], actual [%s]", it->first.c_str(),
                   it->second.c_str(), curIt->second.c_str());
            return false;
        }
    }
    return true;
}
}} // namespace indexlib::test
