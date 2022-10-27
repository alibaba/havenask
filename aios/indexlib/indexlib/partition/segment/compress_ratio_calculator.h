#ifndef __INDEXLIB_COMPRESS_RATIO_CALCULATOR_H
#define __INDEXLIB_COMPRESS_RATIO_CALCULATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index_base/segment/segment_data.h"

DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(config, KVIndexConfig);
DECLARE_REFERENCE_CLASS(config, KKVIndexConfig);

IE_NAMESPACE_BEGIN(partition);

class CompressRatioCalculator
{
public:
    CompressRatioCalculator();
    ~CompressRatioCalculator();

public:
    void Init(const index_base::PartitionDataPtr& partData,
              const config::IndexPartitionSchemaPtr& schema);

    double GetCompressRatio(const std::string& fileName) const;

private:
    void GetSegmentDatas(const index_base::PartitionDataPtr& partData,
                         index_base::SegmentDataVector& segDataVec);

    double GetRatio(const index_base::SegmentDataVector& segDataVec,
                    const std::string& filePath);

private:
    // first: FileName, second: ratio
    typedef std::map<std::string, double> RatioMap;
    RatioMap mRatioMap;

private:
    static constexpr double DEFAULT_COMPRESS_RATIO = 1.0;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(CompressRatioCalculator);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_COMPRESS_RATIO_CALCULATOR_H
