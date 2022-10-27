#ifndef __INDEXLIB_SPATIAL_INDEX_CONFIG_IMPL_H
#define __INDEXLIB_SPATIAL_INDEX_CONFIG_IMPL_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/config/single_field_index_config.h"
#include "indexlib/config/impl/single_field_index_config_impl.h"
#include "indexlib/config/spatial_index_config.h"
#include "indexlib/config/impl/spatial_index_config_impl.h"

IE_NAMESPACE_BEGIN(config);

class SpatialIndexConfigImpl : public SingleFieldIndexConfigImpl
{
private:
    static const double DEFAULT_MAX_DIST_ERROR_VALUE;
    static const double DEFAULT_MAX_SEARCH_DIST_VALUE;
    static const double MIN_MAX_DIST_ERROR_VALUE;

public:
    SpatialIndexConfigImpl(const std::string& indexName, IndexType indexType);
    SpatialIndexConfigImpl(const SpatialIndexConfigImpl& other);
    ~SpatialIndexConfigImpl();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void AssertEqual(const IndexConfigImpl& other) const override;
    void AssertCompatible(const IndexConfigImpl& other) const override
    { AssertEqual(other); }
    void Check() const override;
    IndexConfigImpl* Clone() const override;

    double GetMaxSearchDist() { return mMaxSearchDist; }
    double GetMaxDistError() { return mMaxDistError; }
    double GetDistanceLoss() { return mDistanceLoss; }
    SpatialIndexConfig::DistCalculator GetDistCalculator() { return mCalculator; }

public:
    //for test
    void SetMaxSearchDist(double dist) { mMaxSearchDist = dist; }
    void SetMaxDistError(double dist) { mMaxDistError = dist; }
    void SetDistanceLoss(double distLoss) { mDistanceLoss = distLoss; }
    
private:
    std::string CalculatorToStr(SpatialIndexConfig::DistCalculator calculator);
    SpatialIndexConfig::DistCalculator StrToCalculator(const std::string& str);
    bool CheckFieldType(FieldType ft) const override
    { return ft == ft_location || ft == ft_line || ft == ft_polygon; }

private:
    double mMaxSearchDist;
    double mMaxDistError;
    SpatialIndexConfig::DistCalculator mCalculator;
    double mDistanceLoss;
private:
    friend class SpatialIndexConfigTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SpatialIndexConfigImpl);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_SPATIAL_INDEX_CONFIG_IMPL_H
