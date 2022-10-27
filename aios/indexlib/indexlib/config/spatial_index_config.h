#ifndef __INDEXLIB_SPATIAL_INDEX_CONFIG_H
#define __INDEXLIB_SPATIAL_INDEX_CONFIG_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/config/single_field_index_config.h"

DECLARE_REFERENCE_CLASS(config, SpatialIndexConfigImpl);

IE_NAMESPACE_BEGIN(config);

//TODO: add test
class SpatialIndexConfig : public SingleFieldIndexConfig
{
public:
    enum DistCalculator{
        HAVERSINE,
        UNKNOW
    };
    static const double DEFAULT_DIST_LOSS_ACCURACY;

public:
    SpatialIndexConfig(const std::string& indexName, IndexType indexType);
    SpatialIndexConfig(const SpatialIndexConfig& other);
    ~SpatialIndexConfig();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void AssertEqual(const IndexConfig& other) const override;
    void AssertCompatible(const IndexConfig& other) const override;
    void Check() const override;
    IndexConfig* Clone() const override;

    double GetMaxSearchDist();
    double GetMaxDistError();
    double GetDistanceLoss();
    DistCalculator GetDistCalculator();

public:
    //for test
    void SetMaxSearchDist(double dist);
    void SetMaxDistError(double dist);
    void SetDistanceLoss(double distLoss);
    
private:
    SpatialIndexConfigImpl* mImpl;
private:
    friend class SpatialIndexConfigTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SpatialIndexConfig);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_SPATIAL_INDEX_CONFIG_H
