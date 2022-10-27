#include "indexlib/config/spatial_index_config.h"
#include "indexlib/config/impl/spatial_index_config_impl.h"

using namespace std;

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, SpatialIndexConfig);

const double SpatialIndexConfig::DEFAULT_DIST_LOSS_ACCURACY = 0.025;

SpatialIndexConfig::SpatialIndexConfig(const string& indexName, IndexType indexType)
    : SingleFieldIndexConfig(indexName, indexType)
    , mImpl(new SpatialIndexConfigImpl(indexName, indexType))
{
    SingleFieldIndexConfig::ResetImpl(mImpl);
}
SpatialIndexConfig::SpatialIndexConfig(const SpatialIndexConfig& other)
    : SingleFieldIndexConfig(other)
    , mImpl(new SpatialIndexConfigImpl(*(other.mImpl)))
{
    SingleFieldIndexConfig::ResetImpl(mImpl);
}
SpatialIndexConfig::~SpatialIndexConfig()
{}

void SpatialIndexConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    mImpl->Jsonize(json);
}
void SpatialIndexConfig::AssertEqual(const IndexConfig& other) const
{
    const SpatialIndexConfig& other2 = (const SpatialIndexConfig&)other;
    mImpl->AssertEqual(*(other2.mImpl));
}
void SpatialIndexConfig::AssertCompatible(const IndexConfig& other) const
{
    const SpatialIndexConfig& other2 = (const SpatialIndexConfig&)other;
    mImpl->AssertEqual(*(other2.mImpl));
}
void SpatialIndexConfig::Check() const
{
    mImpl->Check();
}
IndexConfig* SpatialIndexConfig::Clone() const
{
    return new SpatialIndexConfig(*this);
}

double SpatialIndexConfig::GetMaxSearchDist()
{
    return mImpl->GetMaxSearchDist();
}
double SpatialIndexConfig::GetMaxDistError()
{
    return mImpl->GetMaxDistError();
}
double SpatialIndexConfig::GetDistanceLoss()
{
    return mImpl->GetDistanceLoss();
}
SpatialIndexConfig::DistCalculator SpatialIndexConfig::GetDistCalculator()
{
    return mImpl->GetDistCalculator();
}

//for test
void SpatialIndexConfig::SetMaxSearchDist(double dist)
{
    mImpl->SetMaxSearchDist(dist);
}
void SpatialIndexConfig::SetMaxDistError(double dist)
{
    mImpl->SetMaxDistError(dist);
}
void SpatialIndexConfig::SetDistanceLoss(double distLoss)
{
    mImpl->SetDistanceLoss(distLoss);
}

IE_NAMESPACE_END(config);

