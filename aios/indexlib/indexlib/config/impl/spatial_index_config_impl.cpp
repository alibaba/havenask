#include "indexlib/config/impl/spatial_index_config_impl.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/config/config_define.h"

using namespace std;

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, SpatialIndexConfigImpl);

//Equatorial circumference, minLevel:1
const double SpatialIndexConfigImpl::DEFAULT_MAX_SEARCH_DIST_VALUE = 40076000.0;
//detailLevel:11
const double SpatialIndexConfigImpl::MIN_MAX_DIST_ERROR_VALUE = 0.05;
const double SpatialIndexConfigImpl::DEFAULT_MAX_DIST_ERROR_VALUE = MIN_MAX_DIST_ERROR_VALUE;

SpatialIndexConfigImpl::SpatialIndexConfigImpl(const string& indexName, IndexType indexType) 
    : SingleFieldIndexConfigImpl(indexName, indexType)
    , mMaxSearchDist(DEFAULT_MAX_SEARCH_DIST_VALUE)
    , mMaxDistError(DEFAULT_MAX_DIST_ERROR_VALUE)
    , mCalculator(SpatialIndexConfig::DistCalculator::HAVERSINE)
    , mDistanceLoss(SpatialIndexConfig::DEFAULT_DIST_LOSS_ACCURACY)
{
}

SpatialIndexConfigImpl::SpatialIndexConfigImpl(const SpatialIndexConfigImpl& other)
    : SingleFieldIndexConfigImpl(other)
    , mMaxSearchDist(other.mMaxSearchDist)
    , mMaxDistError(other.mMaxDistError)
    , mCalculator(other.mCalculator)
    , mDistanceLoss(other.mDistanceLoss)
{
}

SpatialIndexConfigImpl::~SpatialIndexConfigImpl() 
{
}

void SpatialIndexConfigImpl::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    SingleFieldIndexConfigImpl::Jsonize(json);
    json.Jsonize(MAX_SEARCH_DIST, mMaxSearchDist, mMaxSearchDist);
    json.Jsonize(MAX_DIST_ERROR, mMaxDistError, mMaxDistError);
    json.Jsonize(DIST_LOSS_ACCURACY, mDistanceLoss, mDistanceLoss);
    if (json.GetMode() == TO_JSON)
    {
        string calculator = CalculatorToStr(mCalculator);
        json.Jsonize(DIST_CALCULATOR, calculator);
    }
    else
    {
        string calculator;
        json.Jsonize(DIST_CALCULATOR, calculator, CALCULATOR_HAVERSINE);
        mCalculator = StrToCalculator(calculator);
    }
    mOptionFlag = of_none;
}

void SpatialIndexConfigImpl::AssertEqual(const IndexConfigImpl& other) const
{
    SingleFieldIndexConfigImpl::AssertEqual(other);
    const SpatialIndexConfigImpl& other2 = (const SpatialIndexConfigImpl&) other;
    IE_CONFIG_ASSERT_EQUAL(mMaxSearchDist, other2.mMaxSearchDist, 
                           "max search dist not equal");
    IE_CONFIG_ASSERT_EQUAL(mMaxDistError, other2.mMaxDistError, 
                           "min dist error not equal");
    IE_CONFIG_ASSERT_EQUAL(mCalculator, other2.mCalculator, 
                           "calculator type not equal");
    IE_CONFIG_ASSERT_EQUAL(mDistanceLoss, other2.mDistanceLoss, 
                           "distance loss not equal");
}

IndexConfigImpl* SpatialIndexConfigImpl::Clone() const
{
    return new SpatialIndexConfigImpl(*this);
}

void SpatialIndexConfigImpl::Check() const
{
    SingleFieldIndexConfigImpl::Check();
    if (mMaxDistError < MIN_MAX_DIST_ERROR_VALUE)
    {
        INDEXLIB_FATAL_ERROR(Schema, 
                             "max dist error must greater or equal than %lf",
                             MIN_MAX_DIST_ERROR_VALUE);
    }

    if (mMaxSearchDist < mMaxDistError)
    {
        INDEXLIB_FATAL_ERROR(Schema, 
                             "max search dist must greater than max dist error");
    }
    if (mDistanceLoss >= 1.0 || mDistanceLoss <= 0.0)
    {
        INDEXLIB_FATAL_ERROR(
                Schema, "invalid distance loss: %lf, should between (0,1)",
                mDistanceLoss);
    }
}

string SpatialIndexConfigImpl::CalculatorToStr(SpatialIndexConfig::DistCalculator calculator)
{
    if (calculator == SpatialIndexConfig::DistCalculator::HAVERSINE)
    {
        return CALCULATOR_HAVERSINE;
    }
    assert(false);
    return "";
}

SpatialIndexConfig::DistCalculator SpatialIndexConfigImpl::StrToCalculator(const string& str)
{
    if (str == CALCULATOR_HAVERSINE)
    {
        return SpatialIndexConfig::DistCalculator::HAVERSINE;
    }
    assert(false);
    return SpatialIndexConfig::DistCalculator::UNKNOW;
}

IE_NAMESPACE_END(config);

