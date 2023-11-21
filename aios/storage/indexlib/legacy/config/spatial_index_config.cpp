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
#include "indexlib/config/spatial_index_config.h"

#include "indexlib/config/config_define.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/index/inverted_index/config/SpatialIndexConfig.h"

using namespace std;

namespace indexlib { namespace config {
AUTIL_LOG_SETUP(indexlib.config, SpatialIndexConfig);

const double SpatialIndexConfig::DEFAULT_DIST_LOSS_ACCURACY = 0.025;

struct SpatialIndexConfig::Impl {
    double maxSearchDist = DEFAULT_MAX_SEARCH_DIST_VALUE;
    double maxDistError = DEFAULT_MAX_DIST_ERROR_VALUE;
    SpatialIndexConfig::DistCalculator calculator = SpatialIndexConfig::DistCalculator::HAVERSINE;
    double distanceLoss = SpatialIndexConfig::DEFAULT_DIST_LOSS_ACCURACY;
};

SpatialIndexConfig::SpatialIndexConfig(const string& indexName, InvertedIndexType indexType)
    : SingleFieldIndexConfig(indexName, indexType)
    , mImpl(make_unique<Impl>())
{
}
SpatialIndexConfig::SpatialIndexConfig(const SpatialIndexConfig& other)
    : SingleFieldIndexConfig(other)
    , mImpl(make_unique<Impl>(*(other.mImpl)))
{
}
SpatialIndexConfig::~SpatialIndexConfig() {}

void SpatialIndexConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    SingleFieldIndexConfig::Jsonize(json);
    json.Jsonize(MAX_SEARCH_DIST, mImpl->maxSearchDist, mImpl->maxSearchDist);
    json.Jsonize(MAX_DIST_ERROR, mImpl->maxDistError, mImpl->maxDistError);
    json.Jsonize(DIST_LOSS_ACCURACY, mImpl->distanceLoss, mImpl->distanceLoss);
    if (json.GetMode() == TO_JSON) {
        string calculator = CalculatorToStr(mImpl->calculator);
        json.Jsonize(DIST_CALCULATOR, calculator);
    } else {
        string calculator;
        json.Jsonize(DIST_CALCULATOR, calculator, CALCULATOR_HAVERSINE);
        mImpl->calculator = StrToCalculator(calculator);
    }
    SetOptionFlag(of_none);
}
void SpatialIndexConfig::AssertEqual(const IndexConfig& other) const
{
    SingleFieldIndexConfig::AssertEqual(other);
    const SpatialIndexConfig& other2 = (const SpatialIndexConfig&)other;
    IE_CONFIG_ASSERT_EQUAL(mImpl->maxSearchDist, other2.mImpl->maxSearchDist, "max search dist not equal");
    IE_CONFIG_ASSERT_EQUAL(mImpl->maxDistError, other2.mImpl->maxDistError, "min dist error not equal");
    IE_CONFIG_ASSERT_EQUAL(mImpl->calculator, other2.mImpl->calculator, "calculator type not equal");
    IE_CONFIG_ASSERT_EQUAL(mImpl->distanceLoss, other2.mImpl->distanceLoss, "distance loss not equal");
}
void SpatialIndexConfig::AssertCompatible(const IndexConfig& other) const { AssertEqual(other); }
void SpatialIndexConfig::Check() const
{
    SingleFieldIndexConfig::Check();
    if (mImpl->maxDistError < MIN_MAX_DIST_ERROR_VALUE) {
        INDEXLIB_FATAL_ERROR(Schema, "max dist error must greater or equal than %lf, index name [%s]",
                             MIN_MAX_DIST_ERROR_VALUE, GetIndexName().c_str());
    }

    if (mImpl->maxSearchDist < mImpl->maxDistError) {
        INDEXLIB_FATAL_ERROR(Schema, "max search dist must greater than max dist error, index name [%s]",
                             GetIndexName().c_str());
    }
    if (mImpl->distanceLoss >= 1.0 || mImpl->distanceLoss <= 0.0) {
        INDEXLIB_FATAL_ERROR(Schema, "invalid distance loss: %lf, should between (0,1), index name [%s]",
                             mImpl->distanceLoss, GetIndexName().c_str());
    }
}
IndexConfig* SpatialIndexConfig::Clone() const { return new SpatialIndexConfig(*this); }

std::unique_ptr<indexlibv2::config::InvertedIndexConfig> SpatialIndexConfig::ConstructConfigV2() const
{
    return DoConstructConfigV2<indexlibv2::config::SpatialIndexConfig>();
}

double SpatialIndexConfig::GetMaxSearchDist() { return mImpl->maxSearchDist; }
double SpatialIndexConfig::GetMaxDistError() { return mImpl->maxDistError; }
double SpatialIndexConfig::GetDistanceLoss() { return mImpl->distanceLoss; }
SpatialIndexConfig::DistCalculator SpatialIndexConfig::GetDistCalculator() { return mImpl->calculator; }
// for test
void SpatialIndexConfig::SetMaxSearchDist(double dist) { mImpl->maxSearchDist = dist; }
void SpatialIndexConfig::SetMaxDistError(double dist) { mImpl->maxDistError = dist; }
void SpatialIndexConfig::SetDistanceLoss(double distLoss) { mImpl->distanceLoss = distLoss; }
string SpatialIndexConfig::CalculatorToStr(SpatialIndexConfig::DistCalculator calculator)
{
    if (calculator == SpatialIndexConfig::DistCalculator::HAVERSINE) {
        return CALCULATOR_HAVERSINE;
    }
    assert(false);
    return "";
}

SpatialIndexConfig::DistCalculator SpatialIndexConfig::StrToCalculator(const string& str)
{
    if (str == CALCULATOR_HAVERSINE) {
        return SpatialIndexConfig::DistCalculator::HAVERSINE;
    }
    assert(false);
    return SpatialIndexConfig::DistCalculator::UNKNOW;
}

bool SpatialIndexConfig::CheckFieldType(FieldType ft) const
{
    return ft == ft_location || ft == ft_line || ft == ft_polygon;
}

bool SpatialIndexConfig::FulfillConfigV2(indexlibv2::config::InvertedIndexConfig* configV2) const
{
    if (!SingleFieldIndexConfig::FulfillConfigV2(configV2)) {
        AUTIL_LOG(ERROR, "fulfill spatial index config failed");
        return false;
    }
    auto typedConfigV2 = dynamic_cast<indexlibv2::config::SpatialIndexConfig*>(configV2);
    assert(typedConfigV2);
    typedConfigV2->SetMaxSearchDist(mImpl->maxSearchDist);
    typedConfigV2->SetMaxDistError(mImpl->maxDistError);
    typedConfigV2->SetDistanceLoss(mImpl->distanceLoss);
    assert(mImpl->calculator == HAVERSINE);
    typedConfigV2->SetCalculator(indexlibv2::config::SpatialIndexConfig::DistCalculator::HAVERSINE);
    return true;
}

}} // namespace indexlib::config
