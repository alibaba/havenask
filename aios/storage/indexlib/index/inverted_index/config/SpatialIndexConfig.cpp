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
#include "indexlib/index/inverted_index/config/SpatialIndexConfig.h"

#include "indexlib/config/ConfigDefine.h"
#include "indexlib/util/Exception.h"

using namespace std;

namespace indexlibv2::config {
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
    , _impl(make_unique<Impl>())
{
}
SpatialIndexConfig::SpatialIndexConfig(const SpatialIndexConfig& other)
    : SingleFieldIndexConfig(other)
    , _impl(make_unique<Impl>(*(other._impl)))
{
}
SpatialIndexConfig::~SpatialIndexConfig() {}

void SpatialIndexConfig::Serialize(autil::legacy::Jsonizable::JsonWrapper& json) const
{
    assert(json.GetMode() == autil::legacy::Jsonizable::TO_JSON);
    SingleFieldIndexConfig::Serialize(json);
    json.Jsonize(MAX_SEARCH_DIST, _impl->maxSearchDist, _impl->maxSearchDist);
    json.Jsonize(MAX_DIST_ERROR, _impl->maxDistError, _impl->maxDistError);
    json.Jsonize(DIST_LOSS_ACCURACY, _impl->distanceLoss, _impl->distanceLoss);
    string calculator = CalculatorToStr(_impl->calculator);
    json.Jsonize(DIST_CALCULATOR, calculator);
}

Status SpatialIndexConfig::CheckEqual(const InvertedIndexConfig& other) const
{
    auto status = SingleFieldIndexConfig::CheckEqual(other);
    RETURN_IF_STATUS_ERROR(status, "SingleFieldIndexConfig not equal");
    const SpatialIndexConfig& other2 = (const SpatialIndexConfig&)other;
    CHECK_CONFIG_EQUAL(_impl->maxSearchDist, other2._impl->maxSearchDist, "max search dist not equal");
    CHECK_CONFIG_EQUAL(_impl->maxDistError, other2._impl->maxDistError, "min dist error not equal");
    CHECK_CONFIG_EQUAL(_impl->calculator, other2._impl->calculator, "calculator type not equal");
    CHECK_CONFIG_EQUAL(_impl->distanceLoss, other2._impl->distanceLoss, "distance loss not equal");
    return Status::OK();
}
void SpatialIndexConfig::Check() const
{
    SingleFieldIndexConfig::Check();
    if (_impl->maxDistError < MIN_MAX_DIST_ERROR_VALUE) {
        INDEXLIB_FATAL_ERROR(Schema, "max dist error must greater or equal than %lf, index name [%s]",
                             MIN_MAX_DIST_ERROR_VALUE, GetIndexName().c_str());
    }

    if (_impl->maxSearchDist < _impl->maxDistError) {
        INDEXLIB_FATAL_ERROR(Schema, "max search dist must greater than max dist error, index name [%s]",
                             GetIndexName().c_str());
    }
    if (_impl->distanceLoss >= 1.0 || _impl->distanceLoss <= 0.0) {
        INDEXLIB_FATAL_ERROR(Schema, "invalid distance loss: %lf, should between (0,1), index name [%s]",
                             _impl->distanceLoss, GetIndexName().c_str());
    }
}
InvertedIndexConfig* SpatialIndexConfig::Clone() const { return new SpatialIndexConfig(*this); }

double SpatialIndexConfig::GetMaxSearchDist() { return _impl->maxSearchDist; }
double SpatialIndexConfig::GetMaxDistError() { return _impl->maxDistError; }
double SpatialIndexConfig::GetDistanceLoss() { return _impl->distanceLoss; }
SpatialIndexConfig::DistCalculator SpatialIndexConfig::GetDistCalculator() { return _impl->calculator; }

void SpatialIndexConfig::SetMaxSearchDist(double dist) { _impl->maxSearchDist = dist; }
void SpatialIndexConfig::SetMaxDistError(double dist) { _impl->maxDistError = dist; }
void SpatialIndexConfig::SetDistanceLoss(double distLoss) { _impl->distanceLoss = distLoss; }
void SpatialIndexConfig::SetCalculator(SpatialIndexConfig::DistCalculator calculator)
{
    _impl->calculator = calculator;
}
string SpatialIndexConfig::CalculatorToStr(SpatialIndexConfig::DistCalculator calculator) const
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

void SpatialIndexConfig::DoDeserialize(const autil::legacy::Any& any,
                                       const config::IndexConfigDeserializeResource& resource)
{
    SingleFieldIndexConfig::DoDeserialize(any, resource);
    autil::legacy::Jsonizable::JsonWrapper json(any);
    json.Jsonize(MAX_SEARCH_DIST, _impl->maxSearchDist, _impl->maxSearchDist);
    json.Jsonize(MAX_DIST_ERROR, _impl->maxDistError, _impl->maxDistError);
    json.Jsonize(DIST_LOSS_ACCURACY, _impl->distanceLoss, _impl->distanceLoss);
    string calculator;
    json.Jsonize(DIST_CALCULATOR, calculator, CALCULATOR_HAVERSINE);
    _impl->calculator = StrToCalculator(calculator);
    SetOptionFlag(of_none);
}

} // namespace indexlibv2::config
