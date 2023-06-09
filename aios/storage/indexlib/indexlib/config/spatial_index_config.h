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
#ifndef __INDEXLIB_SPATIAL_INDEX_CONFIG_H
#define __INDEXLIB_SPATIAL_INDEX_CONFIG_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/single_field_index_config.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace config {

// TODO: add test
class SpatialIndexConfig : public SingleFieldIndexConfig
{
public:
    enum DistCalculator { HAVERSINE, UNKNOW };
    static const double DEFAULT_DIST_LOSS_ACCURACY;

public:
    SpatialIndexConfig(const std::string& indexName, InvertedIndexType indexType);
    SpatialIndexConfig(const SpatialIndexConfig& other);
    ~SpatialIndexConfig();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void AssertEqual(const IndexConfig& other) const override;
    void AssertCompatible(const IndexConfig& other) const override;
    void Check() const override;
    IndexConfig* Clone() const override;
    std::unique_ptr<indexlibv2::config::InvertedIndexConfig> ConstructConfigV2() const override;

    double GetMaxSearchDist();
    double GetMaxDistError();
    double GetDistanceLoss();
    DistCalculator GetDistCalculator();

public:
    // for test
    void SetMaxSearchDist(double dist);
    void SetMaxDistError(double dist);
    void SetDistanceLoss(double distLoss);

private:
    std::string CalculatorToStr(SpatialIndexConfig::DistCalculator calculator);
    SpatialIndexConfig::DistCalculator StrToCalculator(const std::string& str);
    bool CheckFieldType(FieldType ft) const override;
    bool FulfillConfigV2(indexlibv2::config::InvertedIndexConfig* configV2) const override;

private:
    // Equatorial circumference, minLevel:1
    static constexpr double DEFAULT_MAX_SEARCH_DIST_VALUE = 40076000.0;
    // detailLevel:11
    static constexpr double MIN_MAX_DIST_ERROR_VALUE = 0.05;
    static constexpr double DEFAULT_MAX_DIST_ERROR_VALUE = MIN_MAX_DIST_ERROR_VALUE;

private:
    struct Impl;
    std::unique_ptr<Impl> mImpl;

private:
    friend class SpatialIndexConfigTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SpatialIndexConfig);
}} // namespace indexlib::config

#endif //__INDEXLIB_SPATIAL_INDEX_CONFIG_H
