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
#pragma once
#include "indexlib/index/inverted_index/config/SingleFieldIndexConfig.h"

namespace indexlibv2::config {

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
    void Serialize(autil::legacy::Jsonizable::JsonWrapper& json) const override;
    Status CheckEqual(const InvertedIndexConfig& other) const override;
    void Check() const override;
    InvertedIndexConfig* Clone() const override;

    double GetMaxSearchDist();
    double GetMaxDistError();
    double GetDistanceLoss();
    DistCalculator GetDistCalculator();

public:
    void SetMaxSearchDist(double dist);
    void SetMaxDistError(double dist);
    void SetDistanceLoss(double distLoss);
    void SetCalculator(DistCalculator calculator);

protected:
    void DoDeserialize(const autil::legacy::Any& any, const config::IndexConfigDeserializeResource& resource) override;

private:
    std::string CalculatorToStr(SpatialIndexConfig::DistCalculator calculator) const;
    SpatialIndexConfig::DistCalculator StrToCalculator(const std::string& str);
    bool CheckFieldType(FieldType ft) const override;

private:
    // Equatorial circumference, minLevel:1
    static constexpr double DEFAULT_MAX_SEARCH_DIST_VALUE = 40076000.0;
    // detailLevel:11
    static constexpr double MIN_MAX_DIST_ERROR_VALUE = 0.05;
    static constexpr double DEFAULT_MAX_DIST_ERROR_VALUE = MIN_MAX_DIST_ERROR_VALUE;

    inline static const std::string MAX_SEARCH_DIST = "max_search_dist";
    inline static const std::string MAX_DIST_ERROR = "max_dist_err";
    inline static const std::string DIST_LOSS_ACCURACY = "distance_loss_accuracy";
    inline static const std::string DIST_CALCULATOR = "dist_calculator";
    inline static const std::string CALCULATOR_HAVERSINE = "haversine";

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;

private:
    friend class SpatialIndexConfigTest;
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::config
