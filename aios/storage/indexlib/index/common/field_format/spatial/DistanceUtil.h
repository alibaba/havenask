/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <math.h>

#include "autil/Log.h"
#include "autil/NoCopyable.h"

namespace indexlib::index {
class Rectangle;
class DistanceUtil : private autil::NoCopyable
{
public:
    DistanceUtil() = default;
    ~DistanceUtil() = default;

public:
    static const double DEGREES_TO_RADIANS;
    static const double RADIANS_TO_DEGREES;
    static const double EARTH_MEAN_RADIUS;
    static const double EARTH_EQUATORIAL_RADIUS;

public:
    static double Dist2Radians(double dist, double radius) { return dist / radius; }

    static double Radians2Dist(double radians, double radius) { return radians * radius; }

    static double ToRadians(double degrees) { return degrees * DEGREES_TO_RADIANS; }

    static double ToDegrees(double radians) { return radians * RADIANS_TO_DEGREES; }

    static double Dist2Degrees(double dist, double radius) { return ToDegrees(Dist2Radians(dist, radius)); }

    static double Degrees2Dist(double degrees, double radius) { return Radians2Dist(ToRadians(degrees), radius); }

    // distance unit is meter
    static std::shared_ptr<Rectangle> CalculateBoundingBoxFromPoint(double lat, double lon, double distance);

    static double DistHaversineRAD(double lat1, double lon1, double lat2, double lon2);

private:
    static std::shared_ptr<Rectangle> CalculateBoundingBoxFromPointDeg(double lat, double lon, double distDEG);
    static double NormLonDEG(double lon_deg);
    static double CalcBoxByDistFromPt_deltaLonDEG(double lat, double lon, double distDEG);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
