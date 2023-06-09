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
#include "indexlib/index/common/field_format/spatial/DistanceUtil.h"

#include <cmath>

#include "indexlib/index/common/field_format/spatial/shape/Rectangle.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, DistanceUtil);

const double DistanceUtil::DEGREES_TO_RADIANS = M_PI / 180;
const double DistanceUtil::RADIANS_TO_DEGREES = 1 / DEGREES_TO_RADIANS;
const double DistanceUtil::EARTH_MEAN_RADIUS = 6371008.7714;
const double DistanceUtil::EARTH_EQUATORIAL_RADIUS = 6378137.0;

std::shared_ptr<Rectangle> DistanceUtil::CalculateBoundingBoxFromPoint(double lat, double lon, double distance)
{
    double distDEG = Dist2Degrees(distance, EARTH_MEAN_RADIUS);
    return CalculateBoundingBoxFromPointDeg(lat, lon, distDEG);
}

std::shared_ptr<Rectangle> DistanceUtil::CalculateBoundingBoxFromPointDeg(double lat, double lon, double distDEG)
{
    double minX;
    double maxX;
    double minY;
    double maxY;
    if (distDEG == 0) {
        minX = lon;
        maxX = lon;
        minY = lat;
        maxY = lat;
    } else if (distDEG >= 180) { // distance is >= opposite side of the globe
        minX = -180;
        maxX = 180;
        minY = -90;
        maxY = 90;
    } else {
        //--calc latitude bounds
        maxY = lat + distDEG;
        minY = lat - distDEG;

        if (maxY >= 90 || minY <= -90) { // touches either pole
            // we have special logic for longitude
            minX = -180;
            maxX = 180;                      // world wrap: 360 deg
            if (maxY <= 90 && minY >= -90) { // doesn't pass either pole: 180 deg
                minX = NormLonDEG(lon - 90);
                maxX = NormLonDEG(lon + 90);
            }
            if (maxY > 90)
                maxY = 90;
            if (minY < -90)
                minY = -90;
        } else {
            //--calc longitude bounds
            double lon_delta_deg = CalcBoxByDistFromPt_deltaLonDEG(lat, lon, distDEG);

            minX = NormLonDEG(lon - lon_delta_deg);
            maxX = NormLonDEG(lon + lon_delta_deg);
        }
    }
    return std::make_shared<Rectangle>(minX, minY, maxX, maxY);
}

double DistanceUtil::NormLonDEG(double lon_deg)
{
    if (lon_deg >= -180 && lon_deg <= 180) {
        return lon_deg; // common case, and avoids slight double precision shifting
    }
    double off = fmod((lon_deg + 180.0), 360);
    if (off < 0) {
        return 180.0 + off;
    } else if (off == 0 && lon_deg > 0) {
        return 180.0;
    } else {
        return -180.0 + off;
    }
}

double DistanceUtil::CalcBoxByDistFromPt_deltaLonDEG(double lat, double lon, double distDEG)
{
    // http://gis.stackexchange.com/questions/19221/find-tangent-point-on-circle-furthest-east-or-west
    if (distDEG == 0) {
        return 0;
    }
    double lat_rad = ToRadians(lat);
    double dist_rad = ToRadians(distDEG);
    double result_rad = asin(sin(dist_rad) / cos(lat_rad));

    if (!std::isnan(result_rad)) // check if result_rad = NaN
    {
        return ToDegrees(result_rad);
    }
    return 90;
}

double DistanceUtil::DistHaversineRAD(double lat1, double lon1, double lat2, double lon2)
{
    // TODO investigate slightly different formula using asin() and min()
    // http://www.movable-type.co.uk/scripts/gis-faq-5.1.html

    // Check for same position
    if (lat1 == lat2 && lon1 == lon2) {
        return 0.0;
    }
    double hsinX = sin((lon1 - lon2) * 0.5);
    double hsinY = sin((lat1 - lat2) * 0.5);
    double h = hsinY * hsinY + (cos(lat1) * cos(lat2) * hsinX * hsinX);
    if (h > 1) // numeric robustness issue. If we didn't check, the answer would be NaN!
        h = 1;
    return 2 * atan2(sqrt(h), sqrt(1 - h));
}
} // namespace indexlib::index
