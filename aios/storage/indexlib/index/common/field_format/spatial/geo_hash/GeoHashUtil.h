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

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "indexlib/index/common/field_format/spatial/geo_hash/geohash.h"

namespace indexlib::index {

class GeoHashUtil : private autil::NoCopyable
{
public:
    GeoHashUtil() = default;
    ~GeoHashUtil() = default;

public:
    static constexpr uint64_t ZERO_LEVEL_HASH_ID = 0;

public:
    static uint64_t Encode(double lon, double lat, int8_t level);
    static std::vector<uint64_t> Encode(double lon, double lat, int8_t minLevel, int8_t maxLevel);
    static void GetSubGeoHashIds(uint64_t hashId, std::vector<uint64_t>& subGeoHashes);
    static void GetGeoHashArea(uint64_t hashId, double& minX, double& minY, double& maxX, double& maxY);
    static void GetGeoHashAreaFromHigherLevel(uint64_t hashId, GEOHASH_area higherArea, double& minX, double& minY,
                                              double& maxX, double& maxY);
    static int8_t GetLevelOfHashId(uint64_t hashId);
    // level [1,MAX_LEVEL]
    static int8_t DistanceToLevel(double dist);
    static bool IsLevelValid(int8_t level);
    static void SetLeafTag(uint64_t& hashId);
    static void RemoveLeafTag(uint64_t& hashId);
    static bool IsLeafCell(uint64_t cellId) { return cellId & PLUSFLAG_MASK; }

public:
    // only for test
    static uint64_t GeoStrToHash(const std::string& geoHashStr);
    static std::string HashToGeoStr(uint64_t hashId);

private:
    static uint64_t GetParentEncode(uint64_t hashId, int8_t level);
    static uint64_t GeoCharToBits(char value, int8_t level);
    static char GeoHashToChar(uint64_t hashId, int8_t level);
    static uint64_t GetBitsInLevel(uint64_t hashId, int8_t level);
    static bool IsCoordinateValid(double lon, double lat);
    static uint64_t RemoveLevel(uint64_t hashId);
    static uint64_t InnerEncode(double lon, double lat, int8_t level);
    static void InnerDecode(uint64_t hashId, double& minX, double& minY, double& maxX, double& maxY);

private:
    static constexpr int8_t MAX_LEVEL = 11;
    static constexpr uint64_t LEVEL_MASK = 0x1E; // 11110
    static constexpr uint64_t PLUSFLAG_MASK = 0x1;
    static double _levelDistErr[MAX_LEVEL + 1];
    static uint64_t _mask[MAX_LEVEL + 1];
    static char _bitsToCharTable[33];
    static int64_t _charToBitsTable[44];

private:
    AUTIL_LOG_DECLARE();
};

////////////////////////////////////////////////////////////
inline uint64_t GeoHashUtil::GetParentEncode(uint64_t hashId, int8_t level)
{
    return (hashId & _mask[level]) + ((uint64_t)level << 1);
}

inline int8_t GeoHashUtil::GetLevelOfHashId(uint64_t hashId) { return (hashId & LEVEL_MASK) >> 1; }

inline uint64_t GeoHashUtil::GetBitsInLevel(uint64_t hashId, int8_t level)
{
    assert(IsLevelValid(level));
    uint64_t bits = hashId << (5 * (level - 1));
    return bits >> 59;
}

inline uint64_t GeoHashUtil::RemoveLevel(uint64_t hashId) { return hashId & (~LEVEL_MASK); }

inline bool GeoHashUtil::IsCoordinateValid(double lon, double lat)
{
    return (lat >= -90.0 && lat <= 90.0 && lon >= -180.0 && lon <= 180.0);
}

inline bool GeoHashUtil::IsLevelValid(int8_t level) { return (level > 0 && level <= MAX_LEVEL); }

inline void GeoHashUtil::SetLeafTag(uint64_t& hashId) { hashId |= PLUSFLAG_MASK; }

inline void GeoHashUtil::RemoveLeafTag(uint64_t& hashId) { hashId &= (~PLUSFLAG_MASK); }

} // namespace indexlib::index
