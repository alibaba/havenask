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
#include "indexlib/index/common/field_format/spatial/geo_hash/GeoHashUtil.h"
// TODO: remove
// #include "autil/HashAlgorithm.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, GeoHashUtil);

double GeoHashUtil::_levelDistErr[MAX_LEVEL + 1] = {40076000.0, 5009400.0, 1252300.0, 156500.0, 39100.0, 4900.0,
                                                    1200.0,     152.9,     38.2,      4.8,      1.2,     0.149};

uint64_t GeoHashUtil::_mask[MAX_LEVEL + 1] = {0L,
                                              0xF800000000000000L,
                                              0xFFC0000000000000L,
                                              0xFFFE000000000000L,
                                              0xFFFFF00000000000L,
                                              0xFFFFFF8000000000L,
                                              0xFFFFFFFC00000000L,
                                              0xFFFFFFFFE0000000L,
                                              0xFFFFFFFFFF000000L,
                                              0xFFFFFFFFFFF80000L,
                                              0xFFFFFFFFFFFFC000L,
                                              0xFFFFFFFFFFFFFE00L};

char GeoHashUtil::_bitsToCharTable[33] = "0123456789bcdefghjkmnpqrstuvwxyz";
int64_t GeoHashUtil::_charToBitsTable[44] = {
    /* 0 */ 0,  /* 1 */ 1,  /* 2 */ 2,  /* 3 */ 3,  /* 4 */ 4,
    /* 5 */ 5,  /* 6 */ 6,  /* 7 */ 7,  /* 8 */ 8,  /* 9 */ 9,
    /* : */ -1, /* ; */ -1, /* < */ -1, /* = */ -1, /* > */ -1,
    /* ? */ -1, /* @ */ -1, /* A */ -1, /* B */ 10, /* C */ 11,
    /* D */ 12, /* E */ 13, /* F */ 14, /* G */ 15, /* H */ 16,
    /* I */ -1, /* J */ 17, /* K */ 18, /* L */ -1, /* M */ 19,
    /* N */ 20, /* O */ -1, /* P */ 21, /* Q */ 22, /* R */ 23,
    /* S */ 24, /* T */ 25, /* U */ 26, /* V */ 27, /* W */ 28,
    /* X */ 29, /* Y */ 30, /* Z */ 31};

uint64_t GeoHashUtil::Encode(double lon, double lat, int8_t level)
{
    if (!IsLevelValid(level)) {
        return 0;
    }
    if (!IsCoordinateValid(lon, lat)) {
        return 0;
    }

    return InnerEncode(lon, lat, level);
}

std::vector<uint64_t> GeoHashUtil::Encode(double lon, double lat, int8_t minLevel, int8_t maxLevel)
{
    std::vector<uint64_t> terms;
    if (maxLevel < minLevel) {
        return {};
    }
    if (!IsLevelValid(minLevel) || !IsLevelValid(maxLevel)) {
        return {};
    }
    if (!IsCoordinateValid(lon, lat)) {
        return {};
    }

    uint64_t hashId = Encode(lon, lat, maxLevel);
    terms.reserve(maxLevel - minLevel + 1);
    for (int8_t level = minLevel; level <= maxLevel; ++level) {
        terms.push_back(GetParentEncode(hashId, level));
    }
    return terms;
}

void GeoHashUtil::GetGeoHashArea(uint64_t hashId, double& minX, double& minY, double& maxX, double& maxY)
{
    return InnerDecode(hashId, minX, minY, maxX, maxY);
}

void GeoHashUtil::GetSubGeoHashIds(uint64_t hashId, std::vector<uint64_t>& subGeoHashes)
{
    subGeoHashes.clear();
    uint64_t level = GetLevelOfHashId(hashId);
    if (level >= (uint64_t)MAX_LEVEL) {
        return;
    }

    uint64_t subLevel = level + 1;
    uint64_t shift = 64 - 5 * subLevel;
    uint64_t rawHashId = RemoveLevel(hashId);
    subGeoHashes.reserve(32);
    for (size_t i = 0; i < 32; ++i) {
        subGeoHashes.push_back(rawHashId + (i << shift) + (subLevel << 1));
    }
}

int8_t GeoHashUtil::DistanceToLevel(double dist)
{
    int8_t level = 1;
    while (_levelDistErr[level] > dist && level < MAX_LEVEL) {
        level++;
    }
    return level;
}

#define SET_BIT(bits, mid, range, value, offset)                                                                       \
    mid = ((range)->max + (range)->min) / 2.0;                                                                         \
    if ((value) >= mid) {                                                                                              \
        (range)->min = mid;                                                                                            \
        (bits) |= (0x1L << (offset));                                                                                  \
    } else {                                                                                                           \
        (range)->max = mid;                                                                                            \
        (bits) |= (0x0L << (offset));                                                                                  \
    }                                                                                                                  \
    (offset)--;
uint64_t GeoHashUtil::InnerEncode(double lon, double lat, int8_t level)
{
    assert(lat >= -90.0 && lat <= 90.0);
    assert(lon >= -180.0 && lon <= 180.0);
    assert(level > 0 && level <= MAX_LEVEL);

    double val1 = lon;
    double val2 = lat;
    GEOHASH_range lon_range = {180, -180};
    GEOHASH_range lat_range = {90, -90};
    GEOHASH_range* range1 = &lon_range;
    GEOHASH_range* range2 = &lat_range;

    uint64_t hash = 0;
    int32_t pos = 63;
    for (int8_t i = 0; i < level; i++) {
        double mid;
        SET_BIT(hash, mid, range1, val1, pos);
        SET_BIT(hash, mid, range2, val2, pos);
        SET_BIT(hash, mid, range1, val1, pos);
        SET_BIT(hash, mid, range2, val2, pos);
        SET_BIT(hash, mid, range1, val1, pos);

        double val_tmp = val1;
        val1 = val2;
        val2 = val_tmp;
        GEOHASH_range* range_tmp = range1;
        range1 = range2;
        range2 = range_tmp;
    }
    assert(pos >= 8);
    return hash + ((uint64_t)level << 1);
}
#undef SET_BIT

#define REFINE_RANGE(range, bits, offset)                                                                              \
    if (((bits) & (offset)) == (offset))                                                                               \
        (range)->min = ((range)->max + (range)->min) / 2.0;                                                            \
    else                                                                                                               \
        (range)->max = ((range)->max + (range)->min) / 2.0;

void GeoHashUtil::GetGeoHashAreaFromHigherLevel(uint64_t hashId, GEOHASH_area higherArea, double& minX, double& minY,
                                                double& maxX, double& maxY)
{
    GEOHASH_range* range1 = NULL;
    GEOHASH_range* range2 = NULL;
    int8_t level = GetLevelOfHashId(hashId);
    if (level == 1) {
        InnerDecode(hashId, minX, minY, maxX, maxY);
        return;
    }
    if ((level & (int8_t)1) == (int8_t)1) {
        range1 = &higherArea.longitude;
        range2 = &higherArea.latitude;
    } else {
        range2 = &higherArea.longitude;
        range1 = &higherArea.latitude;
    }

    uint64_t bits = GetBitsInLevel(hashId, level);
    REFINE_RANGE(range1, bits, 0x10);
    REFINE_RANGE(range2, bits, 0x08);
    REFINE_RANGE(range1, bits, 0x04);
    REFINE_RANGE(range2, bits, 0x02);
    REFINE_RANGE(range1, bits, 0x01);
    minX = higherArea.longitude.min;
    maxX = higherArea.longitude.max;
    minY = higherArea.latitude.min;
    maxY = higherArea.latitude.max;
}

void GeoHashUtil::InnerDecode(uint64_t hashId, double& minX, double& minY, double& maxX, double& maxY)
{
    GEOHASH_area area = {{90, -90}, {180, -180}};
    GEOHASH_range* range1 = &area.longitude;
    GEOHASH_range* range2 = &area.latitude;
    int8_t level = GetLevelOfHashId(hashId);
    assert(IsLevelValid(level));
    for (int8_t i = 1; i <= level; ++i) {
        uint64_t bits = GetBitsInLevel(hashId, i);
        REFINE_RANGE(range1, bits, 0x10);
        REFINE_RANGE(range2, bits, 0x08);
        REFINE_RANGE(range1, bits, 0x04);
        REFINE_RANGE(range2, bits, 0x02);
        REFINE_RANGE(range1, bits, 0x01);

        GEOHASH_range* range_tmp = range1;
        range1 = range2;
        range2 = range_tmp;
    }
    minX = area.longitude.min;
    maxX = area.longitude.max;
    minY = area.latitude.min;
    maxY = area.latitude.max;
}
#undef REFINE_RANGE

uint64_t GeoHashUtil::GeoStrToHash(const std::string& geoHashStr)
{
    uint64_t level = geoHashStr.size();
    assert(level <= (uint64_t)MAX_LEVEL);
    uint64_t hash = 0;
    for (size_t i = 0; i < level; i++) {
        uint64_t bits = GeoCharToBits(geoHashStr[i], i + 1);
        hash |= bits;
    }
    return hash + ((uint64_t)level << 1);
}

std::string GeoHashUtil::HashToGeoStr(uint64_t hashId)
{
    size_t level = GetLevelOfHashId(hashId);
    std::string geoStr;
    for (size_t i = 0; i < level; i++) {
        char value = GeoHashToChar(hashId, i + 1);
        geoStr.append(1, value);
    }
    return geoStr;
}

uint64_t GeoHashUtil::GeoCharToBits(char value, int8_t level)
{
    value = toupper(value);
    uint64_t bitsIdx = (size_t)(value - '0');
    assert(bitsIdx < 44);
    uint64_t bits = uint64_t(_charToBitsTable[bitsIdx]);
    return bits << (64 - 5 * level);
}

char GeoHashUtil::GeoHashToChar(uint64_t hashId, int8_t level)
{
    uint64_t bits = GetBitsInLevel(hashId, level);
    return _bitsToCharTable[bits];
}
} // namespace indexlib::index
