#ifndef __INDEXLIB_GEO_HASH_H
#define __INDEXLIB_GEO_HASH_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/common/field_format/spatial/geo_hash/geohash.h"

IE_NAMESPACE_BEGIN(common);

class GeoHashUtil
{
public:
    GeoHashUtil();
    ~GeoHashUtil();

public:
    static const uint64_t ZERO_LEVEL_HASH_ID = 0;
public:
    static uint64_t Encode(double lon, double lat, int8_t level);
    static void Encode(double lon, double lat, std::vector<uint64_t>& terms,
                       int8_t minLevel, int8_t maxLevel);
    static void GetSubGeoHashIds(uint64_t hashId, std::vector<uint64_t>& subGeoHashes);
    static void GetGeoHashArea(uint64_t hashId, double& minX, double& minY,
                               double& maxX, double& maxY);
    static void GetGeoHashAreaFromHigherLevel(
        uint64_t hashId, GEOHASH_area higherArea,
        double& minX, double& minY,
        double& maxX, double& maxY);
    static int8_t GetLevelOfHashId(uint64_t hashId);
    //level [1,MAX_LEVEL]
    static int8_t DistanceToLevel(double dist);
    static bool IsLevelValid(int8_t level);
    static void SetLeafTag(uint64_t& hashId);
    static void RemoveLeafTag(uint64_t& hashId);
    static bool IsLeafCell(uint64_t cellId) { return cellId & PLUSFLAG_MASK; }
public:
    //only for test
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
    static void InnerDecode(uint64_t hashId, double& minX, double& minY,
                            double& maxX, double& maxY);
private:
    static const int8_t MAX_LEVEL = 11;
    static const uint64_t LEVEL_MASK = 0x1E;    //11110
    static const uint64_t PLUSFLAG_MASK = 0x1;
    static double mLevelDistErr[MAX_LEVEL + 1];
    static uint64_t mMask[MAX_LEVEL + 1];
    static char mBitsToCharTable[33];
    static int64_t mCharToBitsTable[44];

private:
    friend class GeoHashUtilTest;
    IE_LOG_DECLARE();
};
DEFINE_SHARED_PTR(GeoHashUtil);
////////////////////////////////////////////////////////////
inline uint64_t GeoHashUtil::GetParentEncode(uint64_t hashId, int8_t level)
{
    return (hashId & mMask[level]) + ((uint64_t)level << 1);
}

inline int8_t GeoHashUtil::GetLevelOfHashId(uint64_t hashId)
{
    return (hashId & LEVEL_MASK) >> 1;
}

inline uint64_t GeoHashUtil::GetBitsInLevel(uint64_t hashId, int8_t level)
{
    assert(IsLevelValid(level));
    uint64_t bits = hashId << (5 * (level - 1));
    return bits >> 59;
}

inline uint64_t GeoHashUtil::RemoveLevel(uint64_t hashId)
{
    return hashId & (~LEVEL_MASK);
}

inline bool GeoHashUtil::IsCoordinateValid(double lon, double lat)
{
    return (lat >= -90.0 && lat <= 90.0 &&
            lon >= -180.0 && lon <= 180.0);
}

inline bool GeoHashUtil::IsLevelValid(int8_t level)
{
    return (level > 0 && level <= MAX_LEVEL);
}


inline void GeoHashUtil::SetLeafTag(uint64_t& hashId)
{
    hashId |= PLUSFLAG_MASK;
}

inline void GeoHashUtil::RemoveLeafTag(uint64_t& hashId)
{
    hashId &= (~PLUSFLAG_MASK);
}

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_GEO_HASH_H
