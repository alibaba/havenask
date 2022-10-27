#ifndef __INDEXLIB_HASH_TABLE_OPTIONS_H
#define __INDEXLIB_HASH_TABLE_OPTIONS_H

IE_NAMESPACE_BEGIN(common);

struct HashTableOptions
{
    HashTableOptions(int32_t _occupancyPct)
        : occupancyPct(_occupancyPct)
        , mayStretch(false)
        , maxNumHashFunc(8)
    {}
    int32_t occupancyPct;
    bool mayStretch;
    // cuckoo
    uint8_t maxNumHashFunc;
};


IE_NAMESPACE_END(common);

#endif //__INDEXLIB_HASH_TABLE_OPTIONS_H
