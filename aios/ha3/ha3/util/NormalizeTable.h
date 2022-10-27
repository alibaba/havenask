#ifndef ISEARCH_NORMALIZETABLE_H
#define ISEARCH_NORMALIZETABLE_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>

BEGIN_HA3_NAMESPACE(util);

class NormalizeTable
{
public:
    NormalizeTable(bool caseSensitive, 
                   bool traditionalSensitive, 
                   bool widthSensitive,
                   const std::map<uint16_t, uint16_t> *traditionalTablePatch);
    ~NormalizeTable();
private:
    NormalizeTable(const NormalizeTable &rhs);
    NormalizeTable& operator= (const NormalizeTable &rhs);
public:
    uint16_t operator[] (uint16_t u) {
        return _table[u];
    }
private:
    uint16_t *_table;
private:
    static const uint16_t CHN_T2S_SET[0x10000];
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(NormalizeTable);

END_HA3_NAMESPACE(util);

#endif //ISEARCH_NORMALIZETABLE_H
