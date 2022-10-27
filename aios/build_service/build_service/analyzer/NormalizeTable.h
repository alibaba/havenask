#ifndef ISEARCH_BS_NORMALIZETABLE_H
#define ISEARCH_BS_NORMALIZETABLE_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service {
namespace analyzer {

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
    uint16_t operator[] (uint16_t u) const {
        return _table[u];
    }
private:
    uint16_t *_table;
private:
    static const uint16_t CHN_T2S_SET[0x10000];
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(NormalizeTable);

}
}

#endif //ISEARCH_BS_NORMALIZETABLE_H
