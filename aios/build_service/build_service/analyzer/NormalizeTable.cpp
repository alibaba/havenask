#include "build_service/analyzer/NormalizeTable.h"

using namespace std;

namespace build_service {
namespace analyzer {
BS_LOG_SETUP(util, NormalizeTable);


NormalizeTable::NormalizeTable(bool caseSensitive,
                               bool traditionalSensitive,
                               bool widthSensitive,
                               const map<uint16_t, uint16_t> *traditionalTablePatch)
{
    _table = new uint16_t[0x10000];
    if (!traditionalSensitive) {
        for (int i = 0; i < 0x10000; ++i) {
            _table[i] = CHN_T2S_SET[i];
        }
    } else {
        for (int i = 0; i < 0x10000; ++i) {
            _table[i] = i;
        }
    }

    if (!widthSensitive) {
        _table[0x3000] = ' ';
        for (int i = 0xFF01; i <= 0xFF5E; ++i) {
            _table[i] = i - 0xFEE0;
        }
    }

    if (!caseSensitive) {
        for (int i = 'A'; i <= 'Z'; ++i) {
            _table[i] += 'a' - 'A';
        }
        for (int i = 0xFF21; i <= 0xFF3a; ++i) {
            _table[i] += 'a' - 'A';
        }
    }
    
    if (!traditionalSensitive && traditionalTablePatch) {
        for(map<uint16_t, uint16_t>::const_iterator it = traditionalTablePatch->begin();
                it != traditionalTablePatch->end(); ++it)
            {
                _table[it->first] = it->second;
            }
    }
}

NormalizeTable::~NormalizeTable() {
    delete[] _table;
}

}
}
