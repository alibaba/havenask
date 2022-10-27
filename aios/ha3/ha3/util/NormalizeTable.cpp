#include <ha3/util/NormalizeTable.h>

using namespace std;

BEGIN_HA3_NAMESPACE(util);
HA3_LOG_SETUP(util, NormalizeTable);


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
        if(traditionalTablePatch) {
            for(map<uint16_t, uint16_t>::const_iterator it = traditionalTablePatch->begin();
                it != traditionalTablePatch->end();it++)
            {
                _table[it->first] = it->second;
            }
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
}

NormalizeTable::~NormalizeTable() { 
    delete[] _table;
}

END_HA3_NAMESPACE(util);

