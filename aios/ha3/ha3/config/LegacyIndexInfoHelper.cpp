#include <ha3/config/LegacyIndexInfoHelper.h>

using namespace std;
using namespace suez::turing;
BEGIN_HA3_NAMESPACE(config);
HA3_LOG_SETUP(config, LegacyIndexInfoHelper);

LegacyIndexInfoHelper::LegacyIndexInfoHelper() { 
    _indexInfos = NULL;
}

LegacyIndexInfoHelper::LegacyIndexInfoHelper(const IndexInfos *indexInfos) {
    _indexInfos = indexInfos;
}

LegacyIndexInfoHelper::~LegacyIndexInfoHelper() { 
}

indexid_t LegacyIndexInfoHelper::getIndexId(const string &indexName) const {
    assert(_indexInfos);
    const IndexInfo *indexInfo = _indexInfos->getIndexInfo(indexName.c_str());
    if (indexInfo) {
        return indexInfo->getIndexId();
    } else {
        return INVALID_INDEXID;
    }
}

int32_t 
LegacyIndexInfoHelper::getFieldPosition(const string &indexName, const string &fieldname) const {
    assert(_indexInfos);
    const IndexInfo *indexInfo = _indexInfos->getIndexInfo(indexName.c_str());
    if (indexInfo) {
       return indexInfo->getFieldPosition(fieldname.c_str());
    } else {
        return -1;
    }
}

std::vector<std::string>
LegacyIndexInfoHelper::getFieldList(const std::string &indexName) const {
    assert(_indexInfos);

    std::vector<std::string> fields;
    const IndexInfo *indexInfo = _indexInfos->getIndexInfo(indexName.c_str());
    if (NULL == indexInfo) {
        HA3_LOG(TRACE3, "not found the IndexInfo, indexName: %s", indexName.c_str());
        return fields;
    }

    const map<string, fieldbitmap_t> &fieldMap = indexInfo->getFieldName2Bitmaps();
    for (map<string, fieldbitmap_t>::const_iterator it = fieldMap.begin();
         it != fieldMap.end(); it++)
    {
        fields.push_back(it->first);
    }
    return fields;
}
void LegacyIndexInfoHelper::setIndexInfos(const IndexInfos *indexInfos) {
    _indexInfos = indexInfos;
}

END_HA3_NAMESPACE(config);

