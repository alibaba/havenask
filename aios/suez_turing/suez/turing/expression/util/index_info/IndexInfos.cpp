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
#include "suez/turing/expression/util/IndexInfos.h"

#include <cstddef>
#include <utility>

#include "alog/Logger.h"

using namespace std;

namespace suez {
namespace turing {

AUTIL_LOG_SETUP(expression, IndexInfos);
AUTIL_LOG_SETUP(expression, IndexInfo);

IndexInfo::IndexInfo() {
    indexId = INVALID_INDEXID;
    indexType = it_unknown;
    singleFieldType = ft_unknown;
    indexHashType = indexlib::index::pk_default_hash;
    isFieldIndex = false;
    analyzerName = "aliws";
    _totalFieldBoost = 0;
    isSubDocIndex = false;
}

uint32_t IndexInfo::getFieldCount() const { return (uint32_t)_name2fieldbitmap.size(); }

fieldbitmap_t IndexInfo::getFieldBitmap(const char *fieldName) const {
    map<string, fieldbitmap_t>::const_iterator it = _name2fieldbitmap.find(fieldName);
    if (it != _name2fieldbitmap.end()) {
        return it->second;
    }
    return INVALID_FIELDBITMAP;
}

int32_t IndexInfo::getFieldPosition(const char *fieldName) const {
    fieldbitmap_t fieldBit = getFieldBitmap(fieldName);
    if (INVALID_FIELDBITMAP == fieldBit) {
        return -1;
    }
    int32_t bitPosition = -1;
    while (fieldBit) {
        ++bitPosition;
        fieldBit >>= 1;
    }

    return bitPosition;
}

bool IndexInfo::addField(const char *fieldName, fieldboost_t boost) {
    map<string, fieldbitmap_t>::const_iterator it = _name2fieldbitmap.find(fieldName);
    if (it != _name2fieldbitmap.end()) {
        AUTIL_LOG(WARN, "The fieldName:[%s] is already exist", fieldName);
        return false;
    }
    int32_t fieldCount = getFieldCount();
    if (FieldBoost::FIELDS_LIMIT == fieldCount + 1) {
        AUTIL_LOG(WARN, "beyond the limit of fields in the index:%s", indexName.c_str());
        return false;
    }
    fieldbitmap_t fieldBitmap = 0x01 << fieldCount;
    _name2fieldbitmap.insert(make_pair(fieldName, fieldBitmap));
    _fieldsBoost[fieldCount] = boost;
    _totalFieldBoost += boost;
    return true;
}

///////////////////////////////////////////////////////////////
// IndexInfos
///////////////////////////////////////////////////////////////

IndexInfos::IndexInfos() {}

IndexInfos::IndexInfos(const IndexInfos &indexInfos) {
    for (IndexVector::const_iterator it = indexInfos.begin(); it != indexInfos.end(); it++) {
        addIndexInfo(new IndexInfo(*(*it)));
    }
}

IndexInfos::~IndexInfos() {
    for (IndexVector::iterator it = _indexInfos.begin(); it != _indexInfos.end(); it++) {
        delete *it;
    }
    _indexInfos.clear();
    _indexName2IdMap.clear();
    _fieldBoostTable.clear();
}

const IndexInfo *IndexInfos::getIndexInfo(const char *indexName) const {
    NameMap::const_iterator it = _indexName2IdMap.find(indexName);
    if (it == _indexName2IdMap.end()) {
        AUTIL_LOG(TRACE2, "NOT find the indexname: %s", indexName);
        return NULL;
    }
    return getIndexInfo(it->second);
}

const IndexInfo *IndexInfos::getIndexInfo(indexid_t id) const {
    if (id >= _indexInfos.size()) {
        AUTIL_LOG(TRACE3, "the indexId:[%d] is out of bound[%ld]", id, _indexInfos.size());
        return NULL;
    }
    return _indexInfos[id];
}

void IndexInfos::addIndexInfo(IndexInfo *indexInfo) {
    NameMap::iterator it = _indexName2IdMap.find(indexInfo->indexName);
    if (it != _indexName2IdMap.end()) {
        AUTIL_LOG(WARN, "Duplicated index: id:%d, name:%s", indexInfo->indexId, indexInfo->indexName.c_str());
        delete indexInfo;
        return;
    }
    indexInfo->indexId = _indexInfos.size();
    AUTIL_LOG(DEBUG, "add index: id:%d, name:%s", indexInfo->indexId, indexInfo->indexName.c_str());
    _indexInfos.push_back(indexInfo);
    _indexName2IdMap.insert(make_pair(indexInfo->indexName, indexInfo->indexId));
    _fieldBoostTable.setFieldBoost(indexInfo->indexId, indexInfo->getFieldBoost());
}

vector<string> IndexInfos::getIndexNameVector() const {
    vector<string> nameVec;
    for (IndexVector::const_iterator it = _indexInfos.begin(); it != _indexInfos.end(); ++it) {
        nameVec.push_back((*it)->getIndexName());
    }
    return nameVec;
}
void IndexInfos::stealIndexInfos(IndexVector &indexInfos) {
    indexInfos = _indexInfos;
    _indexInfos.clear();
    _indexName2IdMap.clear();
    _fieldBoostTable.clear();
}

} // namespace turing
} // namespace suez
