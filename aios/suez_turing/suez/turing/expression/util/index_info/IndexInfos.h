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

#include <map>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "indexlib/indexlib.h"
#include "suez/turing/expression/util/FieldBoost.h"
#include "suez/turing/expression/util/FieldBoostTable.h"

namespace suez {
namespace turing {

class IndexInfo {
public:
    IndexInfo();
    ~IndexInfo() {}

public:
    /* return the field count in this index.*/
    uint32_t getFieldCount() const;

    /* get the 'fieldbitmap' the given field: 0x01, 0x02, 0x03... 0x80 */
    fieldbitmap_t getFieldBitmap(const char *fieldName) const;

    /* get the internal field position in this 'PACK' or 'EXTRA_PACK' index. */
    int32_t getFieldPosition(const char *fieldName) const;

    const FieldBoost &getFieldBoost() const { return _fieldsBoost; }

    InvertedIndexType getIndexType() const { return indexType; }
    void setIndexType(const InvertedIndexType type) { indexType = type; }

    const std::map<std::string, fieldbitmap_t> &getFieldName2Bitmaps() const { return _name2fieldbitmap; }

    const std::string &getIndexName() const { return indexName; }
    void setIndexName(const char *idxName) { indexName = idxName; }

    const std::string &getAnalyzerName() const { return analyzerName; }
    void setAnalyzerName(const char *aName) { analyzerName = aName; }

    indexid_t getIndexId() const { return indexId; }

    bool addField(const char *fieldName, fieldboost_t boost);
    FieldType getSingleFieldType() const { return singleFieldType; }
    void setSingleFieldType(FieldType ft) { singleFieldType = ft; }
    indexlib::index::PrimaryKeyHashType getPrimaryKeyHashType() const { return indexHashType; }
    void setPrimaryKeyHashType(indexlib::index::PrimaryKeyHashType type) { indexHashType = type; }

public:
    indexid_t indexId;
    InvertedIndexType indexType;
    FieldType singleFieldType;
    indexlib::index::PrimaryKeyHashType indexHashType;
    bool isFieldIndex;
    bool isSubDocIndex;
    std::string indexName;
    std::string fieldName;
    std::string analyzerName;

private:
    FieldBoost _fieldsBoost;
    std::map<std::string, fieldbitmap_t> _name2fieldbitmap;
    fieldboost_t _totalFieldBoost;

private:
    AUTIL_LOG_DECLARE();
};

class IndexInfos {
public:
    typedef std::vector<IndexInfo *> IndexVector;
    typedef std::map<std::string, fieldid_t> NameMap;
    typedef IndexVector::iterator Iterator;
    typedef IndexVector::const_iterator ConstIterator;

public:
    IndexInfos();
    IndexInfos(const IndexInfos &indexInfos);
    ~IndexInfos();

public:
    void addIndexInfo(IndexInfo *indexInfo);
    const IndexInfo *getIndexInfo(const char *indexName) const;
    const IndexInfo *getIndexInfo(indexid_t indexId) const;
    std::vector<std::string> getIndexNameVector() const;
    uint32_t getIndexCount() const { return _indexInfos.size(); }
    const FieldBoostTable &getFieldBoostTable() const { return _fieldBoostTable; }

    Iterator begin() { return _indexInfos.begin(); }
    ConstIterator begin() const { return _indexInfos.begin(); }
    Iterator end() { return _indexInfos.end(); }
    ConstIterator end() const { return _indexInfos.end(); }
    void stealIndexInfos(IndexVector &indexIndexs);

private:
    IndexVector _indexInfos;
    NameMap _indexName2IdMap;
    FieldBoostTable _fieldBoostTable;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace turing
} // namespace suez
