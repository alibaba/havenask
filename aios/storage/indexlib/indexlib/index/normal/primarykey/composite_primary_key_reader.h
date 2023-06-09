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

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/primary_key/PrimaryKeyIndexReader.h"

namespace indexlib { namespace index {

// Read from primary key index and temporary pk to doc id map. The pk to doc id map is used for batch build and
// keeps state for docs that are in the batch but not yet built.

// CompositePrimaryKeyReader是PrimaryKeyIndexReader在batch build场景的一个子类。用于处理batch
// build在batch构建过程未结束时，batch内doc没有及时更新到索引上，也没有更新比如partition
// info等信息，导致部分数据结构(这里是PrimaryKeyIndex)无法通过读索引直接找到。
// 这里只能将信息额外记录到CompositePrimaryKeyReader的内存态辅助结构中。
// 当CompositePrimaryKeyReader对应的batch结束时，临时的内存态辅助结构会实际写到索引中，之后索引状态会和流式构建一致。
class CompositePrimaryKeyReader
{
public:
    CompositePrimaryKeyReader() {}
    virtual ~CompositePrimaryKeyReader() {}

public:
    void Init(const std::shared_ptr<PrimaryKeyIndexReader>& primaryKeyIndexReader)
    {
        return Reinit(primaryKeyIndexReader);
    }
    void Reinit(const std::shared_ptr<PrimaryKeyIndexReader>& primaryKeyIndexReader)
    {
        assert(primaryKeyIndexReader);
        _primaryKeyIndexReader = primaryKeyIndexReader;
        _pkToDocIdMap.clear();
        _docIdToPkMap.clear();
    }
    void InsertOrUpdate(std::string pk, docid_t docId)
    {
        auto iter = _pkToDocIdMap.find(pk);
        if (iter != _pkToDocIdMap.end()) {
            _docIdToPkMap.erase(iter->second);
        }
        _pkToDocIdMap[pk] = docId;
        _docIdToPkMap[docId] = pk;

        assert(_pkToDocIdMap.size() == _docIdToPkMap.size());
    }
    std::shared_ptr<PrimaryKeyIndexReader> GetPrimaryKeyIndexReader() { return _primaryKeyIndexReader; }

public:
    // virtual for test
    virtual docid_t Lookup(const std::string& pkStr) const
    {
        if (_pkToDocIdMap.find(pkStr) != _pkToDocIdMap.end()) {
            return _pkToDocIdMap.at(pkStr);
        }
        return _primaryKeyIndexReader->Lookup(pkStr, _primaryKeyIndexReader->GetBuildExecutor());
    }

    void RemoveDocument(docid_t docId)
    {
        auto iter = _docIdToPkMap.find(docId);
        if (iter != _docIdToPkMap.end()) {
            _pkToDocIdMap.erase(iter->second);
            _docIdToPkMap.erase(iter);
        }

        assert(_pkToDocIdMap.size() == _docIdToPkMap.size());
    }

private:
    std::map<std::string, docid_t> _pkToDocIdMap;
    std::map<docid_t, std::string> _docIdToPkMap;
    std::shared_ptr<PrimaryKeyIndexReader> _primaryKeyIndexReader;

    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(CompositePrimaryKeyReader);

}} // namespace indexlib::index
