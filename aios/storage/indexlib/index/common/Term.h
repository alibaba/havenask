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
#include <string>

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/index/common/LiteTerm.h"

namespace indexlib::index {

class Term : public autil::legacy::Jsonizable
{
public:
    // null term
    explicit Term(const std::string& indexName = "")
        : _indexName(indexName)
        , _hasValidHashKey(false)
        , _isNull(true)
        , _hintValues(-1)
    {
    }

    Term(const std::string& word, const std::string& indexName, const std::string& truncateName = "")
        : _word(word)
        , _indexName(indexName)
        , _truncateName(truncateName)
        , _hasValidHashKey(false)
        , _isNull(false)
        , _hintValues(-1)
    {
    }

    // use indexId instead of indexName
    Term(const std::string& word, indexid_t indexId, indexid_t truncIndexId = INVALID_INDEXID)
        : _word(word)
        , _hasValidHashKey(false)
        , _isNull(false)
        , _hintValues(-1)
    {
        SetIndexId(indexId, truncIndexId);
    }

    /* --------------------------------------------------------------------
       only built-in inverted_index lookup can use lite term for optimize,
       which should make sure not use literal word & literal indexName.
     * attention: spatial index & customize index should keep key word, should not use lite term
     * attention: range index & date index use NumberTerm for range search
     * --------------------------------------------------------------------*/
    Term(dictkey_t termHashKey, indexid_t indexId, indexid_t truncIndexId = INVALID_INDEXID)
        : _hasValidHashKey(false)
        , _isNull(false)
        , _hintValues(-1)
    {
        EnableLiteTerm(termHashKey, indexId, truncIndexId);
    }

    Term(const Term& t)
    {
        if (&t != this) {
            _word = t._word;
            _indexName = t._indexName;
            _truncateName = t._truncateName;
            _liteTerm = t._liteTerm;
            _hasValidHashKey = t._hasValidHashKey;
            _isNull = t._isNull;
            _hintValues = t._hintValues;
        }
    }
    Term& operator=(const Term& t)
    {
        if (&t != this) {
            _word = t._word;
            _indexName = t._indexName;
            _truncateName = t._truncateName;
            _liteTerm = t._liteTerm;
            _hasValidHashKey = t._hasValidHashKey;
            _isNull = t._isNull;
            _hintValues = t._hintValues;
        }
        return *this;
    }
    Term(Term&& t)
    {
        if (&t != this) {
            _word = std::move(t._word);
            _indexName = std::move(t._indexName);
            _truncateName = std::move(t._truncateName);
            _liteTerm = std::move(t._liteTerm);
            _hasValidHashKey = t._hasValidHashKey;
            _isNull = t._isNull;
            _hintValues = t._hintValues;
        }
    }
    Term& operator=(Term&& t)
    {
        if (&t != this) {
            _word = std::move(t._word);
            _indexName = std::move(t._indexName);
            _truncateName = std::move(t._truncateName);
            _liteTerm = std::move(t._liteTerm);
            _hasValidHashKey = t._hasValidHashKey;
            _isNull = t._isNull;
            _hintValues = t._hintValues;
        }
        return *this;
    }

    virtual ~Term() {}

public:
    const std::string& GetWord() const { return _word; }

    void SetWord(const std::string& word)
    {
        _word = word;
        _isNull = false;
    }

    const std::string& GetIndexName() const { return _indexName; }

    void SetIndexName(const std::string& indexName) { _indexName = indexName; }

    const std::string& GetTruncateName() const { return _truncateName; }

    bool HasTruncateName() const { return !_truncateName.empty(); }

    std::string GetTruncateIndexName() const { return _indexName + '_' + _truncateName; }

    void SetTruncateName(const std::string& truncateName) { _truncateName = truncateName; }

    virtual std::string GetTermName() const { return "Term"; }
    virtual bool operator==(const Term& term) const;
    bool operator!=(const Term& term) const { return !(*this == term); }
    virtual std::string ToString() const;

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

    void SetTermHashKey(dictkey_t termHashKey)
    {
        _liteTerm.SetTermHashKey(termHashKey);
        _hasValidHashKey = true;
    }

    bool GetTermHashKey(dictkey_t& termHashKey) const
    {
        if (_hasValidHashKey) {
            termHashKey = _liteTerm.GetTermHashKey();
            return true;
        }
        return false;
    }

    void EnableLiteTerm(dictkey_t termHashKey, indexid_t indexId, indexid_t truncIndexId = INVALID_INDEXID)
    {
        _liteTerm = LiteTerm(termHashKey, indexId, truncIndexId);
        _hasValidHashKey = true;
    }

    const LiteTerm* GetLiteTerm() const { return (_liteTerm.GetIndexId() == INVALID_INDEXID) ? NULL : &_liteTerm; }

    void SetIndexId(indexid_t indexId, indexid_t truncIndexId = INVALID_INDEXID)
    {
        _liteTerm.SetIndexId(indexId);
        _liteTerm.SetTruncateIndexId(truncIndexId);
    }

    void SetNull(bool flag) { _isNull = flag; }
    bool IsNull() const { return _isNull; }

    void SetHintValues(int32_t hintValues) { _hintValues = hintValues; }

    bool HasHintValues() const { return _hintValues != -1; }

    int32_t GetHintValues() const { return _hintValues; }

protected:
    std::string _word;
    std::string _indexName;
    std::string _truncateName;
    LiteTerm _liteTerm;
    bool _hasValidHashKey;
    bool _isNull;
    int32_t _hintValues;

private:
    AUTIL_LOG_DECLARE();
};

std::ostream& operator<<(std::ostream& os, const Term& term);

typedef std::shared_ptr<Term> TermPtr;
} // namespace indexlib::index
