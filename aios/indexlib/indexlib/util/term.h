#ifndef __INDEXLIB_TERM_H
#define __INDEXLIB_TERM_H

#include <tr1/memory>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/util/lite_term.h"
#include <string>

IE_NAMESPACE_BEGIN(util);

class Term : public autil::legacy::Jsonizable
{
public:
    Term()
        : mHasValidHashKey(false)
    {
    }

    Term(const std::string& word, const std::string& indexName,
         const std::string& truncateName = "")
        : mWord(word)
        , mIndexName(indexName)
        , mTruncateName(truncateName)
        , mHasValidHashKey(false)
    {
    }

    // use indexId instead of indexName
    Term(const std::string& word, indexid_t indexId, 
         indexid_t truncIndexId = INVALID_INDEXID)
        : mWord(word)
        , mHasValidHashKey(false)
    {
        SetIndexId(indexId, truncIndexId);
    }
    
    /* --------------------------------------------------------------------
       only built-in inverted_index lookup can use lite term for optimize,
       which should make sure not use literal word & literal indexName. 
     * attention: spatial index & customize index should keep key word, should not use lite term
     * attention: range index & date index use NumberTerm for range search
     * --------------------------------------------------------------------*/
    Term(dictkey_t termHashKey, indexid_t indexId, 
         indexid_t truncIndexId = INVALID_INDEXID)
        : mHasValidHashKey(false)
    {
        EnableLiteTerm(termHashKey, indexId, truncIndexId);
    }
    
    Term(const Term& t)
    {
        if (&t != this)
        {
            mWord = t.mWord;
            mIndexName = t.mIndexName;
            mTruncateName = t.mTruncateName;
            mLiteTerm = t.mLiteTerm;
            mHasValidHashKey = t.mHasValidHashKey;
        }
    }
    virtual ~Term()
    {
    }
public:
    const std::string& GetWord() const
    {
        return mWord;
    }

    void SetWord(const std::string &word)
    {
        mWord = word;
    }
    
    const std::string& GetIndexName() const
    {
        return mIndexName;
    }

    void SetIndexName(const std::string &indexName)
    {
        mIndexName = indexName;
    }

    const std::string& GetTruncateName() const
    {
        return mTruncateName;
    }

    bool HasTruncateName() const { return !mTruncateName.empty(); }

    std::string GetTruncateIndexName() const
    {
        return mIndexName + '_' + mTruncateName;
    }

    void SetTruncateName(const std::string &truncateName)
    {
        mTruncateName = truncateName;
    }

    virtual std::string GetTermName() const {return "Term";}
    virtual bool operator == (const Term& term) const;
    bool operator != (const Term& term) const {return !(*this == term);}
    virtual std::string ToString() const;

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);

    void SetTermHashKey(dictkey_t termHashKey)
    {
        mLiteTerm.SetTermHashKey(termHashKey);
        mHasValidHashKey = true;
    }

    bool GetTermHashKey(dictkey_t &termHashKey) const
    {
        if (mHasValidHashKey)
        {
            termHashKey = mLiteTerm.GetTermHashKey();
            return true;
        }
        return false;
    }

    void EnableLiteTerm(dictkey_t termHashKey, indexid_t indexId, 
                        indexid_t truncIndexId = INVALID_INDEXID)
    {
        mLiteTerm = LiteTerm(indexId, termHashKey, truncIndexId);
        mHasValidHashKey = true;
    }
        
    const LiteTerm* GetLiteTerm() const
    {
        return (mLiteTerm.GetIndexId() == INVALID_INDEXID) ? NULL : &mLiteTerm;
    }

    void SetIndexId(indexid_t indexId, indexid_t truncIndexId = INVALID_INDEXID)
    {
        mLiteTerm.SetIndexId(indexId);
        mLiteTerm.SetTruncateIndexId(truncIndexId);
    }

protected:
    std::string mWord;
    std::string mIndexName;
    std::string mTruncateName;
    LiteTerm mLiteTerm;
    bool mHasValidHashKey;

private:
    IE_LOG_DECLARE();
};

std::ostream& operator <<(std::ostream &os, const Term& term);

typedef std::tr1::shared_ptr<Term> TermPtr;

IE_NAMESPACE_END(util);

#endif //__INDEXENGINETERM_H
