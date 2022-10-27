#ifndef __INDEXLIB_TOKENIZESECTION_H
#define __INDEXLIB_TOKENIZESECTION_H

#include <tr1/memory>
#include <autil/ObjectAllocator.h>
#include <autil/Lock.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/document/extend_document/tokenize/analyzer_token.h"

IE_NAMESPACE_BEGIN(document);

class TokenNode
{
public:
    TokenNode () {
        _nextBasic = NULL;
        _nextExtend = NULL;
    }
public:
    TokenNode* getNextBasic() { return _nextBasic; }
    TokenNode* getNextExtend() { return _nextExtend; }
    void setNextBasic(TokenNode *tokenNode) { _nextBasic = tokenNode; }
    void setNextExtend(TokenNode *tokenNode) { _nextExtend = tokenNode; }
    AnalyzerToken* getToken() { return &_token; }
    void setToken(const AnalyzerToken &token) { _token = token; }
private:
    AnalyzerToken _token;
    TokenNode *_nextBasic;
    TokenNode *_nextExtend;
};

typedef autil::ObjectAllocator<TokenNode> TokenNodeAllocator;
typedef std::tr1::shared_ptr<TokenNodeAllocator> TokenNodeAllocatorPtr;

class TokenNodeAllocatorPool
{
public:
    typedef std::map<pthread_t, TokenNodeAllocatorPtr> AllocatorMap;
public:
    TokenNodeAllocatorPool();
    ~TokenNodeAllocatorPool();
private:
    TokenNodeAllocatorPool(const TokenNodeAllocatorPool &);
    TokenNodeAllocatorPool& operator=(const TokenNodeAllocatorPool &);
public:
    static TokenNodeAllocatorPtr getAllocator();
    static void reset();
public:
    // for test
    static size_t size() { return _allocators.size(); }
private:
    static autil::ThreadMutex _lock;
    static AllocatorMap _allocators;
};

class TokenizeSection
{
public:
    class Iterator;
public:
    TokenizeSection(const TokenNodeAllocatorPtr &tokenNodeAllocatorPtr);
    ~TokenizeSection();
public:
    Iterator createIterator();

    void insertBasicToken(const AnalyzerToken &token,
                          Iterator &iterator);
    void insertExtendToken(const AnalyzerToken &token,
                           Iterator &iterator);
    void erase(Iterator &iterator);

    void setSectionWeight(section_weight_t sectionWeight) { _sectionWeight = sectionWeight; }
    section_weight_t getSectionWeight() { return _sectionWeight; }

    uint32_t getBasicLength () const { return _basicLength; }
    uint32_t getTokenCount() const { return _count; }
public:
    class Iterator
    {
    public:
        Iterator();
        explicit Iterator(const TokenizeSection *tokenizeSection);
    public:
        bool next();
        bool nextBasic();
        bool nextExtend();

        AnalyzerToken* getToken() const;
        AnalyzerToken* operator *() const { return getToken(); }
        operator bool () const {return getToken();}
    private:
        bool isBasicTail();
    private:
        TokenNode *_basicNode;
        TokenNode *_curNode;
        TokenNode *_preNode;
        const TokenizeSection* _section;
    private:
        friend class TokenizeSection;
    };

private:
    void insertBasicNode(TokenNode *dstNode, TokenNode *curNode);
    void insertExtendNode(TokenNode *dstNode, TokenNode *curNode);
    TokenNode* createTokenNode();
    void eraseExtend(TokenNode* basicNode);

private:
    TokenNodeAllocatorPtr _tokenNodeAllocatorPtr;
    TokenNode *_header;
    uint32_t _basicLength;
    uint32_t _count;
    section_weight_t _sectionWeight;

private:
    friend class Iterator;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TokenizeSection);

IE_NAMESPACE_END(document);

#endif //__INDEXLIB_TOKENIZESECTION_H
