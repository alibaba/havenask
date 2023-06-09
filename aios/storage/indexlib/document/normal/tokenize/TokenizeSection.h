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

#include "autil/Lock.h"
#include "autil/ObjectAllocator.h"
#include "indexlib/document/normal/tokenize/AnalyzerToken.h"
#include "indexlib/index/inverted_index/Types.h"

namespace indexlib { namespace document {

class TokenNode
{
public:
    TokenNode()
    {
        _nextBasic = NULL;
        _nextExtend = NULL;
    }

public:
    TokenNode* getNextBasic() { return _nextBasic; }
    TokenNode* getNextExtend() { return _nextExtend; }
    void setNextBasic(TokenNode* tokenNode) { _nextBasic = tokenNode; }
    void setNextExtend(TokenNode* tokenNode) { _nextExtend = tokenNode; }
    AnalyzerToken* getToken() { return &_token; }
    void setToken(const AnalyzerToken& token) { _token = token; }

private:
    AnalyzerToken _token;
    TokenNode* _nextBasic;
    TokenNode* _nextExtend;
};

typedef autil::ObjectAllocator<TokenNode> TokenNodeAllocator;

class TokenNodeAllocatorPool
{
public:
    typedef std::map<pthread_t, std::shared_ptr<TokenNodeAllocator>> AllocatorMap;

public:
    TokenNodeAllocatorPool();
    ~TokenNodeAllocatorPool();

private:
    TokenNodeAllocatorPool(const TokenNodeAllocatorPool&);
    TokenNodeAllocatorPool& operator=(const TokenNodeAllocatorPool&);

public:
    static std::shared_ptr<TokenNodeAllocator> getAllocator();
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
    TokenizeSection(const std::shared_ptr<TokenNodeAllocator>& tokenNodeAllocatorPtr);
    ~TokenizeSection();

public:
    Iterator createIterator();

    void insertBasicToken(const AnalyzerToken& token, Iterator& iterator);
    void insertExtendToken(const AnalyzerToken& token, Iterator& iterator);
    void erase(Iterator& iterator);

    void setSectionWeight(section_weight_t sectionWeight) { _sectionWeight = sectionWeight; }
    section_weight_t getSectionWeight() { return _sectionWeight; }

    uint32_t getBasicLength() const { return _basicLength; }
    uint32_t getTokenCount() const { return _count; }

public:
    class Iterator
    {
    public:
        Iterator();
        explicit Iterator(const TokenizeSection* tokenizeSection);

    public:
        bool next();
        bool nextBasic();
        bool nextExtend();

        AnalyzerToken* getToken() const;
        AnalyzerToken* operator*() const { return getToken(); }
        operator bool() const { return getToken(); }

    private:
        bool isBasicTail();

    private:
        TokenNode* _basicNode;
        TokenNode* _curNode;
        TokenNode* _preNode;
        const TokenizeSection* _section;

    private:
        friend class TokenizeSection;
    };

private:
    void insertBasicNode(TokenNode* dstNode, TokenNode* curNode);
    void insertExtendNode(TokenNode* dstNode, TokenNode* curNode);
    TokenNode* createTokenNode();
    void eraseExtend(TokenNode* basicNode);

private:
    std::shared_ptr<TokenNodeAllocator> _tokenNodeAllocatorPtr;
    TokenNode* _header;
    uint32_t _basicLength;
    uint32_t _count;
    section_weight_t _sectionWeight;

private:
    friend class Iterator;

private:
    AUTIL_LOG_DECLARE();
};

}} // namespace indexlib::document
