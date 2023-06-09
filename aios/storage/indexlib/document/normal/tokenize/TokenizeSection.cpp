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
#include "indexlib/document/normal/tokenize/TokenizeSection.h"

using namespace autil;

namespace indexlib { namespace document {
AUTIL_LOG_SETUP(indexlib.document, TokenizeSection);

ThreadMutex TokenNodeAllocatorPool::_lock;
TokenNodeAllocatorPool::AllocatorMap TokenNodeAllocatorPool::_allocators;

TokenNodeAllocatorPool::TokenNodeAllocatorPool() {}

TokenNodeAllocatorPool::~TokenNodeAllocatorPool() {}

std::shared_ptr<TokenNodeAllocator> TokenNodeAllocatorPool::getAllocator()
{
    ScopedLock lock(_lock);
    pthread_t tid = pthread_self();
    AllocatorMap::iterator it = _allocators.find(tid);
    if (it != _allocators.end()) {
        return it->second;
    }
    auto allocator = std::make_shared<TokenNodeAllocator>();
    _allocators[tid] = allocator;
    return allocator;
}

void TokenNodeAllocatorPool::reset()
{
    ScopedLock lock(_lock);
    _allocators.clear();
}

TokenizeSection::TokenizeSection(const std::shared_ptr<TokenNodeAllocator>& tokenNodeAllocatorPtr)
{
    _tokenNodeAllocatorPtr = tokenNodeAllocatorPtr;
    _header = NULL;
    _basicLength = 0;
    _count = 0;
    _sectionWeight = 0;
}

TokenizeSection::~TokenizeSection()
{
    while (_header) {
        eraseExtend(_header);
        TokenNode* nextBasic = _header->getNextBasic();
        _tokenNodeAllocatorPtr->free(_header);
        _header = nextBasic;
    }
}

void TokenizeSection::eraseExtend(TokenNode* basicNode)
{
    if (!basicNode) {
        return;
    }
    TokenNode* extend = basicNode->getNextExtend();
    while (extend) {
        TokenNode* nextExtend = extend->getNextExtend();
        _tokenNodeAllocatorPtr->free(extend);
        extend = nextExtend;
        --_count;
    }
    basicNode->setNextExtend(NULL);
}

TokenizeSection::Iterator TokenizeSection::createIterator() { return Iterator(this); }

void TokenizeSection::insertBasicToken(const AnalyzerToken& token, Iterator& iterator)
{
    if (this != iterator._section) {
        return;
    }

    TokenNode* tokenNode = createTokenNode();
    assert(tokenNode);
    tokenNode->setToken(token);

    if (iterator.isBasicTail()) {
        iterator._preNode->setNextBasic(tokenNode);
    } else {
        if (_header == NULL) {
            _header = tokenNode;
            iterator._preNode = NULL;
        } else {
            insertBasicNode(tokenNode, iterator._basicNode);
            iterator._preNode = iterator._basicNode;
        }
    }

    iterator._basicNode = tokenNode;
    iterator._curNode = tokenNode;
    _basicLength++;
    _count++;
}

void TokenizeSection::insertExtendToken(const AnalyzerToken& token, Iterator& iterator)
{
    if (this != iterator._section || _header == NULL || iterator._curNode == NULL) {
        return;
    }
    // update section
    TokenNode* tokenNode = createTokenNode();
    tokenNode->setToken(token);

    insertExtendNode(tokenNode, iterator._curNode);
    iterator._preNode = iterator._curNode;
    iterator._curNode = tokenNode;
    _count++;
}

void TokenizeSection::insertBasicNode(TokenNode* dstNode, TokenNode* curNode)
{
    dstNode->setNextBasic(curNode->getNextBasic());
    curNode->setNextBasic(dstNode);
}

void TokenizeSection::insertExtendNode(TokenNode* dstNode, TokenNode* curNode)
{
    dstNode->setNextExtend(curNode->getNextExtend());
    curNode->setNextExtend(dstNode);
}

void TokenizeSection::erase(Iterator& iterator)
{
    if (this != iterator._section) {
        return;
    }

    TokenNode* curNode = iterator._curNode;
    if (curNode == NULL) {
        return;
    }
    if (curNode == iterator._basicNode) {
        eraseExtend(curNode);
        if (curNode == _header) {
            _header = _header->getNextBasic();
        }
        if (iterator._preNode != NULL) {
            iterator._preNode->setNextBasic(curNode->getNextBasic());
        }
        iterator._basicNode = curNode->getNextBasic();
        iterator._curNode = curNode->getNextBasic();
        _basicLength--;
    } else {
        iterator._preNode->setNextExtend(curNode->getNextExtend());
        iterator._curNode = curNode->getNextExtend();
    }
    _tokenNodeAllocatorPtr->free(curNode);
    _count--;
}

TokenNode* TokenizeSection::createTokenNode() { return _tokenNodeAllocatorPtr->allocate(); }

TokenizeSection::Iterator::Iterator()
{
    _basicNode = NULL;
    _curNode = NULL;
    _preNode = NULL;
    _section = NULL;
}

TokenizeSection::Iterator::Iterator(const TokenizeSection* tokenizeSection)
{
    _curNode = tokenizeSection->_header;
    _basicNode = tokenizeSection->_header;
    _preNode = NULL;
    _section = tokenizeSection;
}

bool TokenizeSection::Iterator::isBasicTail() { return _basicNode == NULL && _preNode != NULL; }

bool TokenizeSection::Iterator::next()
{
    if (!nextExtend() || _curNode == NULL) {
        return nextBasic();
    }
    return true;
}

bool TokenizeSection::Iterator::nextBasic()
{
    if (NULL != _basicNode) {
        _preNode = _basicNode;
        _basicNode = _basicNode->getNextBasic();
        _curNode = _basicNode;
        return _curNode != NULL;
    }
    return false;
}

bool TokenizeSection::Iterator::nextExtend()
{
    if (NULL != _curNode) {
        _preNode = _curNode;
        _curNode = _curNode->getNextExtend();
        return _curNode != NULL;
    }
    return false;
}

AnalyzerToken* TokenizeSection::Iterator::getToken() const
{
    if (_curNode != NULL) {
        return _curNode->getToken();
    }
    return NULL;
}
}} // namespace indexlib::document
