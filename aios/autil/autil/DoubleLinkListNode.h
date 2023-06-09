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

#include <stddef.h>


namespace autil {

template <class T>
class DoubleLinkListNode
{
public:
    DoubleLinkListNode();

public:
    void reset();

private:
    DoubleLinkListNode(const DoubleLinkListNode &);
public:
    DoubleLinkListNode<T> *_nextListNode;
    DoubleLinkListNode<T> *_prevListNode;
    T _data;
};

////////////////////////////////////////////////////
template<class T>
DoubleLinkListNode<T>::DoubleLinkListNode()
{
    reset();
}

template<class T>
inline void DoubleLinkListNode<T>::reset() {
    _nextListNode = NULL;
    _prevListNode = NULL;
}

}

