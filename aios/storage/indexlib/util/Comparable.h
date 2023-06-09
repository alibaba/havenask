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
// ===================================================================
// Comparable.h - Class for comparing AVL tree nodes
// Written by Brad Appleton (1997)
// http://www.bradapp.com/ftp/src/libs/C++/AvlTrees.html

#pragma once

#include <memory>

namespace indexlib { namespace util {

using namespace std;

// cmp_t is an enumeration type indicating the result of a
// comparison.
//     NOTE: I would place this inside the Comparable class but
//           when I do, g++ complains when I use cmp_t. Even
//           when I prefix it with Comparable:: or Comparable<KeyType>::
//           (If you can get this working please let me know)
//
enum cmp_t {
    MIN_CMP = -1, // less than
    EQ_CMP = 0,   // equal to
    MAX_CMP = 1   // greater than
};

// Class "Comparable" corresponds to an arbitrary comparable element
// with a keyfield that has an ordering relation. The template parameter
// KeyType is the "type" of the keyfield
//
template <class KeyType>
class Comparable
{
private:
    KeyType myKey;

public:
    Comparable(KeyType key) : myKey(key) {};
    virtual ~Comparable() {};

    // Use default copy-ctor, assignment, & destructor

    // Compare this item against the given key & return the result
    virtual cmp_t Compare(KeyType key) const { return (key == myKey) ? EQ_CMP : ((key < myKey) ? MIN_CMP : MAX_CMP); }

    // Get the key-field of an item
    KeyType Key() const { return myKey; }
};
}} // namespace indexlib::util
