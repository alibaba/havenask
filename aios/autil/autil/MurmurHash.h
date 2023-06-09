#pragma once
/*
  Murmurhash from http://sites.google.com/site/murmurhash/

  All code is released to the public domain. For business purposes, Murmurhash is
  under the MIT license.
*/


#include <stdint.h>


namespace autil {

class MurmurHash
{
public:
    static uint64_t MurmurHash64A(const void * key, int len, uint64_t seed);
    static uint64_t MurmurHash64A(const void * key, int len, uint64_t seed, uint64_t salt); 
private:
    MurmurHash();
    ~MurmurHash();
    MurmurHash(const MurmurHash &);
    MurmurHash& operator = (const MurmurHash &);
};

}

