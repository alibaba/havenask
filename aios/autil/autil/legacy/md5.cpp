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
#include "autil/legacy/md5.h"

#include <stdio.h>
#include <cstring>
#include <iosfwd>

using namespace std;

/////////////////////////////////////////////// MACRO //////////////////////////////////////////////////
#define SHIFT_LEFT(a, b) ((a) << (b) | (a) >> (32 - b))

/**
 * each operation
 */
#define F(b, c, d) (((b) & (c)) | ( (~(b)) & (d)))
#define G(b, c, d) (((d) & (b)) | ( (~(d)) & (c)))
#define H(b, c, d) ((b) ^ (c) ^ (d))
#define I(b, c, d) ((c) ^ ((b) | (~(d))))

/**
 * each round
 */
#define FF(a, b, c, d, word, shift, k) \
{ \
    (a) += F((b), (c), (d)) + (word) + (k); \
    (a) = SHIFT_LEFT((a), (shift)); \
    (a) += (b); \
}
#define GG(a, b, c, d, word, shift, k) \
{ \
    (a) += G((b), (c), (d)) + (word) + (k); \
    (a) = SHIFT_LEFT((a), (shift)); \
    (a) += (b); \
}
#define HH(a, b, c, d, word, shift, k) \
{ \
    (a) += H((b), (c), (d)) + (word) + (k); \
    (a) = SHIFT_LEFT((a), (shift)); \
    (a) += (b); \
}
#define II(a, b, c, d, word, shift, k) \
{ \
    (a) += I((b), (c), (d)) + (word) + (k); \
    (a) = SHIFT_LEFT((a), (shift)); \
    (a) += (b); \
}

namespace autil
{
namespace legacy
{

//////////////////////////////////////////////////////// LOCAL DECLEARATION //////////////////////////////////////////////////////////
struct Md5Block
{
    uint32_t word[16];
};

void Initialize(void);
void CalMd5(struct Md5Block block, uint32_t h[4]);
void CopyBytesToBlock(const uint8_t* poolIn, struct Md5Block& block);
void Print(struct Md5Block block);
void GenerateK(void);

/**
* the little endian case of Md5 calculation
*
* @param poolIn Input data
* @param inputBytesNum Length of input data
* @param md5[16] A 128-bit pool for storing md5
*/
void DoMd5Little(const uint8_t* poolIn, const uint64_t inputBytesNum, uint8_t md5[16]);

/**
* the big endian case of Md5 calculation,
* regard four bytes as a 32-bit little endian word
*
* @param poolIn Input data
* @param inputBytesNum Length of input data
* @param md5[16] A 128-bit pool for storing md5
*/
void DoMd5Big(const uint8_t* poolIn, const uint64_t inputBytesNum, uint8_t md5[16]);

////////////////////////////////////////////////////////// GLOBAL VARIABLE /////////////////////////////////////////////////////////
const uint8_t gPadding[64] =
{
    0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
    0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
    0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
    0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
    0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
    0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
    0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
    0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0
};

////////////////////////////////////////////////////// PUBLIC ///////////////////////////////////////////////////////////////
void DoMd5(const uint8_t* poolIn, const uint64_t inputBytesNum, uint8_t md5[16])
{
    ///detect big or little endian
    union
    {
        uint32_t a;
        uint8_t b;
    } symbol;

    symbol.a = 1;

    ///for little endian
    if (symbol.b == 1)
    {
        DoMd5Little(poolIn, inputBytesNum, md5);
    }
    ///for big endian
    else
    {
        DoMd5Big(poolIn, inputBytesNum, md5);
    }
}///DoMd5

bool CheckMd5(const uint8_t* poolIn, const uint64_t inputBytesNum, const uint8_t md5[16])
{
    uint8_t newMd5[16];
    DoMd5(poolIn, inputBytesNum, newMd5);

    ///check each byte
    for (uint32_t i = 0; i < 16; ++i)
    {
        if (newMd5[i] != md5[i])
        {
            return false;
        }
    }

    return true;
}

//////////////////////////////////////////////// PUBLIC ///////////////////////////////////////////////
Md5Stream::Md5Stream()
{
    ///detect big or little endian
    union
    {
        uint32_t a;
        uint8_t b;
    } symbol;

    symbol.a = 1;

    ///for little endian
    if (symbol.b == 1)
    {
        mLittleEndian = true;
    }
    ///for big endian
    else
    {
        mLittleEndian = false;
    }

    Initailize();
} /// Md5Stream::Md5Stream()

void Md5Stream::Put(const uint8_t* poolIn, const uint64_t inputBytesNum)
{
    Md5Block block;

    mTotalBytes += inputBytesNum;
    
    uint64_t length = inputBytesNum + mBufPosition;
    uint64_t fullLen = (length >> 6) << 6;   ///complete blocked length
    // /* uint64_t partLen = */length & 0x3F;        ///length remained
   
    uint32_t i;
    if (fullLen >= 64)
    {
        i = 64 - mBufPosition;
        memcpy(&mBuf[mBufPosition], poolIn, i);
        
        if (mLittleEndian)
        {
            memcpy(block.word, mBuf, 64);
        }
        else
        {
            CopyBytesToBlock(mBuf, block);
        }
        CalMd5(block, mH);
        
        for (; i < fullLen - mBufPosition; i += 64)
        {
            if (mLittleEndian)
            {
                memcpy(block.word, &poolIn[i], 64);
            }
            else
            {
                CopyBytesToBlock(&poolIn[i], block);
            }
            
            CalMd5(block, mH);
        }
        
        mBufPosition = inputBytesNum - i;
        memcpy(mBuf, &poolIn[i], mBufPosition);
    }
    else
    {
        memcpy(&mBuf[mBufPosition], poolIn, inputBytesNum);
        mBufPosition += inputBytesNum;
    }
} /// Md5Stream::Put()

std::string Md5Stream::GetMd5String()
{
    uint8_t hash[16];
    Get(hash);
    
    char buffer[33] = {0};
    snprintf(buffer, sizeof(buffer),
             "%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x",
             hash[0], hash[1], hash[2], hash[3],
             hash[4], hash[5], hash[6], hash[7],
             hash[8], hash[9], hash[10], hash[11],
             hash[12], hash[13], hash[14], hash[15]);
    return std::string(buffer);
}

void Md5Stream::Get(uint8_t hash[16])
{
    Md5Block block;
    Md5Block tempBlock;
    
    if (mBufPosition > 55)        ///append two more blocks
    {
        ///copy input data into block and pad
        if (mLittleEndian)
        {
            memcpy(block.word, mBuf, mBufPosition);
            memcpy( ((uint8_t*)&(block.word[mBufPosition >> 2])) + (mBufPosition & 0x3), gPadding, (64 - mBufPosition));
        }
        else
        {
            memcpy(tempBlock.word, mBuf, mBufPosition);
            memcpy( ((uint8_t*)&(tempBlock.word[mBufPosition >> 2])) + (mBufPosition & 0x3), gPadding, (64 - mBufPosition));            
            CopyBytesToBlock((uint8_t*)tempBlock.word, block);
        }

        ///calculate Md5
        CalMd5(block, mH);

        ///set rest byte to 0x0
        memset(block.word, 0x0, 64);
    }
    else        ///append one more block
    {
        ///copy input data into block and pad
        memcpy(block.word, mBuf, mBufPosition);
        memcpy( ((uint8_t*)&(block.word[mBufPosition >> 2])) + (mBufPosition & 0x3), gPadding, (64 - mBufPosition));
    }

    ///append length (bits)
    uint64_t bitsNum = mTotalBytes * 8;
    memcpy(&(block.word[14]), &bitsNum, 8);

    if (mLittleEndian)
    {
        ///calculate Md5
        CalMd5(block, mH);
    }
    else
    {
        CopyBytesToBlock((uint8_t*)block.word, tempBlock);
        CalMd5(tempBlock, mH);
    }

    ///clear sensitive information
    memset(block.word, 0, 64);

    ///fill hash value
    memcpy(&(hash[0]), &(mH[0]), 16);
    
    Initailize();
} /// Md5Stream::Get()

////////////////////////////////////////////////////////// PRIVATE /////////////////////////////////////////////////////////
void Md5Stream::Initailize()
{
    mH[0] = 0x67452301;
    mH[1] = 0xEFCDAB89;
    mH[2] = 0x98BADCFE;
    mH[3] = 0x10325476;    
    mBufPosition = 0;
    memset(mBuf, 0, 64);

    mTotalBytes = 0;
} /// Md5Stream::Initailize()


void DoMd5Little(const uint8_t* poolIn, const uint64_t inputBytesNum, uint8_t hash[16])
{
    struct Md5Block block;

    ///initialize hash value
    uint32_t h[4];
    h[0] = 0x67452301;
    h[1] = 0xEFCDAB89;
    h[2] = 0x98BADCFE;
    h[3] = 0x10325476;

    ///padding and divide input data into blocks
    uint64_t fullLen = (inputBytesNum >> 6) << 6;    ///complete blocked length
    uint64_t partLen = inputBytesNum & 0x3F;        ///length remained

    uint32_t i;
    for (i = 0; i < fullLen; i += 64)
    {
        ///copy input data into block
        memcpy(block.word, &(poolIn[i]), 64);

        ///calculate Md5
        CalMd5(block, h);
    }


    if (partLen > 55)        ///append two more blocks
    {
        ///copy input data into block and pad
        memcpy(block.word, &(poolIn[i]), partLen);
        memcpy( ((uint8_t*)&(block.word[partLen >> 2])) + (partLen & 0x3), gPadding, (64 - partLen));

        ///calculate Md5
        CalMd5(block, h);

        ///set rest byte to 0x0
        memset(block.word, 0x0, 64);
    }
    else        ///append one more block
    {
        ///copy input data into block and pad
        memcpy(block.word, &(poolIn[i]), partLen);
        memcpy( ((uint8_t*)&(block.word[partLen >> 2])) + (partLen & 0x3), gPadding, (64 - partLen));
    }

    ///append length (bits)
    uint64_t bitsNum = inputBytesNum * 8;
    memcpy(&(block.word[14]), &bitsNum, 8);

    ///calculate Md5
    CalMd5(block, h);

    ///clear sensitive information
    memset(block.word, 0, 64);

    ///fill hash value
    memcpy(&(hash[0]), &(h[0]), 16);
}///DoMd5Little

void DoMd5Big(const uint8_t* poolIn, const uint64_t inputBytesNum, uint8_t hash[16])
{
    struct Md5Block block;
    uint8_t tempBlock[64];

    ///initialize hash value
    uint32_t h[4];
    h[0] = 0x67452301;
    h[1] = 0xEFCDAB89;
    h[2] = 0x98BADCFE;
    h[3] = 0x10325476;

    ///padding and divide input data into blocks
    uint64_t fullLen = (inputBytesNum >> 6) << 6;
    uint64_t partLen = inputBytesNum & 0x3F;

    uint32_t i;
    for (i = 0; i < fullLen; i += 64)
    {
        ///copy input data into block, in little endian
        CopyBytesToBlock(&(poolIn[i]), block);

        ///calculate Md5
        CalMd5(block, h);
    }

    ///append two more blocks
    if (partLen > 55)
    {
        ///put input data into a temporary block
        memcpy(tempBlock, &(poolIn[i]), partLen);
        memcpy(&(tempBlock[partLen]), gPadding, (64 - partLen));

        ///copy temporary data into block, in little endian
        CopyBytesToBlock(tempBlock, block);

        ///calculate Md5
        CalMd5(block, h);

        memset(tempBlock, 0x0, 64);
    }
    ///append one more block
    else
    {
        memcpy(tempBlock, &(poolIn[i]), partLen);
        memcpy( &(tempBlock[partLen]), gPadding, (64 - partLen));
    }
    ///append length (bits)
    uint64_t bitsNum = inputBytesNum * 8;
    memcpy(&(tempBlock[56]), &bitsNum, 8);

    ///copy temporary data into block, in little endian
    CopyBytesToBlock(tempBlock, block);

    ///calculate Md5
    CalMd5(block, h);

    ///clear sensitive information
    memset(block.word, 0, 64);
    memset(tempBlock, 0, 64);

    ///fill hash value
    memcpy(&(hash[0]), &(h[0]), 16);
}///DoMd5Big

/**
 * copy a pool into a block, using little endian
 */
void CopyBytesToBlock(const uint8_t* poolIn, struct Md5Block& block)
{
    uint32_t j = 0;
    for (uint32_t i = 0; i < 32; ++i, j += 4)
    {
        block.word[i] = ((uint32_t) poolIn[j])
                    | (((uint32_t) poolIn[j+1]) << 8)
                    | (((uint32_t) poolIn[j+2]) << 16)
                    | (((uint32_t) poolIn[j+3]) << 24);
    }
}

/**
 * calculate md5 hash value from a block
 */
void CalMd5(struct Md5Block block, uint32_t h[4])
{
    uint32_t a = h[0];
    uint32_t b = h[1];
    uint32_t c = h[2];
    uint32_t d = h[3];

    // Round 1
    FF (a, b, c, d, block.word[0], 7, 0xd76aa478);
    FF (d, a, b, c, block.word[1], 12, 0xe8c7b756);
    FF (c, d, a, b, block.word[2], 17, 0x242070db);
    FF (b, c, d, a, block.word[3], 22, 0xc1bdceee);
    FF (a, b, c, d, block.word[4], 7, 0xf57c0faf);
    FF (d, a, b, c, block.word[5], 12, 0x4787c62a);
    FF (c, d, a, b, block.word[6], 17, 0xa8304613);
    FF (b, c, d, a, block.word[7], 22, 0xfd469501);
    FF (a, b, c, d, block.word[8], 7, 0x698098d8);
    FF (d, a, b, c, block.word[9], 12, 0x8b44f7af);
    FF (c, d, a, b, block.word[10], 17, 0xffff5bb1);
    FF (b, c, d, a, block.word[11], 22, 0x895cd7be);
    FF (a, b, c, d, block.word[12], 7, 0x6b901122);
    FF (d, a, b, c, block.word[13], 12, 0xfd987193);
    FF (c, d, a, b, block.word[14], 17, 0xa679438e);
    FF (b, c, d, a, block.word[15], 22, 0x49b40821);

    // Round 2
    GG (a, b, c, d, block.word[1], 5, 0xf61e2562);
    GG (d, a, b, c, block.word[6], 9, 0xc040b340);
    GG (c, d, a, b, block.word[11], 14, 0x265e5a51);
    GG (b, c, d, a, block.word[0], 20, 0xe9b6c7aa);
    GG (a, b, c, d, block.word[5], 5, 0xd62f105d);
    GG (d, a, b, c, block.word[10], 9,  0x2441453);
    GG (c, d, a, b, block.word[15], 14, 0xd8a1e681);
    GG (b, c, d, a, block.word[4], 20, 0xe7d3fbc8);
    GG (a, b, c, d, block.word[9], 5, 0x21e1cde6);
    GG (d, a, b, c, block.word[14], 9, 0xc33707d6);
    GG (c, d, a, b, block.word[3], 14, 0xf4d50d87);
    GG (b, c, d, a, block.word[8], 20, 0x455a14ed);
    GG (a, b, c, d, block.word[13], 5, 0xa9e3e905);
    GG (d, a, b, c, block.word[2], 9, 0xfcefa3f8);
    GG (c, d, a, b, block.word[7], 14, 0x676f02d9);
    GG (b, c, d, a, block.word[12], 20, 0x8d2a4c8a);

    // Round 3
    HH (a, b, c, d, block.word[5], 4, 0xfffa3942);
    HH (d, a, b, c, block.word[8], 11, 0x8771f681);
    HH (c, d, a, b, block.word[11], 16, 0x6d9d6122);
    HH (b, c, d, a, block.word[14], 23, 0xfde5380c);
    HH (a, b, c, d, block.word[1], 4, 0xa4beea44);
    HH (d, a, b, c, block.word[4], 11, 0x4bdecfa9);
    HH (c, d, a, b, block.word[7], 16, 0xf6bb4b60);
    HH (b, c, d, a, block.word[10], 23, 0xbebfbc70);
    HH (a, b, c, d, block.word[13], 4, 0x289b7ec6);
    HH (d, a, b, c, block.word[0], 11, 0xeaa127fa);
    HH (c, d, a, b, block.word[3], 16, 0xd4ef3085);
    HH (b, c, d, a, block.word[6], 23,  0x4881d05);
    HH (a, b, c, d, block.word[9], 4, 0xd9d4d039);
    HH (d, a, b, c, block.word[12], 11, 0xe6db99e5);
    HH (c, d, a, b, block.word[15], 16, 0x1fa27cf8);
    HH (b, c, d, a, block.word[2], 23, 0xc4ac5665);

    // Round 4
    II (a, b, c, d, block.word[0], 6, 0xf4292244);
    II (d, a, b, c, block.word[7], 10, 0x432aff97);
    II (c, d, a, b, block.word[14], 15, 0xab9423a7);
    II (b, c, d, a, block.word[5], 21, 0xfc93a039);
    II (a, b, c, d, block.word[12], 6, 0x655b59c3);
    II (d, a, b, c, block.word[3], 10, 0x8f0ccc92);
    II (c, d, a, b, block.word[10], 15, 0xffeff47d);
    II (b, c, d, a, block.word[1], 21, 0x85845dd1);
    II (a, b, c, d, block.word[8], 6, 0x6fa87e4f);
    II (d, a, b, c, block.word[15], 10, 0xfe2ce6e0);
    II (c, d, a, b, block.word[6], 15, 0xa3014314);
    II (b, c, d, a, block.word[13], 21, 0x4e0811a1);
    II (a, b, c, d, block.word[4], 6, 0xf7537e82);
    II (d, a, b, c, block.word[11], 10, 0xbd3af235);
    II (c, d, a, b, block.word[2], 15, 0x2ad7d2bb);
    II (b, c, d, a, block.word[9], 21, 0xeb86d391);

    // Add to hash value
    h[0] += a;
    h[1] += b;
    h[2] += c;
    h[3] += d;
}

}}///namespace autil::legacy

//////////////////////////////////////////////////////////////// HIDED /////////////////////////////////////////////////////////////////////
/** The details and concepts of md5 implemention
 */
#if 0
/**
 * print data in a block,  please include <iomanip>
 */
void Print(struct Md5Block block)
{
    for (uint32_t i = 0; i < 16; ++i)
    {
        cout << setfill('0') << setw(8) << hex << block.word[i] << endl;
    }
}

/**
 * table r for rotate shifting each operation
 */
const uint32_t r[64] = {
    7, 12, 17, 22,    7, 12, 17, 22,    7, 12, 17, 22,    7, 12, 17, 22,    //  0 .. 15
    5,  9, 14, 20,    5,  9, 14, 20,    5,  9, 14, 20,    5,  9, 14, 20,    // 16 .. 31
    4, 11, 16, 23,    4, 11, 16, 23,    4, 11, 16, 23,    4, 11, 16, 23,    // 32 .. 47
    6, 10, 15, 21,    6, 10, 15, 21,    6, 10, 15, 21,    6, 10, 15, 21     // 48 .. 63
    };
/**
 * table k used in each operation
 */
const uint32_t k[64] = {
    0xd76aa478, 0xe8c7b756, 0x242070db, 0xc1bdceee,
    0xf57c0faf, 0x4787c62a, 0xa8304613, 0xfd469501,
    0x698098d8, 0x8b44f7af, 0xffff5bb1, 0x895cd7be,
    0x6b901122, 0xfd987193, 0xa679438e, 0x49b40821,
    0xf61e2562, 0xc040b340, 0x265e5a51, 0xe9b6c7aa,
    0xd62f105d, 0x02441453, 0xd8a1e681, 0xe7d3fbc8,
    0x21e1cde6, 0xc33707d6, 0xf4d50d87, 0x455a14ed,
    0xa9e3e905, 0xfcefa3f8, 0x676f02d9, 0x8d2a4c8a,
    0xfffa3942, 0x8771f681, 0x6d9d6122, 0xfde5380c,
    0xa4beea44, 0x4bdecfa9, 0xf6bb4b60, 0xbebfbc70,
    0x289b7ec6, 0xeaa127fa, 0xd4ef3085, 0x04881d05,
    0xd9d4d039, 0xe6db99e5, 0x1fa27cf8, 0xc4ac5665,
    0xf4292244, 0x432aff97, 0xab9423a7, 0xfc93a039,
    0x655b59c3, 0x8f0ccc92, 0xffeff47d, 0x85845dd1,
    0x6fa87e4f, 0xfe2ce6e0, 0xa3014314, 0x4e0811a1,
    0xf7537e82, 0xbd3af235, 0x2ad7d2bb, 0xeb86d391
    };

/**
 * a function generates table k, please include <cmath>
 */
void GenerateK(void)
{
    for (uint32_t i = 0; i < 64; ++i)
    {
        k[i] = floor( abs( sin(i + 1) ) * ( ((uint64_t) 1) << 32 ) );
        // fout << "0x" << hex << k[i] << "," << endl;
    }
}

/**
 * a easy-understanding implementation of CalMd5
 * but with lower efficiency
 */
void CalMd5(struct Md5Block block)
{
    uint32_t a = h[0];
    uint32_t b = h[1];
    uint32_t c = h[2];
    uint32_t d = h[3];

    uint32_t f;
    uint32_t g;
    uint32_t temp;

    for (uint32_t i = 0; i < 64; ++i)
    {
        // Round 1
        if (i < 16)
        {
            f = (b & c) | ( (~b) & d);
            g = i;
        }
        // Round 2
        else if (i < 32)
        {
            f = (d & b) | ( (~d) & c);
            g = (5 * i + 1) % 16;
        }
        // Round 3
        else if (i < 48)
        {
            f = b ^ c ^ d;
            g = (3 * i + 5) % 16;
        }
        // Round 4
        else
        {
            f = c ^ (b | (~d));
            g = (7 * i) % 16;
        }

        temp = d;
        d = c;
        c = b;
        b = b + SHIFT_LEFT( (a + f + k[i] + block.word[g]), r[i] );
        a = temp;
    }

    // Add to hash value
    h[0] += a;
    h[1] += b;
    h[2] += c;
    h[3] += d;
}

#endif ///The details and concepts of md5 implemention
