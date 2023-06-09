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
#include <stdint.h>
#include <iosfwd>
#include <string>

#include "autil/DataBuffer.h"

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil

namespace autil {

#define HLL_DENSE     0       /* 稠密编码方式，占用 16K 内存 */
#define HLL_SPARSE    1       /* 稀疏编码方式, 创建时用 1.5K 内存，在元素不断加入后，自行决定何时转换为稠密编码方式 */
#define HLL_P                       14                          /* The greater is P, the smaller the error. */
#define HLL_Q                       50                          /* (64-HLL_P) The number of bits of the hash value used for
                                                                    determining the number of leading zeros. */
#define HLL_REGISTERS               16384                       /* (1<<HLL_P) With P=14, 16384 registers. */
#define HLL_P_MASK                  16383                       /* (HLL_REGISTERS-1) Mask to index register. */

#define HLL_BITS                    6                           /* Enough to count up to 63 leading zeroes. */
#define HLL_REGISTER_MAX            63                          /* ((1<<HLL_BITS)-1) */

#define HLL_ALPHA_INF               0.721347520444481703680     /* constant for 0.5/ln(2) */

#define HLL_SERIAL_SPARSE_MIN       3000
#define HLL_SERIAL_SPARSE_BYTES     6144
#define HLL_SERIAL_DENSE_BYTES      HLL_REGISTERS               /* HLL_REGISTERS * 6 / 8 = 12288 */

#define HLL_MAGIC                   "HLL"
#define HLL_MAGIC_BYTES             3

#define VARINT32_MAX_BYTES          5

#define HLL_REGI_MAX                512                         /* 总共512个位置 */
#define HLL_REGI_MAX_BYTES          1536                        /* 总共 1.5K */
#define HLL_REGI_AREA_NUM           16                          /* 拆成16个子区域 */
#define HLL_REGI_AREA_SIZE          32                          /* 每个区域32个位置 */

struct HllRegi
{
    //桶号
    uint16_t  index;
    //值
    uint8_t   count;
}__attribute__ ((packed));

class HllCtx
{
public:
    HllCtx():_encoding(0), _eleNum(0), _registers(NULL), _regiArr(NULL) {}
    ~HllCtx() {}
public:
    /* HLL_DENSE or HLL_SPARSE. */
    uint8_t     _encoding;
    int         _eleNum;
    uint8_t *_registers;
    /* 总长 512*sizeof(HllRegi) ，
     * 拆分成32个子区域，每条放16个元素，只要有一个满了，就转成稠密
     * 假设：hash值是很均匀的 */
    HllRegi *_regiArr;
public:
    void serialize(autil::DataBuffer &dataBuffer) const;
    void deserialize(autil::DataBuffer &dataBuffer,
                     autil::mem_pool::Pool *pool);
    int ReadAndDecodeUnit32(autil::DataBuffer &dataBuffer);

    void EncodeAndWriteUint32(uint32_t value, autil::DataBuffer& dataBuffer) const;

    int varintEncodeUint32(uint32_t value, uint8_t *buf) const;

    std::string toString() const;
};

template <> inline void autil::DataBuffer::read(HllCtx &hllCtx) {
    hllCtx.deserialize(*this, _pool);
}

template <> inline void autil::DataBuffer::write(const HllCtx& hllCtx) {
    hllCtx.serialize(*this);
}

std::ostream& operator <<(std::ostream& stream, const HllCtx &x);

class Hyperloglog
{
public:
    Hyperloglog() {}
    ~Hyperloglog() {}

public:

    /**
     * 创建
     * encoding：预计可能加入的元素大概率 >512 时，请直接选择 HLL_DENSE, 提升性能
     * 如果不能确定，请选择 HLL_SPARSE
     *
     * 返回 NULL:表示失败
     */
    static HllCtx* hllCtxCreate( unsigned char encoding, autil::mem_pool::Pool *pool);

    /** 重置 */
    static void hllCtxReset(HllCtx *thiz);

    /** 获得基数统计的值 */
    static uint64_t hllCtxCount(const HllCtx *thiz);

    /**
     * 添加一个新的值
     *      返回值：-1：表示失败 0:表示成功
     */
    static int hllCtxAdd(HllCtx *thiz, const unsigned char *ele, int eleLen, autil::mem_pool::Pool* pool);

    /**
     * 合并
     *      返回值：-1：表示失败 0:表示成功
     */
    static int hllCtxMerge(HllCtx *thiz, HllCtx *forMerge, autil::mem_pool::Pool* pool);

private:
    static double hllSigma( double x );
    static double hllTau( double x );
    static int hllCtxRegiArrToRegisters( uint8_t * registers, HllRegi * regiArr );
    static int hllCtxSparseToDense( HllCtx * thiz, autil::mem_pool::Pool* pool );
    static int hllCtxMergeRegisters( uint8_t * dst, uint8_t * src );

    static uint64_t MurmurHash64A(const uint8_t *data, int len) {
        const uint64_t m = 0xc6a4a7935bd1e995ULL;
        uint64_t h = 0xadc83b19ULL ^ (len * m);
        const uint8_t *end = data + (len - (len & 7));

        while (data != end) {
            uint64_t k = *((uint64_t *)data);

            k *= m;
            k ^= k >> 47;
            k *= m;

            h ^= k;
            h *= m;

            data += 8;
        }

        switch (len & 7) {
            case 7:
                h ^= (uint64_t)data[6] << 48;
            case 6:
                h ^= (uint64_t)data[5] << 40;
            case 5:
                h ^= (uint64_t)data[4] << 32;
            case 4:
                h ^= (uint64_t)data[3] << 24;
            case 3:
                h ^= (uint64_t)data[2] << 16;
            case 2:
                h ^= (uint64_t)data[1] << 8;
            case 1:
                h ^= (uint64_t)data[0];
                h *= m;
        };

        h ^= h >> 47;
        h *= m;
        h ^= h >> 47;

        return h;
    }

    static int varintDecodeUint32( const uint8_t * buffer, uint32_t * value ) {
        const uint8_t *ptr = buffer;

        uint32_t b;
        uint32_t result;

        b = *(ptr++);
        result = (b & 0x7F);
        if (!(b & 0x80))
            goto done;
        b = *(ptr++);
        result |= (b & 0x7F) << 7;
        if (!(b & 0x80))
            goto done;
        b = *(ptr++);
        result |= (b & 0x7F) << 14;
        if (!(b & 0x80))
            goto done;
        b = *(ptr++);
        result |= (b & 0x7F) << 21;
        if (!(b & 0x80))
            goto done;
        b = *(ptr++);
        result |= b << 28;
        if (!(b & 0x80))
            goto done;

        for (uint32_t i = 0; i < VARINT32_MAX_BYTES; i++) {
            b = *(ptr++);
            if (!(b & 0x80))
                goto done;
        }
        return 0;

    done:
        *value = result;
        return (ptr - buffer);
    }

    static int varintEncodeUint32(uint32_t value, uint8_t *buf) {
        uint8_t *target = buf;

        target[0] = (uint8_t)(value | 0x80);

        if (value >= (1 << 7)) {
            target[1] = (uint8_t)((value >> 7) | 0x80);

            if (value >= (1 << 14)) {
                target[2] = (uint8_t)((value >> 14) | 0x80);

                if (value >= (1 << 21)) {
                    target[3] = (uint8_t)((value >> 21) | 0x80);

                    if (value >= (1 << 28)) {
                        target[4] = (uint8_t)(value >> 28);
                        return 5;
                    } else {
                        target[3] &= 0x7F;
                        return 4;
                    }
                } else {
                    target[2] &= 0x7F;
                    return 3;
                }
            } else {
                target[1] &= 0x7F;
                return 2;
            }
        } else {
            target[0] &= 0x7F;
            return 1;
        }
    }
};

}

