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
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <ostream>
#include <string>
#include <cmath>

#include "autil/DataBuffer.h"
#include "autil/Hyperloglog.h"
#include "autil/Log.h"
#include "autil/mem_pool/Pool.h"

namespace autil {
AUTIL_DECLARE_AND_SETUP_LOGGER(autil, Hyperloglog);

#pragma pack(1)
#pragma pack()

std::ostream& operator <<(std::ostream& stream, const HllCtx &x) {
    stream << x.toString();
    return stream;
}

void HllCtx::serialize(autil::DataBuffer &dataBuffer) const {
    dataBuffer.write(_encoding);
    EncodeAndWriteUint32(_eleNum, dataBuffer);

    if (HLL_SPARSE == _encoding) {
      dataBuffer.writeBytes((char *)_regiArr, HLL_REGI_MAX_BYTES);
      return;
    }
    if (_eleNum < HLL_SERIAL_SPARSE_MIN) {
        uint8_t val = _registers[0];
        int num = 1;
        int sum = 0;

        for (int i = 1; i < HLL_REGISTERS; i++) {
          uint8_t cur = _registers[i];
          if (val == cur) {
            num++;
            continue;
          } else {
            sum += num;
            dataBuffer.write(val);
            EncodeAndWriteUint32(num, dataBuffer);
            val = cur;
            num = 1;
          }
        }
        sum += num;
        dataBuffer.write(val);
        EncodeAndWriteUint32(num, dataBuffer);
        dataBuffer.write((uint8_t)(1 << HLL_BITS));
        } else {
            dataBuffer.writeBytes((char*)_registers, HLL_REGISTERS);
        }
        return;
    }

void HllCtx::deserialize( autil::DataBuffer& dataBuffer, autil::mem_pool::Pool* pool) {
    if (pool == NULL) {
      return;
    }
    dataBuffer.read(_encoding);
    _eleNum = ReadAndDecodeUnit32(dataBuffer);
    if (HLL_SPARSE == _encoding) {
      _regiArr = (HllRegi *)pool->allocate(HLL_REGI_MAX_BYTES);
      dataBuffer.readBytes(_regiArr, HLL_REGI_MAX_BYTES);
      return;
    }
            // 稠密编码方式
    _registers = (uint8_t *)pool->allocate(HLL_REGISTERS);
    if (_registers == NULL)
      return;

    if (_eleNum < HLL_SERIAL_SPARSE_MIN) {
      uint8_t val = 0;  // 实际的值
      uint32_t num = 0; // 重复的次数
      int writeIdx = 0;
      dataBuffer.read(val);

      while (val != (1 << HLL_BITS)) // 一直循环到哨兵
      {
        num = ReadAndDecodeUnit32(dataBuffer);
        for (uint32_t i = 0; i < num; i++) {
          if (writeIdx < HLL_REGISTERS)
            _registers[writeIdx] = val;
          writeIdx++;
        }
        dataBuffer.read(val);
      }
    } else {
      dataBuffer.readBytes(_registers, HLL_REGISTERS);
    }
}

int HllCtx::ReadAndDecodeUnit32(autil::DataBuffer &dataBuffer) {
    uint8_t b;
    uint32_t result;

    dataBuffer.read(b);
    result = (b & 0x7F);
    if (!(b & 0x80))
        goto done;
    dataBuffer.read(b);
    result |= (b & 0x7F) << 7;
    if (!(b & 0x80))
        goto done;
    dataBuffer.read(b);
    result |= (b & 0x7F) << 14;
    if (!(b & 0x80))
        goto done;
    dataBuffer.read(b);
    result |= (b & 0x7F) << 21;
    if (!(b & 0x80))
        goto done;
    dataBuffer.read(b);
    result |= b << 28;
    if (!(b & 0x80))
        goto done;
done:
    return result;
}

void HllCtx::EncodeAndWriteUint32(uint32_t value, autil::DataBuffer& dataBuffer) const {
    uint8_t buf[5];
    int len = varintEncodeUint32(value, buf);
    dataBuffer.writeBytes(buf, len);
    return;
}

int HllCtx::varintEncodeUint32(uint32_t value, uint8_t *buf) const {
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

double Hyperloglog::hllSigma( double x )
{
    if (x == 1.) return INFINITY;

    double zPrime;
    double y = 1;
    double z = x;

    do {
        x *= x;
        zPrime = z;
        z += x * y;
        y += y;
    } while (zPrime != z);

    return z;
}

double Hyperloglog::hllTau( double x )
{
    if (x == 0. || x == 1.) return 0.;

    double zPrime;
    double y = 1.0;
    double z = 1 - x;

    do {
        x = sqrt(x);
        zPrime = z;
        y *= 0.5;
        z -= pow(1 - x, 2) * y;
    } while(zPrime != z);

    return z/3;
}


int Hyperloglog::hllCtxRegiArrToRegisters( uint8_t * registers, HllRegi * regiArr )
{
    int  eleNum = 0;

    for ( int i = 0; i < HLL_REGI_MAX; i++ )
    {
        HllRegi curr = regiArr[ i ];

        if ( curr.count == 0 )             continue;       /* count 一定是大于0的 */
        if ( curr.index >= HLL_REGISTERS ) continue;
        if ( curr.count <= registers[ curr.index ] ) continue;

        registers[ curr.index ] = curr.count;
        eleNum++;
    }

    return eleNum;
}


int Hyperloglog::hllCtxSparseToDense( HllCtx * thiz, autil::mem_pool::Pool* pool)
{
    {
        if (thiz->_encoding == HLL_DENSE)
            return 0;
        if (thiz->_regiArr == NULL)
            return -1;

        // 准备一下内存
        if (NULL != thiz->_registers) {
            memset(thiz->_registers, 0, HLL_REGISTERS);
        } else {
            thiz->_registers = (uint8_t*)pool->allocate(HLL_REGISTERS * sizeof(uint8_t));
            memset(thiz->_registers, 0, HLL_REGISTERS);
            if (NULL == thiz->_registers)
                goto failed;
        }

        // 把已经有的 转换到 registers 里面
        HllRegi *regiArr = thiz->_regiArr;

        thiz->_eleNum = hllCtxRegiArrToRegisters(thiz->_registers, regiArr);
        thiz->_encoding = HLL_DENSE;
        thiz->_regiArr = NULL;

        return 0;
    }

failed:
    return -1;
}

int Hyperloglog::hllCtxAdd( HllCtx * thiz, const unsigned char * ele, int eleLen, autil::mem_pool::Pool* pool )
{
    uint64_t hash, index;

    hash = MurmurHash64A( ele, eleLen );

    index =   hash & HLL_P_MASK;          /* Register index. */
    hash  >>= HLL_P;                      /* Remove bits used to address the register. */
    hash  |=  ((uint64_t)1<<HLL_Q);       /* Make sure the loop terminates
                                             and count will be <= Q+1. */
    uint64_t  count = 1;                  /* Initialized to 1 since we count the "00000...1" pattern. */

#if __x86_64__
    __asm__(
            "bsfq %1, %0\n\t"
            "jnz 1f\n\t"
            "movq $-1,%0\n\t"
            "1:"
            :"=q"(count):"q"(hash) );
    count++;
#else
    uint64_t bit = 1ULL;

    while ( ( hash & bit) == 0)
    {
        count++;
        bit <<= 1;
    }
#endif


setRegi:
    if ( HLL_DENSE == thiz->_encoding )                      // 稠密编码
    {
        uint8_t * registers = thiz->_registers;
        uint8_t   oldcount = registers[ index ];

        if ( count > oldcount )
        {
            registers[ index ] = count;
            thiz->_eleNum += 1;                             // 这里的值，基于hash是很平均的，表达桶被设置值的次数
        }
    }
    else                                                    // 稀疏编码
    {
        HllRegi * regiArr = thiz->_regiArr;

        regiArr += ( index % HLL_REGI_AREA_NUM ) * HLL_REGI_AREA_SIZE;  // 计算出对应的子区域的第一个起始位置

        int i = 0;
        for ( ; i < HLL_REGI_AREA_SIZE; i++ )               // 顺序检测这32个位置
        {
            HllRegi curr = regiArr[ i ];

            if ( curr.count == 0 )                          // 空的，追加
            {
                regiArr[ i ].index = index;
                regiArr[ i ].count = count;
                break;
            }

            if ( curr.index == index )                      // 非空 判断index
            {
                if ( curr.count < count )  regiArr[ i ].count = count;
                break;
            }
        }

        if ( i == HLL_REGI_AREA_SIZE )                      // 如果32个位置都满了，这时候需要转换编码方式。
        {
            if ( -1 == hllCtxSparseToDense( thiz, pool ) )
                return -1;

            goto setRegi;
        }
    }

    return 0;
}



uint64_t Hyperloglog::hllCtxCount( const HllCtx * thiz )
{

    if ( thiz == NULL ) return 0ULL;

    int reghisto[ HLL_Q + 2 ] = {0};
    if ( thiz->_encoding == HLL_DENSE )                      // 稠密编码
    {
        uint8_t * registers = thiz->_registers;
        for ( long i = 0; i < 1024; i++ )                   // 1024 * 16 = HLL_REGISTERS
        {
            reghisto[ registers[ 0 ] ]++;
            reghisto[ registers[ 1 ] ]++;
            reghisto[ registers[ 2 ] ]++;
            reghisto[ registers[ 3 ] ]++;
            reghisto[ registers[ 4 ] ]++;
            reghisto[ registers[ 5 ] ]++;
            reghisto[ registers[ 6 ] ]++;
            reghisto[ registers[ 7 ] ]++;
            reghisto[ registers[ 8 ] ]++;
            reghisto[ registers[ 9 ] ]++;
            reghisto[ registers[ 10 ] ]++;
            reghisto[ registers[ 11 ] ]++;
            reghisto[ registers[ 12 ] ]++;
            reghisto[ registers[ 13 ] ]++;
            reghisto[ registers[ 14 ] ]++;
            reghisto[ registers[ 15 ] ]++;

            registers += 16;
        }
    }
    else                                                        // 稀疏编码
    {
        HllRegi * regiArr = thiz->_regiArr;

        for ( long i = 0; i < 128; i++ )                        // 128 * 4 = HLL_REGI_MAX
        {
            reghisto[ regiArr[ 0 ].count ]++;
            reghisto[ regiArr[ 1 ].count ]++;
            reghisto[ regiArr[ 2 ].count ]++;
            reghisto[ regiArr[ 3 ].count ]++;

            regiArr += 4;
        }

        reghisto[0] += ( HLL_REGISTERS - HLL_REGI_MAX );
    }

    double E;
    double m = HLL_REGISTERS;
    double z = m * hllTau( (m - reghisto[ HLL_Q + 1 ]) / (double)m );

    for (int j = HLL_Q; j >= 1; --j) {
        z += reghisto[j];
        z *= 0.5L;
    }

    z += m * hllSigma( reghisto[0] / (double)m );
    E = llroundl( HLL_ALPHA_INF * m * m / z );

    return (uint64_t) E;
}



HllCtx * Hyperloglog::hllCtxCreate( unsigned char encoding, autil::mem_pool::Pool *pool )
{
    if ( encoding != HLL_DENSE && encoding != HLL_SPARSE )
        return NULL;
    if (pool == NULL)
        return NULL;

    HllCtx * thiz = (HllCtx *)pool->allocate ( sizeof(HllCtx) );


    thiz->_eleNum    = 0;
    thiz->_encoding   = encoding;
    thiz->_registers  = NULL;
    thiz->_regiArr = NULL;

    if ( HLL_SPARSE == encoding )
    {
        thiz->_regiArr = (HllRegi *)pool->allocate( HLL_REGI_MAX * sizeof(HllRegi) );
        if ( NULL == thiz->_regiArr ) goto failed;
        memset(thiz->_regiArr, 0, HLL_REGI_MAX * sizeof(HllRegi));
    }

    if ( HLL_DENSE == encoding )
    {
        thiz->_registers = (uint8_t *)pool->allocate( HLL_REGISTERS * sizeof(uint8_t) );
        if ( NULL == thiz->_registers ) goto failed;
        memset(thiz->_registers, 0, HLL_REGISTERS);
    }

    return thiz;

failed:
    return NULL;
}

void  Hyperloglog::hllCtxReset( HllCtx * thiz )
{
    if ( thiz == NULL ) return;

    thiz->_eleNum = 0;

    if ( NULL != thiz->_registers )
        memset( thiz->_registers, 0, HLL_REGISTERS );

    if ( NULL != thiz->_regiArr )
        memset( thiz->_regiArr, 0, HLL_REGI_MAX_BYTES );
}

int Hyperloglog::hllCtxMergeRegisters( uint8_t * dst, uint8_t * src )
{
    int eleNum0 = 0;
    int eleNum1 = 0;
    int eleNum2 = 0;
    int eleNum3 = 0;

    for ( long i = 0; i < HLL_REGISTERS;  )
    {
        uint8_t max0 = dst[ i ];
        uint8_t val0 = src[ i ];

        if ( val0 > max0 )
        {
            dst[ i ] = val0;
            eleNum0 += 1;
        }

        uint8_t max1 = dst[ i + 1 ];
        uint8_t val1 = src[ i + 1 ];

        if ( val1 > max1 )
        {
            dst[ i + 1 ] = val1;
            eleNum1 += 1;
        }

        uint8_t max2 = dst[ i + 2 ];
        uint8_t val2 = src[ i + 2 ];

        if ( val2 > max2 )
        {
            dst[ i + 2 ] = val2;
            eleNum2 += 1;
        }

        uint8_t max3 = dst[ i + 3 ];
        uint8_t val3 = src[ i + 3 ];

        if ( val3 > max3 )
        {
            dst[ i + 3 ] = val3;
            eleNum3 += 1;
        }

        i += 4;
    }

    return eleNum0 + eleNum1 + eleNum2 + eleNum3;
}


int Hyperloglog::hllCtxMerge( HllCtx * thiz, HllCtx * forMerge, autil::mem_pool::Pool* pool )
{
    if ( NULL == thiz )      return -1;
    if ( NULL == forMerge ) return -1;

    if ( -1 == hllCtxSparseToDense( thiz, pool ) )            // 转成稠密编码
        return -1;

    uint8_t encoding = forMerge->_encoding;
    int     eleNum  = 0;

    if ( HLL_DENSE == encoding )
    {
        eleNum = hllCtxMergeRegisters( thiz->_registers, forMerge->_registers );
        thiz->_eleNum += eleNum;

        return 0;
    }

    if ( HLL_SPARSE != encoding )  return -1;

    eleNum = hllCtxRegiArrToRegisters( thiz->_registers, forMerge->_regiArr );
    thiz->_eleNum += eleNum;

    return 0;
}

std::string HllCtx::toString() const {
    uint64_t count = autil::Hyperloglog::hllCtxCount(this);
    std::stringstream ss;
    ss << count;
    return ss.str();
}

}
