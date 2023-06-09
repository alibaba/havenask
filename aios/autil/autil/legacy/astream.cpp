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
#include <stddef.h>
#include <stdint.h>

#include "autil/legacy/astream.h"
#include "autil/legacy/exception.h"

namespace autil{ namespace legacy
{
    ///////////////////////////////////////////////////////////////////////////
    /////////////////////////////    for a_Ostream    /////////////////////////
    ///////////////////////////////////////////////////////////////////////////
#define PUTVARINT32_HELPER(bufferName, data)                   \
    if (data <= 0x7f)                                          \
    {                                                          \
        bufferName[i++] = data | 0x80;                         \
    }                                                          \
    else                                                       \
    {                                                          \
        bufferName[i++] = data & 0x7f;                         \
        if (data <= 0x3fff)                                    \
        {                                                      \
            bufferName[i++] = (data >> 7) | 0x80;              \
        }                                                      \
        else                                                   \
        {                                                      \
            bufferName[i++] = (data >> 7) & 0x7f;              \
            if (data <= 0x1fffff)                              \
            {                                                  \
                bufferName[i++] = (data >> 14) | 0x80;         \
            }                                                  \
            else                                               \
            {                                                  \
                bufferName[i++] = (data >> 14) & 0x7f;         \
                if (data <= 0xfffffff)                         \
                {                                              \
                    bufferName[i++] = (data >> 21) | 0x80;     \
                }                                              \
                else                                           \
                {                                              \
                    bufferName[i++] = (data >> 21) & 0x7f;     \
                    bufferName[i++] = (data >> 28) | 0x80;     \
                }                                              \
            }                                                  \
                                                               \
        }                                                      \
    } 
    
    
    uint32_t a_Ostream::WriteVarint(const uint32_t udata)
    {
        char tmp[5];
        int i = 0;
        PUTVARINT32_HELPER(tmp, udata)
        write(tmp, i);
        return i;
    }
    
    uint32_t a_Ostream::WriteVarint(const uint64_t udata)
    {
        char tmp[10];
        if (udata <= __UINT64_C(0x7ffffffff))
        {
            int i = 0;
            PUTVARINT32_HELPER(tmp, udata)
            write(tmp, i);
            return i;
        }
        else
        {
            tmp[0] = udata & 0x7f;                 
            tmp[1] = (udata >> 7) & 0x7f;          
            tmp[2] = (udata >> 14) & 0x7f;         
            tmp[3] = (udata >> 21) & 0x7f;         
            tmp[4] = (udata >> 28) & 0x7f;         
            uint32_t data = udata >> 35;
            int i = 5;
            PUTVARINT32_HELPER(tmp, data)
            write(tmp, i);
            return i;
        }
    }
    
    ///////////////////////////////////////////////////////////////////////////
    //////////////////////////// for a_OstreamBuffer //////////////////////////
    ///////////////////////////////////////////////////////////////////////////

#define PUTVARINT32_AND_RETURN                           \
    if (bufSize - buffered > 5)                          \
    {                                                    \
        if (udata <= 0x7f)                               \
        {                                                \
            buf[buffered++] = udata | 0x80;              \
            return 1;                                    \
        }                                                \
        buf[buffered++] = udata & 0x7f;                  \
        if (udata <= 0x3fff)                             \
        {                                                \
            buf[buffered++] = (udata >> 7) | 0x80;       \
            return 2;                                    \
        }                                                \
        buf[buffered++] = (udata >> 7) & 0x7f;           \
        if (udata <= 0x1fffff)                           \
        {                                                \
            buf[buffered++] = (udata >> 14) | 0x80;      \
            return 3;                                    \
        }                                                \
        buf[buffered++] = (udata >> 14) & 0x7f;          \
        if (udata <= 0xfffffff)                          \
        {                                                \
            buf[buffered++] = (udata >> 21) | 0x80;      \
            return 4;                                    \
        }                                                \
        else                                             \
        {                                                \
            buf[buffered++] = (udata >> 21) & 0x7f;      \
            buf[buffered++] = (udata >> 28) | 0x80;      \
            return 5;                                    \
        }                                                \
    }                                                    \
    else                                                 \
    {                                                    \
        int i = 0;                                       \
        PUTVARINT32_HELPER(tmp, udata)                   \
        write(tmp, i);                                   \
        return i;                                        \
    }                                                    

    uint32_t a_OstreamBuffer::WriteVarint(const uint32_t udata)
    {
        char tmp[5];
        PUTVARINT32_AND_RETURN
    }

    uint32_t a_OstreamBuffer::WriteVarint(const uint64_t udata)
    {
        char tmp[10];
        if (udata <= __UINT64_C(0x7ffffffff))
        {
            PUTVARINT32_AND_RETURN
        }
        else 
        {
            tmp[0] = udata & 0x7f;                 
            tmp[1] = (udata >> 7) & 0x7f;          
            tmp[2] = (udata >> 14) & 0x7f;         
            tmp[3] = (udata >> 21) & 0x7f;         
            tmp[4] = (udata >> 28) & 0x7f;         
            uint32_t data = udata >> 35;
            int i = 5;
            PUTVARINT32_HELPER(tmp, data);
            write(tmp, i);
            return i;
        }
    }
        
#undef PUTVARINT32_AND_RETURN
#undef PUTVARINT32_HELPER
    
    //////////////////////////////////////////////////////////////////////////////
    /////////////////////////////    for a_Istream    ////////////////////////////
    //////////////////////////////////////////////////////////////////////////////
    // the implementation is the same as Formatter::GetVarint()
    uint32_t a_Istream::ReadVarint(uint32_t& udata)
    {
        const uint8_t MAX_BYTES = 5;

        udata = 0;
        uint8_t bytes = 0;
        int32_t byteRoom = 0;
        while (bytes < MAX_BYTES)
        {
            byteRoom = get();
            if (byteRoom < 0) AUTIL_LEGACY_THROW(StreamCorruptedException, "ReadVarint32");
            udata |= (byteRoom & 0x7f) << (7 * bytes++); 
            if (byteRoom & 0x80) return bytes;
        }
        AUTIL_LEGACY_THROW(StreamCorruptedException, "ReadVarint32"); 
    }
    
    uint32_t a_Istream::ReadVarint(uint64_t& udata)
    {
        const uint8_t MAX_BYTES = 10;

        udata = 0;
        uint8_t bytes = 0;
        int64_t byteRoom = 0;
        while (bytes < MAX_BYTES)
        {
            byteRoom = get();
            if (byteRoom < 0) AUTIL_LEGACY_THROW(StreamCorruptedException, "ReadVarint64");
            udata |= (byteRoom & 0x7f) << (7 * bytes++); 
            if (byteRoom & 0x80) return bytes;
        }
        AUTIL_LEGACY_THROW(StreamCorruptedException, "ReadVarint64"); 
    }
    ////////////////////////////////////////////////////////////////////////////
    //////////////////////////// for a_IstreamBuffer ///////////////////////////
    ////////////////////////////////////////////////////////////////////////////
#define ParseOneByteVarint(pointer, bytes)              \
    byteRoom = *pointer;                                \
    udata |= (byteRoom & 0x7f) << ((bytes - 1) * 7);    \
    if (byteRoom & 0x80)                                \
    {                                                   \
        return bytes;                                   \
    }                       

#define GET_VARINT32(curr, tail, pointer)                               \
    if (curr < tail)                                                    \
    {                                                                   \
        udata = *pointer;                                               \
        if (udata & 0x80)                                               \
        {                                                               \
            udata &= 0x7f;                                              \
            return 1;                                                   \
        }                                                               \
        if (curr < tail)                                                \
        {                                                               \
            uint32_t byteRoom;                                          \
            ParseOneByteVarint(pointer, 2)                              \
            if (curr < tail)                                            \
            {                                                           \
                ParseOneByteVarint(pointer, 3)                          \
                if (curr < tail)                                        \
                {                                                       \
                    ParseOneByteVarint(pointer, 4)                      \
                    if (curr < tail)                                    \
                    {                                                   \
                        ParseOneByteVarint(pointer, 5)                  \
                    }                                                   \
                }                                                       \
            }                                                           \
        }                                                               \
    }
    
    uint32_t a_IstreamBuffer::ReadVarint(uint32_t& udata)
    {
        GET_VARINT32(mCurPos, mSize, (mData + mCurPos++))
        AUTIL_LEGACY_THROW(StreamCorruptedException, "ReadVarint32 In a_IstreamBuffer.");
    }

#define GET_VARINT64(curr, tail, pointer)                                            \
    if (curr < tail)                                                                 \
    {                                                                                \
        udata = *pointer;                                                            \
        if (udata & 0x80)                                                            \
        {                                                                            \
            udata &= 0x7f;                                                           \
            return 1;                                                                \
        }                                                                            \
        if (curr < tail)                                                             \
        {                                                                            \
            uint64_t byteRoom;                                                       \
            ParseOneByteVarint(pointer, 2)                                           \
            if (curr < tail)                                                         \
            {                                                                        \
                ParseOneByteVarint(pointer, 3)                                       \
                if (curr < tail)                                                     \
                {                                                                    \
                    ParseOneByteVarint(pointer, 4)                                   \
                    if (curr < tail)                                                 \
                    {                                                                \
                        ParseOneByteVarint(pointer, 5)                               \
                        if (curr < tail)                                             \
                        {                                                            \
                            ParseOneByteVarint(pointer, 6)                           \
                            if (curr < tail)                                         \
                            {                                                        \
                                ParseOneByteVarint(pointer, 7)                       \
                                if (curr < tail)                                     \
                                {                                                    \
                                    ParseOneByteVarint(pointer, 8)                   \
                                    if (curr < tail)                                 \
                                    {                                                \
                                        ParseOneByteVarint(pointer, 9)               \
                                        if (curr < tail)                             \
                                        {                                            \
                                            ParseOneByteVarint(pointer, 10)          \
                                        }                                            \
                                    }                                                \
                                }                                                    \
                            }                                                        \
                        }                                                            \
                    }                                                                \
                }                                                                    \
            }                                                                        \
        }                                                                            \
    }
    
    uint32_t a_IstreamBuffer::ReadVarint(uint64_t& udata)
    {
        GET_VARINT64(mCurPos, mSize, (mData + mCurPos++))
        AUTIL_LEGACY_THROW(StreamCorruptedException, "ReadVarint64 In a_IstreamBuffer.");
    }
    
    ///////////////////////////////////////////////////////////////////////////
    /////////////////////////// for ByteArrayIstream //////////////////////////
    ///////////////////////////////////////////////////////////////////////////
    
#define DECODE_VARINT32_DIRECTLY(curr)      \
    udata = *curr++;                        \
    if (udata & 0x80)                       \
    {                                       \
        udata &= 0x7f;                      \
        return 1;                           \
    }                                       \
    uint32_t byteRoom;                      \
    ParseOneByteVarint(curr++, 2)           \
    ParseOneByteVarint(curr++, 3)           \
    ParseOneByteVarint(curr++, 4)           \
    ParseOneByteVarint(curr++, 5)

#define DECODE_VARINT64_DIRECTLY(curr)     \
    udata = *curr++;                       \
    if (udata & 0x80)                      \
    {                                      \
        udata &= 0x7f;                     \
        return 1;                          \
    }                                      \
    uint64_t byteRoom;                     \
    ParseOneByteVarint(curr++, 2)          \
    ParseOneByteVarint(curr++, 3)          \
    ParseOneByteVarint(curr++, 4)          \
    ParseOneByteVarint(curr++, 5)          \
    ParseOneByteVarint(curr++, 6)          \
    ParseOneByteVarint(curr++, 7)          \
    ParseOneByteVarint(curr++, 8)          \
    ParseOneByteVarint(curr++, 9)          \
    ParseOneByteVarint(curr++, 10)

#undef GET_VARINT32
#undef GET_VARINT64

    ////////////////////////////////////////////////////////////////////////
    ////////////////////////// for a_IstreamLengthed ///////////////////////
    ////////////////////////////////////////////////////////////////////////
#define READ_VARINT(max)                         \
    while (mLocation != mEnd && count < max)     \
    {                                            \
        byteRoom = (*mLocation & 0x7f);          \
        udata |= byteRoom << (7 * count);        \
        ++count;                                 \
        if (*mLocation++ & 0x80)                 \
        {                                        \
            return count;                        \
        }                                        \
    }                                            

    uint32_t a_IstreamLengthed::ReadVarint(uint32_t& udata)
    {
        ptrdiff_t remainInBuff = mEnd - mLocation;
        /// there are plenty of data in buffer, so we can get from buffer straightforward
        if (remainInBuff >= 5)
        {
            DECODE_VARINT32_DIRECTLY(mLocation)
        }
        // parse varint from bytes left in buffer
        else 
        {
            if (remainInBuff == 0 && mHoldInBuff < mTotal)
            {
                FetchDataFromBuff(LENGTHED_STREAM_BUFFER_SIZE);
            }
            // speed-up for one byte varint32
            udata = *mLocation++;
            if (udata & 0x80)
            {
                udata &= 0x7f;
                return 1;
            }
            // really bad mode
            uint8_t count = 1;
            uint32_t byteRoom;
            // parse varint from bytes left in buffer
            READ_VARINT(5)
            // parse varint from bytes out of  buffer
            while (count < 5 && mHoldInBuff < mTotal)
            {
                FetchDataFromBuff(LENGTHED_STREAM_BUFFER_SIZE);
                READ_VARINT(5)
            }
        }
        AUTIL_LEGACY_THROW(StreamCorruptedException, "ReadVarint32 In a_IstreamLenghted.");
    }
    
    uint32_t a_IstreamLengthed::ReadVarint(uint64_t& udata)
    {
        ptrdiff_t remainInBuff = mEnd - mLocation;
        /// there are plenty of data in buffer, so we can get from buffer straightforward
        if (remainInBuff >= 10)
        {
            DECODE_VARINT64_DIRECTLY(mLocation)
        }
        // parse varint from bytes left in buffer
        else 
        {
            if (remainInBuff == 0 && mHoldInBuff < mTotal)
            {
                FetchDataFromBuff(LENGTHED_STREAM_BUFFER_SIZE);
            }
            // speed-up for one byte varint64
            udata = *mLocation++;
            if (udata & 0x80)
            {
                udata &= 0x7f;
                return 1;
            }
            uint8_t count = 1;
            uint64_t byteRoom;
            // parse varint from bytes left in buffer
            READ_VARINT(10)
            // parse varint from bytes out of  buffer
            while (count < 10 && mHoldInBuff < mTotal)
            {
                FetchDataFromBuff(LENGTHED_STREAM_BUFFER_SIZE);
                READ_VARINT(10)
            }
        }
        AUTIL_LEGACY_THROW(StreamCorruptedException, "ReadVarint64 In a_IstreamLenghted.");
    }

#undef DECODE_VARINT32_DIRECTLY
#undef DECODE_VARINT64_DIRECTLY
 
#undef READ_VARINT
#undef ParseOneByteVarint
}}

