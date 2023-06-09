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

#include <assert.h>
#include <stdint.h>
#include <string.h>
#include <cstddef>
#include <cstdlib>
#include <fstream>
#include <string>
#include <vector>

#include "autil/legacy/exception.h"

namespace autil{ namespace legacy
{
const char aendl = '\n';
/**
* Base interface for stream
*/
class a_ios
{
public:
    virtual ~a_ios()
    {
    }

    bool good()   ///< not bad nor fail nor eof
    {
        return !eof() && !bad() && !fail();
    }

    virtual bool eof()
    {
        return false;
    }

    virtual bool bad()
    {
        return false;
    }

    virtual bool fail()
    {
        return false;
    }
    
};

/**
* input stream interface. get/peek is at default failed.
*/
class a_Istream : public a_ios
{
public:
    virtual int peek()
    {
        return -1;
    }

    virtual int get()
    {
        return -1;
    }

    virtual int GetThenPeek()
    {
        if (get() < 0)
            AUTIL_LEGACY_THROW(ExceptionBase, "failed to get in GetThenPeek");
        return peek();
    }
    
    virtual bool fail()
    {
        return true;
    }

    virtual int32_t skip(int32_t len)
    {
        for (int32_t i = 0; i < len; ++i)
        {
            get();
            if (eof())
                return i;
        }
        return len;
    }

    virtual ~a_Istream() {}
    
    /**
     * @note
     * In a_Istream, we define read(char*, int32_t) will never return negative value.
     * And it will always return the actual number of read bytes.
     * If the End-of-File is reached before len characters have been read, 
     * the buff will contain all the elements read until it, and the eofbit will be set,
     * which can be checked with eof().
     */
    virtual int32_t read(char* buf, int32_t len)
    {
        for (int32_t i = 0; i < len; ++i)
        {
            int ch = get();
            if (eof())
                return i;
            buf[i] = ch;
        }
        return len;
    }

    virtual int32_t readString(std::string &str, int32_t len)
    {
        if (len == 0)
        {
            return 0;
        }
        char* temp = new char[len];
        int32_t n;
        try
        {
            n = read(temp, len);
            str.assign(temp, n);
        } 
        catch (...) 
        {
            delete[] temp;
            throw;
        }
        delete[] temp;
        return n;
    }
    
    virtual uint32_t ReadVarint(uint32_t& udata);
    virtual uint32_t ReadVarint(uint64_t& udata);
};


/**
* Read data from string
*/
class a_IstreamBuffer : public a_Istream
{
    char *mData;
    int mSize;
    int mCurPos;
    bool mLastGetFail;
    bool mNeedAlloc;
public:

    a_IstreamBuffer(): mData(NULL), mSize(0), mCurPos(0), mLastGetFail(false), mNeedAlloc(false) {}

    void reset()
    {
        mCurPos = 0;
        mLastGetFail = false;
    }

    a_IstreamBuffer(const std::string &b, bool needAlloc = true)
    {
        mData = NULL;
        mSize = 0;
        mCurPos = 0;
        mNeedAlloc = false;
        str(b, needAlloc);
    }

    void str(const std::string& str, bool needAlloc = true)
    {
        freeAlloc();
        mNeedAlloc = needAlloc;
        mSize = str.size();
        if (mNeedAlloc)
        {
            mData = (char *) malloc(mSize);
            memcpy(mData, str.data(), mSize);
        }
        else
        {
            mData = (char *)str.data();
        }
        mCurPos = 0;
        mLastGetFail = false;
    }

    virtual int peek()
    {
        if (mCurPos >= mSize)
        {
            mLastGetFail = true;
            return -1;
        }
        return (static_cast<unsigned char>(mData[mCurPos]));
        /**
         * Below is a wrong implementation for peek():
         *
         * return mIter == mBuffer.end() ? -1 : *mIter;
         *
         * In eof(), we only test mLastGetFail is set or not, since we cannot use test like
         * mIter == mBuffer.end()
         * since last sucessfully get() will make mIter == mBuffer.end() but this time eof()
         * should return false.
         *
         * In this way, if peek() hits the last char in the buffer, it returns -1. However,
         * mLastGetFail is not set, thus eof() reports the wrong status "false". This makes
         * caller see the value returned by peek() is a data byte not an end.
         */
    }

    virtual int get()
    {
        if (mCurPos >= mSize)
        {
            mLastGetFail = true;
            return -1;
        }
        return (static_cast<unsigned char>(mData[mCurPos++]));
    }

    virtual int32_t read(char* buf, int32_t len)
    {
        if (mCurPos >= mSize)
        {
            mLastGetFail = true;
            return 0;
        }
        int32_t readBytes = ((mSize - mCurPos >= len) ? len : (mSize - mCurPos));
        if (readBytes > 0)
        {
            if (readBytes < len)
            {
                mLastGetFail = true;
            }
            memcpy(buf, mData + mCurPos, readBytes);
            mCurPos += readBytes;
            return readBytes;
        }
        return 0;
    }

    virtual int32_t readString(std::string &str, int32_t len)
    {
        str.clear();
        if (mCurPos >= mSize)
        {
            mLastGetFail = true;
            return 0;
        }
        int32_t readBytes = ((mSize - mCurPos >= len) ? len : (mSize - mCurPos));
        if (readBytes > 0)
        {
            str.append(mData + mCurPos, readBytes);
            mCurPos += readBytes;
            return readBytes;
        }
        return 0;
    }

    virtual uint32_t ReadVarint(uint32_t& udata);
    virtual uint32_t ReadVarint(uint64_t& udata);
    
    virtual bool eof()
    {
        return mLastGetFail;
    }

    virtual bool bad()
    {
        return false;
    }

    virtual bool fail()
    {
        return false;
    }

    virtual ~a_IstreamBuffer()
    {
        freeAlloc();
    }
private:
    inline void freeAlloc()
    {
        if (mNeedAlloc && mData != NULL)
        {
            free(mData);
            mData = NULL;
        }
    }
};

class a_IstreamStd : public a_Istream
{
    std::istream &mBase;
public:
    a_IstreamStd(std::istream &is) : mBase(is) {}

    virtual int peek()
    {
        return mBase.peek();
    }

    virtual int get()
    {
        return mBase.get();
    }

    virtual int32_t read(char* buf, int32_t len)
    {
        mBase.read(buf, len);
        return mBase.gcount();
    }

    virtual bool eof()
    {
        return mBase.eof();
    }

    virtual bool bad()
    {
        return mBase.bad();
    }

    virtual bool fail()
    {
        return mBase.fail();
    }

    virtual ~a_IstreamStd() {}
};


/**
* output stream interface, put is at default OK, but just throw away
*/
class a_Ostream : public a_ios
{
public:
    a_Ostream() {}
    
    virtual a_Ostream& put(char data)
    {
        return *this;
    }

    virtual void write(const char *d, uint32_t len)
    {
        while (len--)
        {
            put(*d++);
        }
    }
    virtual a_Ostream& operator<<(const char &ch)
    {
        put(ch);
        return *this;
    }
    virtual a_Ostream& operator<<(const std::string &in)
    {
        write(in.c_str(), in.size());
        return *this;
    }
    virtual a_Ostream& operator<<(const char *s)
    {
        return *this << std::string(s);
    }

    virtual uint32_t WriteVarint(const uint32_t);
    virtual uint32_t WriteVarint(const uint64_t);    
    
    virtual ~a_Ostream() {}
};

class a_OstreamNull : public a_Ostream
{
public:
    a_OstreamNull() {}

    virtual void write(const char *d, uint32_t len)
    {}

    virtual a_OstreamNull& operator<<(const char &ch)
    {
        return *this;
    }
    virtual a_OstreamNull& operator<<(const std::string &in)
    {
        return *this;
    }
    virtual a_OstreamNull& operator<<(const char *s)
    {
        return *this;
    }
    
    virtual ~a_OstreamNull() {}
};

/**
* Store written data into string
*/
class a_OstreamBuffer : public a_Ostream
{
    static const int bufSize = 4;
    char buf[bufSize]; /// empircal size

    mutable int buffered;
    mutable std::string mBuffer;
public:
    a_OstreamBuffer() : buffered(0) {}

    void clear()
    {
        buffered = 0;
        mBuffer.clear();
    }

    void str(std::string str)
    {
        mBuffer.swap(str);
        buffered = 0;
    }

    /* Provide this method to make this class behave closer
     * to ostringstream
     */
    std::string str() const
    {
        mBuffer.append(buf, buffered);
        buffered = 0;
        return mBuffer;
    }

    virtual void write(const char *d, uint32_t len)
    {
        assert( bufSize - buffered >= 0);

        if (len < static_cast<uint32_t>(bufSize - buffered))
        {
            memcpy(buf + buffered, d, len);
            buffered += len;
        }
        else
        {
            mBuffer.append(buf, buffered);
            mBuffer.append(d, len);
            buffered = 0;
        }
    }

    virtual a_Ostream& put(char data)
    {
        buf[buffered++] = data;
        if (buffered == bufSize)
        {
            mBuffer.append(buf, bufSize);
            buffered = 0;
        }
        return *this;
    }

    virtual uint32_t WriteVarint(const uint32_t);
    virtual uint32_t WriteVarint(const uint64_t);
    
    virtual ~a_OstreamBuffer() {}
};


/**
* Proxy data to a std::ostream
*/
class a_OstreamStd : public a_Ostream
{
    std::ostream &mBase;
public:
    a_OstreamStd(std::ostream& os) : mBase(os) {}
    virtual a_Ostream& put(char data)
    {
        mBase.put(data);
        return *this;
    }

    virtual void write(const char *d, uint32_t len)
    {
        mBase.write(d, len);
    }

    virtual bool eof()
    {
        return mBase.eof();
    }

    virtual bool fail()
    {
        return mBase.fail();
    }

    virtual ~a_OstreamStd() {}
};


class a_OstreamWrapper : public a_Ostream
{
protected:
    a_Ostream &mBase;
public:
    a_OstreamWrapper(a_Ostream &os) : mBase(os)
    {}

    virtual bool eof()
    {
        return mBase.eof();
    }

    virtual bool bad()
    {
        return mBase.bad();
    }

    virtual bool fail()
    {
        return mBase.fail();
    }

    virtual ~a_OstreamWrapper()
    {}
};

class a_IstreamWrapper : public a_Istream
{
protected:
    a_Istream &mBase;

public:
    a_IstreamWrapper(a_Istream &is) : mBase(is) {}

    virtual int peek()
    {
        return mBase.peek();
    }

    virtual int get()
    {
        return mBase.get();
    }

    virtual bool eof()
    {
        return mBase.eof();
    }

    virtual bool bad()
    {
        return mBase.bad();
    }

    virtual bool fail()
    {
        return mBase.fail();
    }

    virtual ~a_IstreamWrapper() {}
};

/**
* Only allow read at maximum len bytes
* Get/Peek may fail before those bytes are read if underlying stream
 * !good()
*/
class a_IstreamLengthed : public a_Istream
{
    /**
     * [mBegin, mEnd) is the data range that this lengthed stream owns in buffer, 
     * mLocation is the cursor pointer in this range, it always point to the first
     * available byte in this range.
     */
    char *mBegin, *mLocation, *mEnd;     
    /** mHoldInBuff == mEnd - mBegin*/
    uint32_t mHoldInBuff;                
    /** 
     * mTotal is the available data size of current lengthed stream after every FetchDataFromBuff.
     * mTotal == mHoldInBuff + (sizeof data will fill into buffer for this lengthed stream).
     *
     * it is from the standpoint of user, that means mTotal may be more than the actually 
     * available data size, in this situation, it is violating the rule of lengthed,
     * then we will throw exception TO TERMINATE this illegal situation.
     */
    uint32_t mTotal;                     
    
    /**
     * mBuffer point to the head of the communal buffer, with size of LENGTHED_STREAM_BUFFER_SIZE,
     * which was alloced by the base lengthed stream.
     * mBufferTail == mBuffer + LENGTHED_STREAM_BUFFER_SIZE;
     */
    static const uint32_t LENGTHED_STREAM_BUFFER_SIZE = 64 * 1024;    
    char *mBuffer, *mBufferTail;
    /** 
     * mIsAlloced shows if this lengthed stream has the ownership of the communal buffer.
     * if this is the base lengthed stream, then mIsAlloced = true.
     */
    bool mIsAlloced;
    
    bool mFail;
    bool mLastGetEof;
    a_Istream& mBase;
    
    a_IstreamLengthed();
    a_IstreamLengthed(const a_IstreamLengthed& another);
    a_IstreamLengthed& operator=(const a_IstreamLengthed& another);
    /**
     * @note
     * This method helps to own as many as possible data from a communal buffer as itself, that means:
     * 1) if there is enough fresh data in the buffer, this method will fetch the correct size of
     *    data that just match its requirement as itself.
     *
     * 2) if there is fresh data, but not enough to match the requirement, this method will
     *    fetch all data in the buffer as itself.
     *
     * 3) if there is not any fresh data in the buffer, this method will re-fill the buffer
     * from the underlying stream. And it promises to full the buffer, unless we arrive at the 
     * length constraints of the base lengthed stream which specified in the constructor before 
     * the buffer is fully filled. If underlying stream could not provide enough data for this behavior,
     * an exception is throw.
     */
    void FetchDataFromBuff(uint32_t bufferSize)
    {
        mTotal -= mHoldInBuff;
        a_IstreamLengthed *p = dynamic_cast<a_IstreamLengthed *>(&mBase);
        if (p != 0)
        {
            if (p->mLocation == p->mEnd)
            {
                p->FetchDataFromBuff(bufferSize);
            }
            mLocation = mBegin = p->mLocation;
            uint32_t remainInBuff = p->mEnd - p->mLocation;
            if (remainInBuff >= mTotal)
            {
                mHoldInBuff = mTotal;
            }
            else if (p->mEnd == p->mBufferTail)
            {
                /** parent buffer has been fulfilled, so parent may have more data. */
                mHoldInBuff = remainInBuff;
            }
            else
            {
                /**
                * that means child lengthed stream required too many data 
                * that parent can afford.
                */
                AUTIL_LEGACY_THROW(ExceptionBase, "Bad data requirement since parent has no more data to supply");
            }
            
            mEnd = mBegin + mHoldInBuff;
            p->mLocation = mEnd;
        }
        else // fill buffer
        {
            if (!mIsAlloced)
            {
                mBuffer = new char[LENGTHED_STREAM_BUFFER_SIZE];
                mIsAlloced = true;
            }
            uint32_t canRead = mTotal < LENGTHED_STREAM_BUFFER_SIZE ? 
                               mTotal : LENGTHED_STREAM_BUFFER_SIZE;
            mHoldInBuff = mBase.read(mBuffer, canRead);
            if (mHoldInBuff != canRead)
            {
                AUTIL_LEGACY_THROW(ExceptionBase, "no more to read in LengthedStream");
            }
            mLocation = mBegin = mBuffer;
            mEnd = mBegin + mHoldInBuff;
        }
        /** mFail don't need to up-to-date since it should never change in this object
         */
        mFail = mBase.fail();
    }

public:
    a_IstreamLengthed(a_Istream& is, uint32_t len) : mBegin(0), mLocation(0), mEnd(0), 
    mHoldInBuff(0), mTotal(len), mBuffer(0), mIsAlloced(false), mLastGetEof(false), mBase(is)
    {
        FetchDataFromBuff(LENGTHED_STREAM_BUFFER_SIZE);
        if (!mIsAlloced)
        {
            a_IstreamLengthed *p = dynamic_cast<a_IstreamLengthed *>(&mBase);
            mBuffer = p->mBuffer;
        }
        mBufferTail = mBuffer + LENGTHED_STREAM_BUFFER_SIZE;
    }
    
    inline uint32_t GetRemain()
    {
        return mTotal - (mLocation - mBegin);
    }

    virtual int peek()
    {
        if (mLocation < mEnd)
        {
            return static_cast<unsigned char>(*mLocation);
        }
        else if (mHoldInBuff < mTotal)
        {
            FetchDataFromBuff(LENGTHED_STREAM_BUFFER_SIZE);
            return static_cast<unsigned char>(*mLocation);
        }
        else
        {
            mLastGetEof = true;
            return -1;
        }
    }

    virtual int get()
    {
        if (mLocation < mEnd)
        {
            return static_cast<unsigned char>(*mLocation++);
        }
        else if (mHoldInBuff < mTotal)
        {
            FetchDataFromBuff(LENGTHED_STREAM_BUFFER_SIZE);
            return static_cast<unsigned char>(*mLocation++);
        }
        else
        {
            mLastGetEof = true;
            return -1;
        }
    }
    
    /**
     * There is the scenario that we had known that the first byte is ready and peek the next byte
     * For the efficiency, we don't set mLastGetEof here, so user should use the return value to 
     * ensure whether the eof arrived.
     * @return the byte from peek(), if eof, return -1
     */
    virtual int GetThenPeek()
    {
        if (mLocation != mEnd)
        {
            if (++mLocation != mEnd)
            {
                return static_cast<unsigned char>(*mLocation);
            }
            else if (mHoldInBuff < mTotal)
            {
                FetchDataFromBuff(LENGTHED_STREAM_BUFFER_SIZE);
                return static_cast<unsigned char>(*mLocation);
            }
            else
            {
                mLastGetEof = true;
                return -1;
            }
        }
        else
        {
            AUTIL_LEGACY_THROW(ExceptionBase, "Bad Use of GetThenPeek().");
        }   
    }
    
    virtual int32_t skip(int32_t len)
    {
        int32_t canSkip = GetRemain();
        if (canSkip > len)
        {
            canSkip = len;
        }
        ptrdiff_t remainInBuff = mEnd - mLocation;
        if (canSkip > remainInBuff)
        {
            mLocation = mEnd;
            int32_t skipped = mBase.skip(canSkip - remainInBuff);
            mTotal -= skipped;
            return skipped + remainInBuff;
        }
        else
        {
            mLocation += canSkip;
            return canSkip;
        }
    }

    virtual int32_t read(char* buf, int32_t len)
    {
        int32_t canRead = GetRemain();
        if (canRead >= len)
        {
            canRead = len;
        }
        else
        {
            mLastGetEof = true;
        }
        int32_t readed = canRead;
        ptrdiff_t remainInBuff = mEnd - mLocation;
        while (canRead > remainInBuff)
        {
            memcpy(buf, mLocation, remainInBuff);
            buf += remainInBuff;
            canRead -= remainInBuff;
            FetchDataFromBuff(LENGTHED_STREAM_BUFFER_SIZE);
            remainInBuff = mEnd - mLocation;
        }
        memcpy(buf, mLocation, canRead);
        mLocation += canRead;
        return readed;
    }

    virtual uint32_t ReadVarint(uint32_t& udata);
    virtual uint32_t ReadVarint(uint64_t& udata);
    
    virtual bool eof()
    {
        return mLastGetEof;
    }

    /** @deprecated 
     * we just override this method to avoid good() always fail
     */
    virtual bool fail()
    {
        return mFail;
    }
    
    virtual ~a_IstreamLengthed() 
    {
        if (mIsAlloced)
        {
            delete [] mBuffer;
            mBuffer = 0;
        }
    }
    
};

class a_IJoinStream : public a_Istream
{
    std::vector<a_Istream *> mSources;
    std::vector<a_Istream *>::iterator mCurrent;
public:

    a_IJoinStream(a_Istream& first, a_Istream& second)
    {
        mSources.push_back(&first);
        mSources.push_back(&second);
        mCurrent = mSources.begin();
    }

    virtual int peek()
    {
        while (mCurrent != mSources.end())
        {

            int data = (*mCurrent)->peek();
            if (data < 0)
            {
                ++mCurrent;
            }
            else
            {
                return data;
            }
        }
        return -1;
    }

    virtual int get()
    {
        while (mCurrent != mSources.end())
        {
            int data = (*mCurrent)->get();
            if (data < 0)
            {
                ++mCurrent;
            }
            else
            {
                return data;
            }
        }
        return -1;
    }


    virtual bool fail()
    {
        return mCurrent == mSources.end() ? false : (*mCurrent)->fail();
    }

    virtual bool eof()
    {
        return mCurrent == mSources.end();
    }

    virtual ~a_IJoinStream() {}

    virtual int32_t read(char* buf, int32_t len)
    {
        int32_t remain = len;
        int32_t got = 0;

        while (remain > 0 && mCurrent != mSources.end())
        {
            int32_t t = (*mCurrent)->read(buf + got, remain);
            if (t > 0)
            {
                remain -= t;
                got += t;
            }
            if ((*mCurrent)->fail())
            {
                break;
            }

            if ((*mCurrent)->eof())
            {
                ++mCurrent;
            }

        }
        return got;
    }
};

class a_LinePositionAwareIstream : public a_IstreamWrapper
{
    char mLast;
public:
    uint32_t mLine, mCol;

    a_LinePositionAwareIstream(a_Istream &base)
            : a_IstreamWrapper(base), mLast(0), mLine(0), mCol(0) {}

    int get()
    {
        int data = mBase.get();
        switch (data)
        {
            case '\r':
                ++mLine;
                mCol = 0;
                break;
            case '\n':
                if (mLast != '\r')   // for support with windows \r\n
                {
                    ++mLine;
                    mCol = 0;
                }
                break;
            case -1:
                break;
            default:
                ++mCol;
        }
        return data;
    }

    virtual ~a_LinePositionAwareIstream() {}
};

}}

/** the following is to support once defined type in old astream.h
typedef std::istream aistream;
typedef std::ostream aostream;
typedef std::istringstream aistringstream;
typedef std::ostringstream aostringstream;
 */
namespace autil{ namespace legacy
{
/*obsolete*/
typedef std::istream aistream;
/*obsolete*/
typedef std::ostream aostream;
typedef a_IstreamBuffer aistringstream;
typedef a_OstreamBuffer aostringstream;
}}



/**@BRIEF
 * Below is the new stream for cangjie ToString, It is different from a_ios:
 *
 * 1) new stream will not more has virtual function
 * 2) new stream will base on buffer of char *
 */
namespace autil{ namespace legacy
{

/**
 * a structure cantains all required info for back-write message content size.
 * @param mPoint: tells the entrance pointer.
 * @param mIndexInMeta: tells the buffer index where cantains mPoint.
 * @param mWriteInCurrBuff: tells how many bytes of the 5 bytes should write to this buffer.
 */
struct PositionInStream
{
    char * mPoint;
    uint32_t mIndexInMeta;
    uint32_t mWriteInCurrBuff;
};    

class ByteArrayOstream
{
public:
    /** 
     *@param bufSize: how many bytes to allocate from memory, default is SIZE_PER_BUFF
     */
    ByteArrayOstream(uint32_t buffSize = SIZE_PER_BUFF) :
        mCurr(0), mEnd(0), mSizePerBuff(buffSize)
    {}
    
    ByteArrayOstream(const ByteArrayOstream & rhs);
    
    ByteArrayOstream& operator=(const ByteArrayOstream & rhs);
    /**
     * In Deconstructor, we need to release all memory resource.
     */
    ~ByteArrayOstream();
    /**
     * Put one byte into buffer.
     */
    inline void Put(char c)
    {
        if (mCurr == mEnd)
        {
            AllocateBuffer(mSizePerBuff);
        }
        *mCurr++ = c;
    }
    /**
     * Memory copy a block data with len lengthed pointed by start to buffer.
     */
    void Write(const char * start, uint32_t len);
    /**
     * Encoding udata to varint and store the result into buffer
     */
    uint32_t WriteVarint(const uint32_t& udata);
    uint32_t WriteVarint(const uint64_t& udata);
    inline uint32_t WriteVarint(const int32_t& data)
    {
        return WriteVarint(static_cast<uint32_t>(data));
    }
    inline uint32_t WriteVarint(const int64_t& data)
    {
        return WriteVarint(static_cast<uint64_t>(data));
    }
    /**
     * store udata with fixed 4/8 bytes into buffer
     */
    uint32_t WriteFixed(const uint32_t& udata);
    uint32_t WriteFixed(const uint64_t& udata);
    inline uint32_t WriteFixed(const int32_t& data)
    {
        return WriteFixed(static_cast<uint32_t>(data));
    }
    inline uint32_t WriteFixed(const int64_t& data)
    {
        return WriteFixed(static_cast<uint64_t>(data));
    }
    inline uint32_t WriteFixed(const float& data)
    {
        return WriteFixed(reinterpret_cast<const uint32_t&>(data));
    }
    inline uint32_t WriteFixed(const double& data)
    {
        return WriteFixed(reinterpret_cast<const uint64_t&>(data));
    }
    /**
     * Serialize the whole string to buffer
     */
    inline uint32_t WriteString(const std::string& str)
    {
        size_t size = str.size();
        Write(str.data(), size);
        return size;
    }
    /**
     * This is customized for cangjie message content size writeback.
     * In this method, we will skip a fixed 5 bytes and return the position object.
     */
    PositionInStream SkipVarint32();
    /**
     * This method is used to writeback message content size, 
     * corresponding to SkipVarint32.
     *
     * @attention: User must promise every PositionInStream object argument in 
     *             this->ReplaceVarint32(,) is returned from this->SkipVarint32(),
     *             OR, It is big risk to call below method for there is memory address
     *             cross-border in HIGH probability.
     * @brief: Below method will never check if the pointer in PositionInStream is legal.
     */
    void ReplaceVarint32(const PositionInStream& position, const uint32_t & udata);
    /**
     * Below method sync data to the target string and return the string size
     */
    uint32_t DataToString(std::string& str) const;
    /**
     * Return total data size of this ByteArrayOstream
     */
    inline uint32_t GetSize() const 
    {
        return mMeta.empty() ? 0 : (mMeta.size() - 1) * mSizePerBuff + (mCurr - mMeta.back());
    }
    /**
     * Clear() is used to clear the data in this stream.
     */
     void Clear();
     
     /**
     * Below method return MD5 value of this ByteArrayOstream's data.
     */
    void GetMd5(uint8_t md5[16]);
    
private:

    /**
     * When buffer is not enough, we use below method to allocate new memory
     * and update mMeta, mCurr and mEnd.
     * @param buffSize: how many bytes to allocate from memory
     */
    inline void AllocateBuffer(uint32_t buffSize)
    {
        mCurr = new char[buffSize];
        mEnd = mCurr + buffSize;
        mMeta.push_back(mCurr);
    }
    /**
     * @param mMeta stores head pointer of every memory block
     */
    std::vector<char *> mMeta;
    /**
     * @param mCurr: when mCurr != mEnd, it is the pointer where we will store
     *               the next fisrt byte.
     * @param mEnd: it is a flag, which point to the next after the last byte. 
     *
     * @note mEnd >= mCurr
     */
    char * mCurr, * mEnd;
    /**
     * @param mSizePerBuff is the buffer size when we need allcote memory.
     */
    uint32_t mSizePerBuff;
    /**
     * Pre-Condition: SIZE_PER_BUFF can't be 0 or constant negative expression. 
     * Here, we set every buffer size to 4KB, to match kuafu's package size.
     */
    static const uint32_t SIZE_PER_BUFF = 4 * 1024; 

};

}}



