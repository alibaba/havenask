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
/** @file md5.h
 * Define the interface of md5
 * Md5 function can calculate MD5 for a byte stream,
 * the big endian case needs test
 */


#include <stdint.h>
#include <string>

///define namespace
namespace autil
{
namespace legacy
{

/** Calculate Ms5 for a byte stream,
* result is stored in md5[16]
*
* @param poolIn Input data
* @param inputBytesNum Length of input data
* @param md5[16] A 128-bit pool for storing md5
*/
void DoMd5(const uint8_t* poolIn, const uint64_t inputBytesNum, uint8_t md5[16]);

/** check correctness of a Md5-calculated byte stream
*
* @param poolIn Input data
* @param inputBytesNum Length of input data
* @param md5[16] A 128-bit md5 value for checking
*
* @return true if no error detected, false if error detected
*/
bool CheckMd5(const uint8_t* poolIn, const uint64_t inputBytesNum, const uint8_t md5[16]);

/** @brief Md5Stream can receive byte stream as input,
 *  use Put() to put sequential streams,
 *  then call Get() to get md5 for all the input streams
 */
class Md5Stream
{
public:
    /** @brief Constructor
     */
    Md5Stream();
    
    /** @brief Give an input pool into this class
     *
     *  @param poolIn The input pool
     *  @param inputBytesNum The input bytes number in poolIn
     */
    void Put(const uint8_t* poolIn, const uint64_t inputBytesNum);
    
    /** @brief Fill the given hash array with md5 value
     *
     *  @param hash The array to get md5 value
     */
    void Get(uint8_t hash[16]);

    std::string GetMd5String();

private:
    /** @brief Initailize class members
     */
    void Initailize();

    bool mLittleEndian;     /// true if litte endian, false if big endian
    uint8_t mBuf[64];       /// hold remained input stream
    uint64_t mBufPosition;  /// indicate the position of stream end in buf
    uint32_t mH[4];         /// hold inter result
    uint64_t mTotalBytes;   /// total bytes

}; /// class Md5Stream

} } //namespace autil::legacy

