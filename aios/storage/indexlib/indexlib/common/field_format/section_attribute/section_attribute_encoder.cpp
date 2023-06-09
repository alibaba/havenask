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
#include "indexlib/common/field_format/section_attribute/section_attribute_encoder.h"

#include "indexlib/index/common/numeric_compress/S16Compressor.h"
#include "indexlib/index_define.h"
#include "indexlib/util/Status2Exception.h"

namespace indexlib::common {
IE_LOG_SETUP(common, SectionAttributeEncoder);

static uint8_t lenMaskForWeightUnit[4][2] = {
    {3, 0},
    {12, 2},
    {48, 4},
    {192, 6},
};
const uint8_t SectionAttributeEncoder::itemNumPerUnit[16] = {28, 21, 21, 21, 14, 9, 8, 7, 6, 6, 5, 5, 4, 3, 2, 1};

uint32_t SectionAttributeEncoder::Encode(section_len_t* lengths, section_fid_t* fids, section_weight_t* weights,
                                         uint32_t sectionCount, uint8_t* buf, uint32_t bufLen)
{
    assert(sectionCount <= MAX_SECTION_COUNT_PER_DOC);

    uint8_t* cursor = buf;
    uint8_t* bufEnd = buf + bufLen;
    EncodeSectionLens(lengths, sectionCount, cursor, bufEnd);
    EncodeFieldIds(fids, sectionCount, cursor, bufEnd);
    EncodeSectionWeights(weights, sectionCount, cursor, bufEnd);

    return cursor - buf;
}

void SectionAttributeEncoder::EncodeSectionWeights(const section_weight_t* srcWeights, uint32_t sectionCount,
                                                   uint8_t*& cursor, uint8_t* bufEnd)
{
    section_weight_t weightUnit[4];
    int32_t leftSectionCount = (int32_t)sectionCount;
    uint32_t encodedSectionCount = 0;

    while (leftSectionCount > 0) {
        uint32_t weightUnitCursor;
        for (weightUnitCursor = 0; weightUnitCursor < 4 && leftSectionCount > 0;) {
            weightUnit[weightUnitCursor++] = srcWeights[encodedSectionCount];
            encodedSectionCount++;
            leftSectionCount--;
        }
        while (weightUnitCursor < 4) {
            weightUnit[weightUnitCursor++] = 0;
        }

        EncodeWeightUnit(weightUnit, cursor, bufEnd);
    }
}

void SectionAttributeEncoder::EncodeWeightUnit(const section_weight_t* weightUnit, uint8_t*& cursor, uint8_t* bufEnd)
{
    CheckBufferLen(cursor, bufEnd, 1);

    uint8_t& flag = *cursor++;
    flag = 0;

    for (uint32_t i = 0; i < 4; i++) {
        if (weightUnit[i] == 0) {
            continue;
        }

        if (weightUnit[i] <= 255) {
            CheckBufferLen(cursor, bufEnd, 1);
            *cursor++ = (uint8_t)weightUnit[i];
            flag |= (0x1 << (2 * i));
        } else // if (weightUnit[i] <= 65535)
        {
            uint8_t* weightInByte = (uint8_t*)(weightUnit + i);
            CheckBufferLen(cursor, bufEnd, 2);

            *((uint16_t*)cursor) = *((uint16_t*)weightInByte);
            cursor += sizeof(uint16_t);
            flag |= (0x2 << (2 * i));
        }
    }
}

void SectionAttributeEncoder::EncodeFieldIds(const section_fid_t* srcFids, uint32_t sectionCount, uint8_t*& cursor,
                                             uint8_t* bufEnd)
{
    auto [status, unitCount] =
        index::S16Compressor::Encode((uint32_t*)cursor, (uint32_t*)bufEnd, srcFids, sectionCount);
    THROW_IF_STATUS_ERROR(status);
    cursor += sizeof(uint32_t) * unitCount;
}

void SectionAttributeEncoder::EncodeSectionLens(const section_len_t* srcLengths, uint32_t sectionCount,
                                                uint8_t*& cursor, uint8_t* bufEnd)
{
    section_len_t bufferToEncode[MAX_SECTION_COUNT_PER_DOC + 1];
    bufferToEncode[0] = sectionCount;
    memcpy(bufferToEncode + 1, srcLengths, sectionCount * sizeof(section_len_t));

    auto [status, unitCount] =
        index::S16Compressor::Encode((uint32_t*)cursor, (uint32_t*)bufEnd, bufferToEncode, sectionCount + 1);
    THROW_IF_STATUS_ERROR(status);

    cursor += sizeof(uint32_t) * unitCount;
}

void SectionAttributeEncoder::Decode(const uint8_t* srcBuf, uint32_t srcBufLen, uint8_t* destBuf, uint32_t destBufLen)
{
    const uint8_t* srcCursor = (const uint8_t*)srcBuf;
    uint8_t* destCursor = (uint8_t*)destBuf;
    uint8_t* destEnd = destBuf + destBufLen;
    DecodeSectionLens(srcCursor, destCursor, destEnd);

    uint32_t sectionCount = *((section_len_t*)destBuf);
    DecodeSectionFids(srcCursor, sectionCount, destCursor, destEnd);

    const uint8_t* srcEnd = srcBuf + srcBufLen;
    DecodeSectionWeights(srcCursor, srcEnd, sectionCount, destCursor, destEnd);
}

void SectionAttributeEncoder::DecodeSectionLens(const uint8_t*& srcCursor, uint8_t*& destCursor, uint8_t* destEnd)
{
    section_len_t* sectionLenCursor = (section_len_t*)destCursor;
    uint32_t k = ((uint32_t)(*srcCursor)) >> 28;
    CheckBufferLen(sectionLenCursor, (section_len_t*)destEnd, itemNumPerUnit[k]);
    uint32_t itemCount =
        index::S16Compressor::DecodeOneUnit(sectionLenCursor, (section_len_t*)destEnd, (const uint32_t*)srcCursor);
    uint32_t sectionCount = (uint32_t)(*sectionLenCursor);

    sectionLenCursor += itemCount;
    srcCursor += sizeof(uint32_t);
    int32_t leftCount = (int32_t)sectionCount;
    leftCount -= itemCount - 1;

    CheckBufferLen(sectionLenCursor, (section_len_t*)destEnd, leftCount);

    while (leftCount > 0) {
        const uint32_t* src = (const uint32_t*)srcCursor;
        uint32_t k = (*src) >> 28;
        // uint64_t tmpCursor=0;

        switch (k) {
        case 0:
            sectionLenCursor[0] = (*src) & 1;
            sectionLenCursor[1] = (*src >> 1) & 1;
            sectionLenCursor[2] = (*src >> 2) & 1;
            sectionLenCursor[3] = (*src >> 3) & 1;
            sectionLenCursor[4] = (*src >> 4) & 1;
            sectionLenCursor[5] = (*src >> 5) & 1;
            sectionLenCursor[6] = (*src >> 6) & 1;
            sectionLenCursor[7] = (*src >> 7) & 1;
            sectionLenCursor[8] = (*src >> 8) & 1;
            sectionLenCursor[9] = (*src >> 9) & 1;
            sectionLenCursor[10] = (*src >> 10) & 1;
            sectionLenCursor[11] = (*src >> 11) & 1;
            sectionLenCursor[12] = (*src >> 12) & 1;
            sectionLenCursor[13] = (*src >> 13) & 1;
            sectionLenCursor[14] = (*src >> 14) & 1;
            sectionLenCursor[15] = (*src >> 15) & 1;
            sectionLenCursor[16] = (*src >> 16) & 1;
            sectionLenCursor[17] = (*src >> 17) & 1;
            sectionLenCursor[18] = (*src >> 18) & 1;
            sectionLenCursor[19] = (*src >> 19) & 1;
            sectionLenCursor[20] = (*src >> 20) & 1;
            sectionLenCursor[21] = (*src >> 21) & 1;
            sectionLenCursor[22] = (*src >> 22) & 1;
            sectionLenCursor[23] = (*src >> 23) & 1;
            sectionLenCursor[24] = (*src >> 24) & 1;
            sectionLenCursor[25] = (*src >> 25) & 1;
            sectionLenCursor[26] = (*src >> 26) & 1;
            sectionLenCursor[27] = (*src >> 27) & 1;
            sectionLenCursor += 28;
            break;
        case 1:
            sectionLenCursor[0] = (*src) & 3;
            sectionLenCursor[1] = (*src >> 2) & 3;
            sectionLenCursor[2] = (*src >> 4) & 3;
            sectionLenCursor[3] = (*src >> 6) & 3;
            sectionLenCursor[4] = (*src >> 8) & 3;
            sectionLenCursor[5] = (*src >> 10) & 3;
            sectionLenCursor[6] = (*src >> 12) & 3;
            sectionLenCursor[7] = (*src >> 14) & 1;
            sectionLenCursor[8] = (*src >> 15) & 1;
            sectionLenCursor[9] = (*src >> 16) & 1;
            sectionLenCursor[10] = (*src >> 17) & 1;
            sectionLenCursor[11] = (*src >> 18) & 1;
            sectionLenCursor[12] = (*src >> 19) & 1;
            sectionLenCursor[13] = (*src >> 20) & 1;
            sectionLenCursor[14] = (*src >> 21) & 1;
            sectionLenCursor[15] = (*src >> 22) & 1;
            sectionLenCursor[16] = (*src >> 23) & 1;
            sectionLenCursor[17] = (*src >> 24) & 1;
            sectionLenCursor[18] = (*src >> 25) & 1;
            sectionLenCursor[19] = (*src >> 26) & 1;
            sectionLenCursor[20] = (*src >> 27) & 1;
            sectionLenCursor += 21;
            break;
        case 2:
            sectionLenCursor[0] = (*src) & 1;
            sectionLenCursor[1] = (*src >> 1) & 1;
            sectionLenCursor[2] = (*src >> 2) & 1;
            sectionLenCursor[3] = (*src >> 3) & 1;
            sectionLenCursor[4] = (*src >> 4) & 1;
            sectionLenCursor[5] = (*src >> 5) & 1;
            sectionLenCursor[6] = (*src >> 6) & 1;
            sectionLenCursor[7] = (*src >> 7) & 3;
            sectionLenCursor[8] = (*src >> 9) & 3;
            sectionLenCursor[9] = (*src >> 11) & 3;
            sectionLenCursor[10] = (*src >> 13) & 3;
            sectionLenCursor[11] = (*src >> 15) & 3;
            sectionLenCursor[12] = (*src >> 17) & 3;
            sectionLenCursor[13] = (*src >> 19) & 3;
            sectionLenCursor[14] = (*src >> 21) & 1;
            sectionLenCursor[15] = (*src >> 22) & 1;
            sectionLenCursor[16] = (*src >> 23) & 1;
            sectionLenCursor[17] = (*src >> 24) & 1;
            sectionLenCursor[18] = (*src >> 25) & 1;
            sectionLenCursor[19] = (*src >> 26) & 1;
            sectionLenCursor[20] = (*src >> 27) & 1;
            sectionLenCursor += 21;
            break;
        case 3:
            sectionLenCursor[0] = (*src) & 1;
            sectionLenCursor[1] = (*src >> 1) & 1;
            sectionLenCursor[2] = (*src >> 2) & 1;
            sectionLenCursor[3] = (*src >> 3) & 1;
            sectionLenCursor[4] = (*src >> 4) & 1;
            sectionLenCursor[5] = (*src >> 5) & 1;
            sectionLenCursor[6] = (*src >> 6) & 1;
            sectionLenCursor[7] = (*src >> 7) & 1;
            sectionLenCursor[8] = (*src >> 8) & 1;
            sectionLenCursor[9] = (*src >> 9) & 1;
            sectionLenCursor[10] = (*src >> 10) & 1;
            sectionLenCursor[11] = (*src >> 11) & 1;
            sectionLenCursor[12] = (*src >> 12) & 1;
            sectionLenCursor[13] = (*src >> 13) & 1;
            sectionLenCursor[14] = (*src >> 14) & 3;
            sectionLenCursor[15] = (*src >> 16) & 3;
            sectionLenCursor[16] = (*src >> 18) & 3;
            sectionLenCursor[17] = (*src >> 20) & 3;
            sectionLenCursor[18] = (*src >> 22) & 3;
            sectionLenCursor[19] = (*src >> 24) & 3;
            sectionLenCursor[20] = (*src >> 26) & 3;
            sectionLenCursor += 21;
            break;
        case 4:
            sectionLenCursor[0] = (*src) & 3;
            sectionLenCursor[1] = (*src >> 2) & 3;
            sectionLenCursor[2] = (*src >> 4) & 3;
            sectionLenCursor[3] = (*src >> 6) & 3;
            sectionLenCursor[4] = (*src >> 8) & 3;
            sectionLenCursor[5] = (*src >> 10) & 3;
            sectionLenCursor[6] = (*src >> 12) & 3;
            sectionLenCursor[7] = (*src >> 14) & 3;
            sectionLenCursor[8] = (*src >> 16) & 3;
            sectionLenCursor[9] = (*src >> 18) & 3;
            sectionLenCursor[10] = (*src >> 20) & 3;
            sectionLenCursor[11] = (*src >> 22) & 3;
            sectionLenCursor[12] = (*src >> 24) & 3;
            sectionLenCursor[13] = (*src >> 26) & 3;
            sectionLenCursor += 14;
            break;
        case 5:
            sectionLenCursor[0] = (*src) & 15;
            sectionLenCursor[1] = (*src >> 4) & 7;
            sectionLenCursor[2] = (*src >> 7) & 7;
            sectionLenCursor[3] = (*src >> 10) & 7;
            sectionLenCursor[4] = (*src >> 13) & 7;
            sectionLenCursor[5] = (*src >> 16) & 7;
            sectionLenCursor[6] = (*src >> 19) & 7;
            sectionLenCursor[7] = (*src >> 22) & 7;
            sectionLenCursor[8] = (*src >> 25) & 7;
            sectionLenCursor += 9;
            break;
        case 6:
            sectionLenCursor[0] = (*src) & 7;
            sectionLenCursor[1] = (*src >> 3) & 15;
            sectionLenCursor[2] = (*src >> 7) & 15;
            sectionLenCursor[3] = (*src >> 11) & 15;
            sectionLenCursor[4] = (*src >> 15) & 15;
            sectionLenCursor[5] = (*src >> 19) & 7;
            sectionLenCursor[6] = (*src >> 22) & 7;
            sectionLenCursor[7] = (*src >> 25) & 7;
            sectionLenCursor += 8;
            break;
        case 7:
            sectionLenCursor[0] = (*src) & 15;
            sectionLenCursor[1] = (*src >> 4) & 15;
            sectionLenCursor[2] = (*src >> 8) & 15;
            sectionLenCursor[3] = (*src >> 12) & 15;
            sectionLenCursor[4] = (*src >> 16) & 15;
            sectionLenCursor[5] = (*src >> 20) & 15;
            sectionLenCursor[6] = (*src >> 24) & 15;
            sectionLenCursor += 7;
            break;
        case 8:
            sectionLenCursor[0] = (*src) & 31;
            sectionLenCursor[1] = (*src >> 5) & 31;
            sectionLenCursor[2] = (*src >> 10) & 31;
            sectionLenCursor[3] = (*src >> 15) & 31;
            sectionLenCursor[4] = (*src >> 20) & 15;
            sectionLenCursor[5] = (*src >> 24) & 15;
            sectionLenCursor += 6;
            break;
        case 9:
            sectionLenCursor[0] = (*src) & 15;
            sectionLenCursor[1] = (*src >> 4) & 15;
            sectionLenCursor[2] = (*src >> 8) & 31;
            sectionLenCursor[3] = (*src >> 13) & 31;
            sectionLenCursor[4] = (*src >> 18) & 31;
            sectionLenCursor[5] = (*src >> 23) & 31;
            sectionLenCursor += 6;
            break;
        case 10:
            sectionLenCursor[0] = (*src) & 63;
            sectionLenCursor[1] = (*src >> 6) & 63;
            sectionLenCursor[2] = (*src >> 12) & 63;
            sectionLenCursor[3] = (*src >> 18) & 31;
            sectionLenCursor[4] = (*src >> 23) & 31;
            sectionLenCursor += 5;
            break;
        case 11:
            sectionLenCursor[0] = (*src) & 31;
            sectionLenCursor[1] = (*src >> 5) & 31;
            sectionLenCursor[2] = (*src >> 10) & 63;
            sectionLenCursor[3] = (*src >> 16) & 63;
            sectionLenCursor[4] = (*src >> 22) & 63;
            sectionLenCursor += 5;
            break;
        case 12:
            sectionLenCursor[0] = (*src) & 127;
            sectionLenCursor[1] = (*src >> 7) & 127;
            sectionLenCursor[2] = (*src >> 14) & 127;
            sectionLenCursor[3] = (*src >> 21) & 127;
            sectionLenCursor += 4;
            break;
        case 13:
            sectionLenCursor[0] = (*src) & 1023;
            sectionLenCursor[1] = (*src >> 10) & 511;
            sectionLenCursor[2] = (*src >> 19) & 511;
            sectionLenCursor += 3;
            break;
        case 14:
            sectionLenCursor[0] = (*src) & 16383;
            sectionLenCursor[1] = (*src >> 14) & 16383;
            sectionLenCursor += 2;
            break;
        case 15:
            *sectionLenCursor++ = (*src) & ((1 << 28) - 1);
            break;
        }

        itemCount = itemNumPerUnit[k];
        leftCount -= itemCount;
        srcCursor += sizeof(uint32_t);
    }

    destCursor = (uint8_t*)sectionLenCursor;
}

void SectionAttributeEncoder::DecodeSectionFids(const uint8_t*& srcCursor, uint32_t sectionCount, uint8_t*& destCursor,
                                                uint8_t* destEnd)
{
    int32_t leftCount = (int32_t)sectionCount;
    section_fid_t* fidCursor = (section_fid_t*)destCursor;
    CheckBufferLen(fidCursor, (section_fid_t*)destEnd, sectionCount);

    bool firstFieldFlag = false;

    while (leftCount > 0) {
        const uint32_t* src = (const uint32_t*)srcCursor;
        __builtin_prefetch(src + 8, 0, 3);
        section_fid_t* tmpDestCursor = fidCursor;

        uint32_t k = (*src) >> 28;
        switch (k) {
        case 0:
            *tmpDestCursor = (*src) & 1;
            if (firstFieldFlag)
                *tmpDestCursor += *(tmpDestCursor - 1);
            else
                firstFieldFlag = true;
            *(tmpDestCursor + 1) = ((*src >> 1) & 1) + *tmpDestCursor;
            *(tmpDestCursor + 2) = ((*src >> 2) & 1) + *(tmpDestCursor + 1);
            *(tmpDestCursor + 3) = ((*src >> 3) & 1) + *(tmpDestCursor + 2);
            *(tmpDestCursor + 4) = ((*src >> 4) & 1) + *(tmpDestCursor + 3);
            *(tmpDestCursor + 5) = ((*src >> 5) & 1) + *(tmpDestCursor + 4);
            *(tmpDestCursor + 6) = ((*src >> 6) & 1) + *(tmpDestCursor + 5);
            *(tmpDestCursor + 7) = ((*src >> 7) & 1) + *(tmpDestCursor + 6);
            *(tmpDestCursor + 8) = ((*src >> 8) & 1) + *(tmpDestCursor + 7);
            *(tmpDestCursor + 9) = ((*src >> 9) & 1) + *(tmpDestCursor + 8);
            *(tmpDestCursor + 10) = ((*src >> 10) & 1) + *(tmpDestCursor + 9);
            *(tmpDestCursor + 11) = ((*src >> 11) & 1) + *(tmpDestCursor + 10);
            *(tmpDestCursor + 12) = ((*src >> 12) & 1) + *(tmpDestCursor + 11);
            *(tmpDestCursor + 13) = ((*src >> 13) & 1) + *(tmpDestCursor + 12);
            *(tmpDestCursor + 14) = ((*src >> 14) & 1) + *(tmpDestCursor + 13);
            *(tmpDestCursor + 15) = ((*src >> 15) & 1) + *(tmpDestCursor + 14);
            *(tmpDestCursor + 16) = ((*src >> 16) & 1) + *(tmpDestCursor + 15);
            *(tmpDestCursor + 17) = ((*src >> 17) & 1) + *(tmpDestCursor + 16);
            *(tmpDestCursor + 18) = ((*src >> 18) & 1) + *(tmpDestCursor + 17);
            *(tmpDestCursor + 19) = ((*src >> 19) & 1) + *(tmpDestCursor + 18);
            *(tmpDestCursor + 20) = ((*src >> 20) & 1) + *(tmpDestCursor + 19);
            *(tmpDestCursor + 21) = ((*src >> 21) & 1) + *(tmpDestCursor + 20);
            *(tmpDestCursor + 22) = ((*src >> 22) & 1) + *(tmpDestCursor + 21);
            *(tmpDestCursor + 23) = ((*src >> 23) & 1) + *(tmpDestCursor + 22);
            *(tmpDestCursor + 24) = ((*src >> 24) & 1) + *(tmpDestCursor + 23);
            *(tmpDestCursor + 25) = ((*src >> 25) & 1) + *(tmpDestCursor + 24);
            *(tmpDestCursor + 26) = ((*src >> 26) & 1) + *(tmpDestCursor + 25);
            *(tmpDestCursor + 27) = ((*src >> 27) & 1) + *(tmpDestCursor + 26);
            tmpDestCursor += 28;
            break;
        case 1:
            *tmpDestCursor = (*src) & 3;
            if (firstFieldFlag)
                *tmpDestCursor += *(tmpDestCursor - 1);
            else
                firstFieldFlag = true;
            *(tmpDestCursor + 1) = ((*src >> 2) & 3) + *(tmpDestCursor);
            *(tmpDestCursor + 2) = ((*src >> 4) & 3) + *(tmpDestCursor + 1);
            *(tmpDestCursor + 3) = ((*src >> 6) & 3) + *(tmpDestCursor + 2);
            *(tmpDestCursor + 4) = ((*src >> 8) & 3) + *(tmpDestCursor + 3);
            *(tmpDestCursor + 5) = ((*src >> 10) & 3) + *(tmpDestCursor + 4);
            *(tmpDestCursor + 6) = ((*src >> 12) & 3) + *(tmpDestCursor + 5);
            *(tmpDestCursor + 7) = ((*src >> 14) & 1) + *(tmpDestCursor + 6);
            *(tmpDestCursor + 8) = ((*src >> 15) & 1) + *(tmpDestCursor + 7);
            *(tmpDestCursor + 9) = ((*src >> 16) & 1) + *(tmpDestCursor + 8);
            *(tmpDestCursor + 10) = ((*src >> 17) & 1) + *(tmpDestCursor + 9);
            *(tmpDestCursor + 11) = ((*src >> 18) & 1) + *(tmpDestCursor + 10);
            *(tmpDestCursor + 12) = ((*src >> 19) & 1) + *(tmpDestCursor + 11);
            *(tmpDestCursor + 13) = ((*src >> 20) & 1) + *(tmpDestCursor + 12);
            *(tmpDestCursor + 14) = ((*src >> 21) & 1) + *(tmpDestCursor + 13);
            *(tmpDestCursor + 15) = ((*src >> 22) & 1) + *(tmpDestCursor + 14);
            *(tmpDestCursor + 16) = ((*src >> 23) & 1) + *(tmpDestCursor + 15);
            *(tmpDestCursor + 17) = ((*src >> 24) & 1) + *(tmpDestCursor + 16);
            *(tmpDestCursor + 18) = ((*src >> 25) & 1) + *(tmpDestCursor + 17);
            *(tmpDestCursor + 19) = ((*src >> 26) & 1) + *(tmpDestCursor + 18);
            *(tmpDestCursor + 20) = ((*src >> 27) & 1) + *(tmpDestCursor + 19);
            tmpDestCursor += 21;
            break;
        case 2:
            *tmpDestCursor = (*src) & 1;
            if (firstFieldFlag)
                *tmpDestCursor += *(tmpDestCursor - 1);
            else
                firstFieldFlag = true;
            *(tmpDestCursor + 1) = ((*src >> 1) & 1) + *(tmpDestCursor);
            *(tmpDestCursor + 2) = ((*src >> 2) & 1) + *(tmpDestCursor + 1);
            *(tmpDestCursor + 3) = ((*src >> 3) & 1) + *(tmpDestCursor + 2);
            *(tmpDestCursor + 4) = ((*src >> 4) & 1) + *(tmpDestCursor + 3);
            *(tmpDestCursor + 5) = ((*src >> 5) & 1) + *(tmpDestCursor + 4);
            *(tmpDestCursor + 6) = ((*src >> 6) & 1) + *(tmpDestCursor + 5);
            *(tmpDestCursor + 7) = ((*src >> 7) & 3) + *(tmpDestCursor + 6);
            *(tmpDestCursor + 8) = ((*src >> 9) & 3) + *(tmpDestCursor + 7);
            *(tmpDestCursor + 9) = ((*src >> 11) & 3) + *(tmpDestCursor + 8);
            *(tmpDestCursor + 10) = ((*src >> 13) & 3) + *(tmpDestCursor + 9);
            *(tmpDestCursor + 11) = ((*src >> 15) & 3) + *(tmpDestCursor + 10);
            *(tmpDestCursor + 12) = ((*src >> 17) & 3) + *(tmpDestCursor + 11);
            *(tmpDestCursor + 13) = ((*src >> 19) & 3) + *(tmpDestCursor + 12);
            *(tmpDestCursor + 14) = ((*src >> 21) & 1) + *(tmpDestCursor + 13);
            *(tmpDestCursor + 15) = ((*src >> 22) & 1) + *(tmpDestCursor + 14);
            *(tmpDestCursor + 16) = ((*src >> 23) & 1) + *(tmpDestCursor + 15);
            *(tmpDestCursor + 17) = ((*src >> 24) & 1) + *(tmpDestCursor + 16);
            *(tmpDestCursor + 18) = ((*src >> 25) & 1) + *(tmpDestCursor + 17);
            *(tmpDestCursor + 19) = ((*src >> 26) & 1) + *(tmpDestCursor + 18);
            *(tmpDestCursor + 20) = ((*src >> 27) & 1) + *(tmpDestCursor + 19);
            tmpDestCursor += 21;
            break;
        case 3:
            *tmpDestCursor = (*src) & 1;
            if (firstFieldFlag)
                *tmpDestCursor += *(tmpDestCursor - 1);
            else
                firstFieldFlag = true;
            *(tmpDestCursor + 1) = ((*src >> 1) & 1) + *(tmpDestCursor);
            *(tmpDestCursor + 2) = ((*src >> 2) & 1) + *(tmpDestCursor + 1);
            *(tmpDestCursor + 3) = ((*src >> 3) & 1) + *(tmpDestCursor + 2);
            *(tmpDestCursor + 4) = ((*src >> 4) & 1) + *(tmpDestCursor + 3);
            *(tmpDestCursor + 5) = ((*src >> 5) & 1) + *(tmpDestCursor + 4);
            *(tmpDestCursor + 6) = ((*src >> 6) & 1) + *(tmpDestCursor + 5);
            *(tmpDestCursor + 7) = ((*src >> 7) & 1) + *(tmpDestCursor + 6);
            *(tmpDestCursor + 8) = ((*src >> 8) & 1) + *(tmpDestCursor + 7);
            *(tmpDestCursor + 9) = ((*src >> 9) & 1) + *(tmpDestCursor + 8);
            *(tmpDestCursor + 10) = ((*src >> 10) & 1) + *(tmpDestCursor + 9);
            *(tmpDestCursor + 11) = ((*src >> 11) & 1) + *(tmpDestCursor + 10);
            *(tmpDestCursor + 12) = ((*src >> 12) & 1) + *(tmpDestCursor + 11);
            *(tmpDestCursor + 13) = ((*src >> 13) & 1) + *(tmpDestCursor + 12);
            *(tmpDestCursor + 14) = ((*src >> 14) & 3) + *(tmpDestCursor + 13);
            *(tmpDestCursor + 15) = ((*src >> 16) & 3) + *(tmpDestCursor + 14);
            *(tmpDestCursor + 16) = ((*src >> 18) & 3) + *(tmpDestCursor + 15);
            *(tmpDestCursor + 17) = ((*src >> 20) & 3) + *(tmpDestCursor + 16);
            *(tmpDestCursor + 18) = ((*src >> 22) & 3) + *(tmpDestCursor + 17);
            *(tmpDestCursor + 19) = ((*src >> 24) & 3) + *(tmpDestCursor + 18);
            *(tmpDestCursor + 20) = ((*src >> 26) & 3) + *(tmpDestCursor + 19);
            tmpDestCursor += 21;
            break;
        case 4:
            *tmpDestCursor = (*src) & 3;
            if (firstFieldFlag)
                *tmpDestCursor += *(tmpDestCursor - 1);
            else
                firstFieldFlag = true;
            *(tmpDestCursor + 1) = ((*src >> 2) & 3) + *(tmpDestCursor);
            *(tmpDestCursor + 2) = ((*src >> 4) & 3) + *(tmpDestCursor + 1);
            *(tmpDestCursor + 3) = ((*src >> 6) & 3) + *(tmpDestCursor + 2);
            *(tmpDestCursor + 4) = ((*src >> 8) & 3) + *(tmpDestCursor + 3);
            *(tmpDestCursor + 5) = ((*src >> 10) & 3) + *(tmpDestCursor + 4);
            *(tmpDestCursor + 6) = ((*src >> 12) & 3) + *(tmpDestCursor + 5);
            *(tmpDestCursor + 7) = ((*src >> 14) & 3) + *(tmpDestCursor + 6);
            *(tmpDestCursor + 8) = ((*src >> 16) & 3) + *(tmpDestCursor + 7);
            *(tmpDestCursor + 9) = ((*src >> 18) & 3) + *(tmpDestCursor + 8);
            *(tmpDestCursor + 10) = ((*src >> 20) & 3) + *(tmpDestCursor + 9);
            *(tmpDestCursor + 11) = ((*src >> 22) & 3) + *(tmpDestCursor + 10);
            *(tmpDestCursor + 12) = ((*src >> 24) & 3) + *(tmpDestCursor + 11);
            *(tmpDestCursor + 13) = ((*src >> 26) & 3) + *(tmpDestCursor + 12);
            tmpDestCursor += 14;
            break;
        case 5:
            *tmpDestCursor = (*src) & 15;
            if (firstFieldFlag)
                *tmpDestCursor += *(tmpDestCursor - 1);
            else
                firstFieldFlag = true;
            *(tmpDestCursor + 1) = ((*src >> 4) & 7) + *(tmpDestCursor);
            *(tmpDestCursor + 2) = ((*src >> 7) & 7) + *(tmpDestCursor + 1);
            *(tmpDestCursor + 3) = ((*src >> 10) & 7) + *(tmpDestCursor + 2);
            *(tmpDestCursor + 4) = ((*src >> 13) & 7) + *(tmpDestCursor + 3);
            *(tmpDestCursor + 5) = ((*src >> 16) & 7) + *(tmpDestCursor + 4);
            *(tmpDestCursor + 6) = ((*src >> 19) & 7) + *(tmpDestCursor + 5);
            *(tmpDestCursor + 7) = ((*src >> 22) & 7) + *(tmpDestCursor + 6);
            *(tmpDestCursor + 8) = ((*src >> 25) & 7) + *(tmpDestCursor + 7);
            tmpDestCursor += 9;
            break;
        case 6:
            *tmpDestCursor = (*src) & 7;
            if (firstFieldFlag)
                *tmpDestCursor += *(tmpDestCursor - 1);
            else
                firstFieldFlag = true;
            *(tmpDestCursor + 1) = ((*src >> 3) & 15) + *(tmpDestCursor);
            *(tmpDestCursor + 2) = ((*src >> 7) & 15) + *(tmpDestCursor + 1);
            *(tmpDestCursor + 3) = ((*src >> 11) & 15) + *(tmpDestCursor + 2);
            *(tmpDestCursor + 4) = ((*src >> 15) & 15) + *(tmpDestCursor + 3);
            *(tmpDestCursor + 5) = ((*src >> 19) & 7) + *(tmpDestCursor + 4);
            *(tmpDestCursor + 6) = ((*src >> 22) & 7) + *(tmpDestCursor + 5);
            *(tmpDestCursor + 7) = ((*src >> 25) & 7) + *(tmpDestCursor + 6);
            tmpDestCursor += 8;
            break;
        case 7:
            *tmpDestCursor = (*src) & 15;
            if (firstFieldFlag)
                *tmpDestCursor += *(tmpDestCursor - 1);
            else
                firstFieldFlag = true;
            *(tmpDestCursor + 1) = ((*src >> 4) & 15) + *(tmpDestCursor);
            *(tmpDestCursor + 2) = ((*src >> 8) & 15) + *(tmpDestCursor + 1);
            *(tmpDestCursor + 3) = ((*src >> 12) & 15) + *(tmpDestCursor + 2);
            *(tmpDestCursor + 4) = ((*src >> 16) & 15) + *(tmpDestCursor + 3);
            *(tmpDestCursor + 5) = ((*src >> 20) & 15) + *(tmpDestCursor + 4);
            *(tmpDestCursor + 6) = ((*src >> 24) & 15) + *(tmpDestCursor + 5);
            tmpDestCursor += 7;
            break;
        case 8:
            *tmpDestCursor = (*src) & 31;
            if (firstFieldFlag)
                *tmpDestCursor += *(tmpDestCursor - 1);
            else
                firstFieldFlag = true;
            *(tmpDestCursor + 1) = ((*src >> 5) & 31) + *(tmpDestCursor);
            *(tmpDestCursor + 2) = ((*src >> 10) & 31) + *(tmpDestCursor + 1);
            *(tmpDestCursor + 3) = ((*src >> 15) & 31) + *(tmpDestCursor + 2);
            *(tmpDestCursor + 4) = ((*src >> 20) & 15) + *(tmpDestCursor + 3);
            *(tmpDestCursor + 5) = ((*src >> 24) & 15) + *(tmpDestCursor + 4);
            tmpDestCursor += 6;
            break;
        case 9:
            *tmpDestCursor = (*src) & 15;
            if (firstFieldFlag)
                *tmpDestCursor += *(tmpDestCursor - 1);
            else
                firstFieldFlag = true;
            *(tmpDestCursor + 1) = ((*src >> 4) & 15) + *(tmpDestCursor);
            *(tmpDestCursor + 2) = ((*src >> 8) & 31) + *(tmpDestCursor + 1);
            *(tmpDestCursor + 3) = ((*src >> 13) & 31) + *(tmpDestCursor + 2);
            *(tmpDestCursor + 4) = ((*src >> 18) & 31) + *(tmpDestCursor + 3);
            *(tmpDestCursor + 5) = ((*src >> 23) & 31) + *(tmpDestCursor + 4);
            tmpDestCursor += 6;
            break;
        case 10:
            *tmpDestCursor = (*src) & 63;
            if (firstFieldFlag)
                *tmpDestCursor += *(tmpDestCursor - 1);
            else
                firstFieldFlag = true;
            *(tmpDestCursor + 1) = ((*src >> 6) & 63) + *(tmpDestCursor);
            *(tmpDestCursor + 2) = ((*src >> 12) & 63) + *(tmpDestCursor + 1);
            *(tmpDestCursor + 3) = ((*src >> 18) & 31) + *(tmpDestCursor + 2);
            *(tmpDestCursor + 4) = ((*src >> 23) & 31) + *(tmpDestCursor + 3);
            tmpDestCursor += 5;
            break;
        case 11:
            *tmpDestCursor = (*src) & 31;
            if (firstFieldFlag)
                *tmpDestCursor += *(tmpDestCursor - 1);
            else
                firstFieldFlag = true;
            *(tmpDestCursor + 1) = ((*src >> 5) & 31) + *(tmpDestCursor);
            *(tmpDestCursor + 2) = ((*src >> 10) & 63) + *(tmpDestCursor + 1);
            *(tmpDestCursor + 3) = ((*src >> 16) & 63) + *(tmpDestCursor + 2);
            *(tmpDestCursor + 4) = ((*src >> 22) & 63) + *(tmpDestCursor + 3);
            tmpDestCursor += 5;
            break;
        case 12:
            *tmpDestCursor = (*src) & 127;
            if (firstFieldFlag)
                *tmpDestCursor += *(tmpDestCursor - 1);
            else
                firstFieldFlag = true;
            *(tmpDestCursor + 1) = ((*src >> 7) & 127) + *(tmpDestCursor);
            *(tmpDestCursor + 2) = ((*src >> 14) & 127) + *(tmpDestCursor + 1);
            *(tmpDestCursor + 3) = ((*src >> 21) & 127) + *(tmpDestCursor + 2);
            tmpDestCursor += 4;
            break;
        case 13:
            *tmpDestCursor = (*src) & 1023;
            if (firstFieldFlag)
                *tmpDestCursor += *(tmpDestCursor - 1);
            else
                firstFieldFlag = true;
            *(tmpDestCursor + 1) = ((*src >> 10) & 511) + *(tmpDestCursor);
            *(tmpDestCursor + 2) = ((*src >> 19) & 511) + *(tmpDestCursor + 1);
            tmpDestCursor += 3;
            break;
        case 14:
            *tmpDestCursor = (*src) & 16383;
            if (firstFieldFlag)
                *tmpDestCursor += *(tmpDestCursor - 1);
            else
                firstFieldFlag = true;
            *(tmpDestCursor + 1) = ((*src >> 14) & 16383) + *(tmpDestCursor);
            tmpDestCursor += 2;
            break;
        case 15:
            *tmpDestCursor = (*src) & ((1 << 28) - 1);
            if (firstFieldFlag)
                *tmpDestCursor += *(tmpDestCursor - 1);
            else
                firstFieldFlag = true;
            tmpDestCursor += 1;
            break;
        }

        leftCount -= itemNumPerUnit[k];
        fidCursor += itemNumPerUnit[k];
        srcCursor += sizeof(uint32_t);
    }

    destCursor = (uint8_t*)fidCursor;
}

void SectionAttributeEncoder::DecodeSectionWeights(const uint8_t*& srcCursor, const uint8_t* srcEnd,
                                                   uint32_t sectionCount, uint8_t*& destCursor, uint8_t* destEnd)
{
    section_weight_t* weightsCursor = (section_weight_t*)destCursor;
    uint32_t decodedWeightCount = 0;
    CheckBufferLen(weightsCursor, (section_weight_t*)destEnd, sectionCount);

    while (decodedWeightCount < sectionCount) {
        __builtin_prefetch(srcCursor + 64, 0, 3);
        uint8_t flag = *srcCursor++;

        uint8_t length0 = (flag & lenMaskForWeightUnit[0][0]) >> lenMaskForWeightUnit[0][1];
        uint8_t length1 = (flag & lenMaskForWeightUnit[1][0]) >> lenMaskForWeightUnit[1][1];
        uint8_t length2 = (flag & lenMaskForWeightUnit[2][0]) >> lenMaskForWeightUnit[2][1];
        uint8_t length3 = (flag & lenMaskForWeightUnit[3][0]) >> lenMaskForWeightUnit[3][1];

        uint32_t offset2 = length0 + length1;
        uint32_t offset3 = length0 + length1 + length2;

        weightsCursor[0] = GetWeight(srcCursor, length0);
        weightsCursor[1] = GetWeight(srcCursor + length0, length1);
        weightsCursor[2] = GetWeight(srcCursor + offset2, length2);
        weightsCursor[3] = GetWeight(srcCursor + offset3, length3);

        weightsCursor += 4;
        decodedWeightCount += 4;
        srcCursor += (offset3 + length3);
    }
    destCursor = (uint8_t*)weightsCursor;
}
} // namespace indexlib::common
