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
#ifndef ADVANCED_PACKET_H
#define ADVANCED_PACKET_H

#include <stdint.h>
#include <stddef.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <stdio.h>
#include <iostream>

#include "aios/network/anet/packet.h"
#include "aios/network/anet/debug.h"
#include "aios/network/anet/crc.h"
#include "aios/network/anet/log.h"
#include "aios/network/anet/globalflags.h"
#include "aios/network/anet/databuffer.h"
#include "aios/network/anet/common.h"
#include "aios/network/anet/connectionpriority.h"
#include "aios/network/anet/ilogger.h"

BEGIN_ANET_NS();

/**
 * This file defines the advanced packet. Advance packet is a subclass
 * of ANET Packet, and it extends its header with more fields which is
 * defined in AdvancePacketHeader and AdvancePacketHeaderAddon.
 * AdvancePacketHeaderAddon is a way to extend the AdvancePacket smoothly
 * in future.
 * In AdvancePacket, we define the following fields.
 *
 * Name              Len(Byte)   Explain
 * --------------------------------------------------------------------------
 * ttl               4           ms to live
 * packetChecksum    4           checksum field
 * chidHigh          4           high 32 bit channel ID
 * version           1:5         version number reserved for co-existance
 * checksumFlag      1:3         flags which indicate which checksum method
 *                               is used
 * packetFlags       1:5         low 5 bits, packet flags
 * priority          1:3         high 3 bits, packet/connection priority
 * nextHeaderType    1           defines 1st addon headerType, 0 means data.
 *                               other type TBD.
 *                               highest bit 1 means if the addon is required
 *                               or not.
 * reserved          1
 *
 * Total header length: 16 bytes
 * Total header length plus Packet header: 32 bytes
 *
 * For AdvancePacketHeaderAddon, it always contains the following two fields
 * at the very beginning.
 *
 * nextHeaderType    1           Next addon type and attribute, same as above.
 * thisHeaderLen     2           Header length of this addon, used by old
 *                               code to skip the optional addon.
 */

/******************************************************************************/
/*                      addon header definitions                              */
/******************************************************************************/

/* Definition of addon packet header attribute. Currently the highest bit
 * 1 means the addon header is required. 0 means the addon header is optional,
 * which can be ignored. */
const uint8_t ADDON_ATTR_MASK = 0x80;
typedef enum
{
  ADDON_ATTR_OPTIONAL = 0x00,
  ADDON_ATTR_REQUIRED = 0x80
} ADDON_HEADER_ATTR;

typedef enum
{
    HEADER_NO_ADDON = 0
    /* Add new addon header type below */
} HEADER_ADDON_TYPE;

inline bool HasAddon(uint8_t addonType)
{
    return (addonType == HEADER_NO_ADDON);
}

inline bool IsAddonRequired(uint8_t addonType)
{
    return ((addonType & ADDON_ATTR_MASK) == ADDON_ATTR_REQUIRED);
}

/******************************************************************************/
/*                      packet flags definitions                              */
/******************************************************************************/
/* We have totally 8 bits for flags, and they are reserved for the following
 * reasons (from low to high):
 * bit 1-3: packet priority
 * bit 4  : End of Reply(EOR) flag
 * bit 5-6: compression bits
 * bit 7-8: checksum bits */

/* EOR means "end of reply", with this bit set, it means more reply will be
 * coming so don't remove the channel on recieving this reply. This flag
 * will be set by the server side when multiple messages need to get back
 * to the client.
 * Currently this bit is for the following situations:
 * 1. When server sending back NACK message before sending back the real reply.
 */

/* Compression mask is used to flag if the packet is compressed or not, and if
 * it is, which kind of compression algorithm it is.
 * We allocate 2 bits for this:
 * 00 - means no compression
 * 01 - compression method 1
 * 10 - compression method 2
 * 11 - compressoin method 3
 */
typedef enum
{
    PACKET_FLAGS_EOR_MASK = 0x01,
    PACKET_FLAGS_COMPRESSION_MASK = 0x06,
    PACKET_FLAGS_CHECKSUM_MASK = 0x18
} PACKET_FLAGS;

typedef enum
{
    PACKET_FLAGS_NOCOMPRESSION = 0x00,
    PACKET_FLAGS_COMPRESSION_1 = 0x02,
    PACKET_FLAGS_COMPRESSION_2 = 0x04,
    PACKET_FLAGS_COMPRESSION_3 = 0x06
} PACKET_FLAGS_COMPRESSION;

/* We can have up to 3 checksum algorithms. */
typedef enum
{
    PACKET_FLAGS_NOCHECKSUM = 0x00,
    PACKET_FLAGS_CHECKSUM_CRC32C = 0x08,
    PACKET_FLAGS_CHECKSUM_2 = 0x10,
    PACKET_FLAGS_CHECKSUM_3 = 0x18
} PACKET_FLAGS_CHECKSUM;

inline bool IsEndOfReply(uint8_t flags)
{
    return(flags & PACKET_FLAGS_EOR_MASK);
}

inline bool IsCompressedPacket(uint8_t flags)
{
    return ((flags & PACKET_FLAGS_COMPRESSION_MASK)
             != PACKET_FLAGS_NOCOMPRESSION);
}

inline int GetChecksumMethod(uint8_t flags)
{
    return (flags & PACKET_FLAGS_CHECKSUM_MASK);
}

inline uint8_t SetChecksumMethod(PACKET_FLAGS_CHECKSUM checksumMethod, uint8_t flags)
{
    return ((flags & ~PACKET_FLAGS_CHECKSUM_MASK ) | (uint8_t)checksumMethod);
}
/******************************************************************************/
/*                      Advance Packet Header                                 */
/******************************************************************************/
/* AddonHeaderBase
 * All addon headers should inherit from the AddonHeaderBase class and implement
 * encode, decode virtual functions. */
class AddonHeaderBase
{
public:
    AddonHeaderBase() { }
    virtual ~AddonHeaderBase() { }

    virtual bool encode(DataBuffer *output) = 0;
    virtual bool decode(DataBuffer *input, PacketHeader *header) = 0;

    uint16_t GetThisHeaderLen() { return thisHeaderLen; }
    uint8_t GetNextHeaderType() { return nextHeaderType; }

protected:
    uint8_t  nextHeaderType;
    uint16_t thisHeaderLen;
};

/* AdvancePacketHeader defination. */
class  AdvancePacketHeader
{
public:
    uint32_t ttlInMs;
    uint32_t packetChecksum;
    uint32_t chidHigh;

    /* Anonymous union to make the value accessible in encode. */
    union
    {
        int8_t flagsData;
        struct
        {
            uint8_t  priority : 3;
            uint8_t  packetFlags : 5;
        };
    };
    uint8_t  packetVersion;
    uint8_t  nextHeaderType;
    uint8_t  reserved;

    /* Addon header, currently 0 */
    /* AddonHeaderBase addonHeaders[0]; */
};

/******************************************************************************/
/*                      Advance Packet Definations                            */
/******************************************************************************/
template <class T>
class AdvancePacket : public T
{
friend class AdvancePacketTest_testEncodeDecode_Test;

public:
    static const int CHID_HIGH_OFFSET = 32;
    /**
     * We override ANET header field pcode, the 16bit as a flag to mark the packet
     * as AdvancePacket or old ANET packet. In the old ANET code, pcode is splitted
     * into two 16 bit value to represent service id and method id respectively.
     * We assume it is impossible for a service to have so many methods so we can
     * steal the highest bit of method id as the AdvancePacket flag.
     * Since pcode is overloaded as the RPC error code in reply message, we should
     * make sure that the highest bit is reserved, that is the highest error code
     * is 32768.
     */
     #define ADVANCE_PACKET_MASK 0x8000L


    /* Class level public functions. */
    /* Tool function to judge if the packet is an advance packet or not by the
     * pcode field of this packet. */
    static bool IsAdvancePacket(int pcode)
    {
        return (pcode & ADVANCE_PACKET_MASK);
    }

    /* Constructor and destructor */
    AdvancePacket();
    virtual ~AdvancePacket();

    /*************************************************************************/
    /* overridden virtual functions from Packet */
    virtual bool encode(DataBuffer *output);
    virtual bool decode(DataBuffer *input, PacketHeader *header);

    /* Get channel id by combing the chid field in Packet and chidHigh field
     * in AdvancePacket. */
    virtual uint64_t getChannelId( void )
    {
        /* magic number MAX_CHANNELID_OFFSET means that the chid field in Packet only reserves
         * MAX_CHANNELID_OFFSET bit for chid. */
        uint64_t chid = Packet::getChannelId();
        chid = chid + ((uint64_t)mAdvHeader.chidHigh << CHID_HIGH_OFFSET);
        return chid;
    }

    virtual void setChannelId(uint64_t chid)
    {
        /* We would spit the 64 bit number into two parts, low MAX_CHANNELID_OFFSET bit will
         * stored in the chid field in Packet header, high 32 bit in chidHigh
         * field in AdvancePacket. The highest 4 bit is reserved for HTTP channel ID and
         * admin channel ID etc. So the maximum chid is 60 bit. */
        DBGASSERT(chid <= (1ULL << 60));
        uint64_t chidHigh = (chid >> CHID_HIGH_OFFSET);

        mAdvHeader.chidHigh = chidHigh;
        Packet::setChannelId(chid - (chidHigh << CHID_HIGH_OFFSET));
    }

    virtual void setDataLen(int32_t len){
        Packet::setDataLen(len);
    }

    virtual int32_t getDataLen()
    {
        return Packet::getDataLen();
    }

    /* Packet priority setting .*/
    virtual CONNPRIORITY getPriority(void) { return((CONNPRIORITY)mAdvHeader.priority); }
    virtual void setPriority(CONNPRIORITY prio)
    {
        DBGASSERT( prio > ANET_PRIORITY_DEFAULT && prio <= ANET_PRIORITY_HIGH);
        mAdvHeader.priority = prio;
    }

    /* Cleanup the advance packet flags from pcode field in packet header,
     * before the packet is passed to the higher logic. */
    virtual void cleanupFlags(void)
    {
        Packet::setPcode( Packet::getPcode() & (~ADVANCE_PACKET_MASK));
    }

    virtual uint8_t getPacketVersion() {
        return mAdvHeader.packetVersion;
    }

    virtual void setPacketVersion(uint8_t version) {
        mAdvHeader.packetVersion = version;
    }
    /*************************************************************************/

    /* Packet TTL operations. */
    uint32_t getTTL(void) { return mAdvHeader.ttlInMs; }
    void setTTL(uint32_t ttl) { mAdvHeader.ttlInMs = ttl; }

    /* Packet channel id, high 32 bit. */
    uint32_t getChidHigh(void) { return mAdvHeader.chidHigh; }
    void setChidHigh(uint32_t chidHigh)
    {
        mAdvHeader.chidHigh = chidHigh;
    }

    /* Mark the packet as advance packet by updating the pcode field. */
    void markAdvancePacket()
    {
        Packet::setPcode( Packet::getPcode() | ADVANCE_PACKET_MASK );
    }
    bool isAdvancePacket()
    {
        return( AdvancePacket<T>::IsAdvancePacket(AdvancePacket<T>::getPcode()));
    }

    /* Packet flag */
    uint8_t getPacketFlags(void) { return  mAdvHeader.packetFlags; }
    void setPacketFlags(uint8_t flags)
    {
        mAdvHeader.packetFlags = flags;
    }

    /* Checksum functions. */
    uint32_t getChecksum(void) { return mAdvHeader.packetChecksum; }
    void setChecksum(uint32_t crc) { mAdvHeader.packetChecksum = crc; }
    bool isChecksumEnabled(void)
    {
        return GetChecksumMethod( mAdvHeader.packetFlags )
               !=  PACKET_FLAGS_NOCHECKSUM;
    }

    PACKET_FLAGS_CHECKSUM getChecksumMethod()
    {
	return static_cast<PACKET_FLAGS_CHECKSUM>(GetChecksumMethod( mAdvHeader.packetFlags ));
    }

    void setChecksumMethod(PACKET_FLAGS_CHECKSUM c)
    {
        mAdvHeader.packetFlags = SetChecksumMethod(c, mAdvHeader.packetFlags);
    }

    /* Debugging assistant functions. */
    virtual void dump()
    {
        Packet::dump();
        printf("====== Dumping AdvancePacket Header =======\n");
        printf("uint32_t ttlInMs        %u\n", mAdvHeader.ttlInMs);
        printf("uint32_t packetChecksum %u\n", mAdvHeader.packetChecksum);
        printf( "uint32_t chidHigh       %u\n", mAdvHeader.chidHigh);
        printf( "uint8_t  priority       %d\n", mAdvHeader.priority);
        printf( "uint8_t  packetFlags    0x%x\n", mAdvHeader.packetFlags);
        printf( "uint8_t  packetVersion  %d\n", mAdvHeader.packetVersion);
        printf( "uint8_t  nextHeaderType %d\n", mAdvHeader.nextHeaderType);
    }

private:
    AdvancePacketHeader mAdvHeader;
};

/*****************************************************************************/
/*****************************************************************************/
template<class T>
AdvancePacket<T>::AdvancePacket()
{
    mAdvHeader.ttlInMs = 0;
    mAdvHeader.packetChecksum = 0;
    mAdvHeader.flagsData = 0;
    mAdvHeader.nextHeaderType = 0;
    mAdvHeader.chidHigh = 0;
    mAdvHeader.packetVersion = 0;
    mAdvHeader.reserved = 0;
}

template<class T>
AdvancePacket<T>::~AdvancePacket()
{
}

template<class T>
bool AdvancePacket<T>::encode(DataBuffer *output)
{
    bool rc = false;
    int oldLen = output->getDataLen();
    int checksumOffset = 0;

    /* Checksum field has to reset to 0 so that we can calculate
     * and fill in the correct checksum value. */
    mAdvHeader.packetChecksum = 0;

    /* Update packet flags to use correct checksum method. */
    if (flags::getChecksumState()) {
        setChecksumMethod(PACKET_FLAGS_CHECKSUM_CRC32C);
    } else {
        setChecksumMethod(PACKET_FLAGS_NOCHECKSUM);
    }

    /* Encode advance packet default header. */
    output->writeInt32(mAdvHeader.ttlInMs);
    checksumOffset = output->getDataLen();
    output->writeInt32(mAdvHeader.packetChecksum);
    output->writeInt32(mAdvHeader.chidHigh);
    output->writeInt8(mAdvHeader.flagsData);
    output->writeInt8(mAdvHeader.packetVersion);
    output->writeInt8(mAdvHeader.nextHeaderType);
    output->writeInt8(mAdvHeader.reserved);

    /* Encode advance packet addon header. */
    if (HasAddon(mAdvHeader.nextHeaderType))
    {
        /* Encode addon header here. */
        ;
    }

    /* For advance packet, we use the 32th bit in pcode as a flag. */
    markAdvancePacket();

    /* Encode the real message body. */
    rc = T::encode(output);

    if (flags::getChecksumState()) {
        /* Now calculate checksum. */
        /* The newLen will contain advHeader and payload. */
        int newLen = output->getDataLen();
        DBGASSERT(newLen >= oldLen + (int)sizeof(mAdvHeader));

        /* Get the advHeader and payload length to calculate checksum. */
        int advPktLen = newLen - oldLen;
        uint8_t *data = (uint8_t *)(output->getData() + oldLen);
        /* Call checksum algorithm to calculate. */
        setChecksum(DoCrc32c(0, data, advPktLen));

        /* Fill in checksum */
        unsigned char *ptr = (unsigned char *)(output->getData() + checksumOffset);
        output->fillInt32(ptr, getChecksum());
   }

    return rc;
}

template<class T>
bool AdvancePacket<T>::decode(DataBuffer *input, PacketHeader *header)
{
    /* Tricky part. The assumption is that the advance packet header
     * is encoded as big endian numbers. */
    uint32_t * p = (uint32_t *)(input->getData() + offsetof(AdvancePacketHeader,
                                                            packetChecksum));
    uint8_t * pFlags = (uint8_t *) (input->getData() +
                       offsetof(AdvancePacketHeader, flagsData));

    /* Store the checksum value and pkt flags comes from the packet so that
     * we can verify it later. */
    uint32_t pktCRCBigEndian = *p;
    /* low 3 bits are for priority */
    uint8_t pktFlags = *pFlags >> 3;

    if (flags::getChecksumState() && GetChecksumMethod(pktFlags)
            != PACKET_FLAGS_NOCHECKSUM && header != NULL)
    {
        /*calculate packet checksum to verify the correctness of the packet.*/
        /*We need to reset the checksum field to 0 in DataBuffer since that
         * is the way when we do the calculation while sending out packet. */
        *p = 0;
        uint8_t *data = (uint8_t *)(input->getData());

        uint32_t checkSum = 0;
        if (GetChecksumMethod(pktFlags) == PACKET_FLAGS_CHECKSUM_CRC32C) {
            checkSum = DoCrc32c(0, data, header->_dataLen);
        }
        else {
            ANET_LOG(ERROR, "Unknown checksum method flags %d for message chid %d",
                             pktFlags, header->_chid);
        }

        /* Verify the checksum is ok .*/
        uint32_t pktCRC = ntohl(pktCRCBigEndian);
        if (pktCRC != checkSum)
        {
            ANET_LOG(ERROR, "Drop pkt. Checksum verification error for pkt, pkt header crc %u, "
                     "actual crc %u, dataLen %d, chid %d, pkgFlags %d", pktCRC, checkSum,
                      header->_dataLen, header->_chid, pktFlags);

            /* need to throw away all the trash data before returns. */
            input->drainData(header->_dataLen);
            return false;
        }

        /* Restore the checksum to its original value. */
        *p = pktCRCBigEndian;
    }

    mAdvHeader.ttlInMs = input->readInt32();
    mAdvHeader.packetChecksum  =  input->readInt32();
    mAdvHeader.chidHigh =  input->readInt32();
    mAdvHeader.flagsData = input->readInt8();
    mAdvHeader.packetVersion = input->readInt8();
    mAdvHeader.nextHeaderType = input->readInt8();
    mAdvHeader.reserved  = input->readInt8();

    /* Decode addon headers */
    if (HasAddon(mAdvHeader.nextHeaderType))
    {
        /* Decode addon header here. */
        ;
    }

    /* Reset the packet length since we already decoded the advance header
     * above. So the header size should be substracted. */
    setDataLen( getDataLen() -  sizeof(AdvancePacketHeader) );

    return T::decode(input, header);
}

END_ANET_NS();
#endif
