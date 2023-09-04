#ifndef ADVANCE_TEST_PACKET
#define ADVANCE_TEST_PACKET
#include <stdint.h>
#include <stdlib.h>

#include "aios/network/anet/advancepacket.h"
#include "aios/network/anet/anet.h"
#include "aios/network/anet/databuffer.h"
#include "aios/network/anet/debug.h"
#include "aios/network/anet/ilogger.h"
#include "aios/network/anet/log.h"
#include "aios/network/anet/packet.h"
#include "aios/network/anet/util.h"

namespace anet {

class DataBuffer;

template <class T>
class AdvanceTestPacket : public AdvancePacket<T> {
public:
    AdvanceTestPacket() {
        randomEncodeError = randomDecodeError = false;
        nBits = 0;
    }
    void injectEncodeError(bool enabled, int numOfBits = 1) {
        randomEncodeError = enabled;
        nBits = numOfBits;
    }
    void injectDecodeError(bool enabled, int numOfBits = 1) {
        randomDecodeError = enabled;
        nBits = numOfBits;
    }

    virtual bool encode(DataBuffer *output);
    virtual bool decode(DataBuffer *input, PacketHeader *header);

private:
    bool randomEncodeError;
    bool randomDecodeError;
    /* how many bits of corruptions. */
    int nBits;
};

template <class T>
bool AdvanceTestPacket<T>::encode(DataBuffer *output) {
    int initPos = output->getDataLen();
    bool rc = AdvancePacket<T>::encode(output);
    int finalPos = output->getDataLen();

    if (rc && randomEncodeError) {
        DBGASSERT(finalPos - initPos >= (int)sizeof(AdvancePacketHeader));
        int dataLen = finalPos - initPos - sizeof(AdvancePacketHeader);
        /* it is possible that the random bit set randomly and disable checksum flag.
         * so we should avoid change flags bit. */
        int offset = getRandInt(3) * 4;
        if (dataLen > 0)
            offset = sizeof(AdvancePacketHeader) + getRandInt(dataLen) - 4;
        unsigned char *p = (unsigned char *)(output->getData()) + initPos + offset;
        uint32_t *p32 = (uint32_t *)p;
        int bits = nBits;
        uint32_t v = *p32;
        while (bits > 0 || v == *p32) {
            int bitOffset = getRandInt(32);
            v = revertBit(v, bitOffset);
            bits--;
        }
        ANET_LOG(
            INFO, "Going to update %ld offset of packet data from %d to %d", (char *)p - output->getData(), *p32, v);
        output->fillInt32(p, v);
    }
    return rc;
}

template <class T>
bool AdvanceTestPacket<T>::decode(DataBuffer *input, PacketHeader *header) {
    if (randomDecodeError) {
        int dataLen = header->_dataLen - sizeof(AdvancePacketHeader);
        int offset = getRandInt(4) * 4;
        int bits = nBits;
        if (dataLen > 0)
            offset = sizeof(AdvancePacketHeader) + getRandInt(dataLen) - 4;
        uint32_t v = *((uint32_t *)(input->getData() + offset));
        while (bits > 0 || v == *((uint32_t *)(input->getData() + offset))) {
            int bitOffset = getRandInt(32);
            v = revertBit(v, bitOffset);
            bits--;
        }

        *((uint32_t *)(input->getData() + offset)) = v;
    }

    bool rc = AdvancePacket<T>::decode(input, header);
    return rc;
}

}; // namespace anet
#endif
