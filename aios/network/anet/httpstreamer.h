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
/**
 * File name: httpstreamer.h
 * Author: zhangli
 * Create time: 2008-12-12 20:02:45
 * $Id: httpstreamer.h 15760 2008-12-30 08:03:52Z zhangli $
 *
 * Description: ***add description here***
 *
 */

#ifndef ANET_HTTPSTREAMER_H_
#define ANET_HTTPSTREAMER_H_
#include <assert.h>
#include <stddef.h>

#include "aios/network/anet/defaultpacketstreamer.h"

namespace anet {
class DataBuffer;
class HTTPPacket;
class HTTPStreamingContext;
class IPacketFactory;
class Packet;
class PacketHeader;
class StreamingContext;

class HTTPStreamer : public DefaultPacketStreamer {
    friend class HTTPStreamerTest_testFindCRLF_Test;
    friend class HTTPStreamerTest_testIsTokenCharacter_Test;

public:
    HTTPStreamer(IPacketFactory *factory);
    bool processData(DataBuffer *dataBuffer, StreamingContext *context);
    StreamingContext *createContext();

    bool getPacketInfo(DataBuffer *input, PacketHeader *header, bool *broken) { return false; }

    void setPkgLimit(size_t size);

    Packet *decode(DataBuffer *input, PacketHeader *header) {
        //        ANET_LOG(ERROR, "SHOULD NOT INVOKE HTTPStreamer::decode(...)!");
        assert(false);
        return NULL;
    }

    static const size_t URI_LIMIT;
    static const size_t PKG_LIMIT;
    static const size_t HEADERS_LIMIT;

protected:
    static const char _table[256];

private:
    /**
     * return ture if start-line parsing completed.
     * set context broken if needed
     **/
    bool processStartLine(DataBuffer *databuffer, HTTPStreamingContext *context);

    /**
     * return ture if all headers parsing completed.
     * set context broken if needed
     **/
    bool processHeaders(DataBuffer *databuffer, HTTPStreamingContext *context);

    bool processHeadersOrTrailers(DataBuffer *databuffer, HTTPStreamingContext *context);

    bool processPerHeaderLine(char *pstart, char *pend, HTTPPacket *packet);

    bool processBody(DataBuffer *databuffer, HTTPStreamingContext *context);

    bool processLengthBody(DataBuffer *databuffer, HTTPStreamingContext *context);

    bool processChunkedBody(DataBuffer *databuffer, HTTPStreamingContext *context);

    bool processEOFBody(DataBuffer *databuffer, HTTPStreamingContext *context);

    bool processChunkSize(DataBuffer *databuffer, HTTPStreamingContext *context);
    bool getCRLF(DataBuffer *databuffer, HTTPStreamingContext *context);

    char *findNextWhiteSpace(char *begin, char *end);
    char *findNextWhiteSpaceAndReplaceZero(char *begin, char *end);
    char *skipWhiteSpaces(char *begin, char *end);
    bool isValidStatusCode(char *p);
    bool isValidToken(char *begin, char *end);
    bool isValidDigits(const char *dist);
    void replaceZero(char *begin, char *end);
    void trim(char *&begin, char *&end, bool left, bool right);
    bool findCRLF(char *start, char *end, char *&CR, size_t &length);
    bool messageHeadersEnd(HTTPStreamingContext *context);
    bool processStatusLine(HTTPStreamingContext *context, char *start, char *end);
    bool processRequestLine(HTTPStreamingContext *context, char *start, char *end);
    size_t getPkgLimit() const;
    inline bool isTokenCharacter(unsigned char c) { return _table[c]; }

protected:
    size_t _pkgLimit;
};

} /*end namespace anet*/

#endif /*ANET_HTTPSTREAMER_H_*/
