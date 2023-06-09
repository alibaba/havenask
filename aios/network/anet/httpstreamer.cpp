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
 * File name: httpstreamer.cpp
 * Author: zhangli
 * Create time: 2008-12-12 20:02:45
 * $Id: httpstreamer.cpp 15760 2008-12-30 08:03:52Z zhangli $
 * 
 * Description: ***add description here***
 * 
 */

#include "aios/network/anet/httpstreamer.h"
#include "aios/network/anet/httpstreamingcontext.h"
#include "aios/network/anet/databuffer.h"
#include "aios/network/anet/log.h"
#include "aios/network/anet/aneterror.h"
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "aios/network/anet/defaultpacketstreamer.h"
#include "aios/network/anet/httppacket.h"
#include "aios/network/anet/ilogger.h"
#include "aios/network/anet/ipacketfactory.h"
#include "aios/network/anet/packet.h"
#include "aios/network/anet/streamingcontext.h"

namespace anet {

const size_t HTTPStreamer::URI_LIMIT = 1024 * 64;
const size_t HTTPStreamer::PKG_LIMIT = 67108864;
const size_t HTTPStreamer::HEADERS_LIMIT = 128;
const char HTTPStreamer::_table[256] = {
    0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
    0,1,0,1,1,1,1,1,0,0,1,1,0,1,1,0,1,1,1,1,1,1,1,1,1,1,0,0,0,0,0,0,
    0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,0,0,1,1,
    1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,1,0,1,0,
    0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
    0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
    0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
    0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
};

HTTPStreamer::HTTPStreamer(IPacketFactory *factory) 
  : DefaultPacketStreamer(factory) 
{
    _existPacketHeader = false;
    _pkgLimit = 0;
}

bool HTTPStreamer::processData(DataBuffer *dataBuffer, 
                               StreamingContext *context) 
{
    ANET_LOG(SPAM,"Begin process Data");
    
    HTTPStreamingContext *httpContext;
    httpContext = dynamic_cast<HTTPStreamingContext*>(context);
    assert(httpContext);
    switch (httpContext->_step) {
    case HTTPStreamingContext::HSS_START_LINE:
        ANET_LOG(SPAM,"HTTPStreamingContext::HSS_START_LINE");
        if (processStartLine(dataBuffer, httpContext)) {
            ANET_LOG(SPAM, "processStartLine finished!");
            httpContext->_step = HTTPStreamingContext::HSS_MESSAGE_HEADER;
        } else {
            ANET_LOG(SPAM, "processStartLine() not finished.");
            break;
        }
    case HTTPStreamingContext::HSS_MESSAGE_HEADER:
        ANET_LOG(SPAM,"HTTPStreamingContext::HSS_MESSAGE_HEADER");
        if (processHeaders(dataBuffer, httpContext)) {
            ANET_LOG(SPAM, "processHeaders finished!");
            httpContext->_step = HTTPStreamingContext::HSS_MESSAGE_BODY;
        } else {
            ANET_LOG(SPAM, "processHeaders() not finished.");
            break;
        }
    case HTTPStreamingContext::HSS_MESSAGE_BODY:
        ANET_LOG(SPAM,"HTTPStreamingContext::HSS_MESSAGE_BODY");
        if (processBody(dataBuffer, httpContext)) {
            ANET_LOG(SPAM, "processBody finished!");
            httpContext->setCompleted(true);
        }
        break;
    default:
        ANET_LOG(WARN,"SHOULD NOT come here: DEFAULT");
        break;
    }

    if (httpContext->isEndOfFile()) {
        HTTPPacket *packet = dynamic_cast<HTTPPacket*>(httpContext->getPacket());
        if ((packet && HTTPPacket::PT_REQUEST == packet->getPacketType())
            || (!httpContext->isCompleted() && !httpContext->isBroken())) 
        {
            ANET_LOG(SPAM, "set broken packet(%p)", packet);
            //error !!! when broken
            context->setErrorNo(AnetError::CONNECTION_CLOSED);
        }
    }    
    return httpContext->isBroken() != true;
}

StreamingContext* HTTPStreamer::createContext() {
    return new HTTPStreamingContext;
}

bool HTTPStreamer::processStartLine(DataBuffer *databuffer, 
                                    HTTPStreamingContext *context) 
{
    char *pdata = databuffer->getData();
    char *pend = pdata + databuffer->getDataLen();
    char *cr = NULL;
    size_t length = 0;
    if (!findCRLF(pdata, pend, cr,  length)) {
        if ((size_t)databuffer->getDataLen() > getPkgLimit()) {
            context->setErrorNo(AnetError::PKG_TOO_LARGE);
        } else {
            ANET_LOG(SPAM, "start line not completed");
        }
        return false;
    }
    *cr = '\0';
    if (length > getPkgLimit()) {
        context->setErrorNo(AnetError::PKG_TOO_LARGE);
        return false;
    }

    assert(!context->getPacket());
    HTTPPacket *packet = dynamic_cast<HTTPPacket*>(_factory->createPacket(0));
    assert(packet);
    context->setPacket(packet);

    bool rc = false;
    if (strncmp(pdata, "HTTP/", 5) == 0) {
        rc =  processStatusLine(context, pdata, cr);
    } else {
        rc =  processRequestLine(context, pdata, cr);
    }
    databuffer->drainData(length);
    context->_drainedLength += length;
    return rc;
}

bool HTTPStreamer::processRequestLine(HTTPStreamingContext *context, 
                                      char *start, char *end) {
    char *p  = findNextWhiteSpace(start, end);
    if (p == end) {
        context->setErrorNo(AnetError::INVALID_DATA);
        ANET_LOG(WARN ,"Broken stream! Only one token in start line!");
        return false;
    }
    *p = '\0';
    HTTPPacket *packet = dynamic_cast<HTTPPacket*>(context->getPacket());
    assert(packet);
    if (!isValidToken(start, p)) {
        context->setErrorNo(AnetError::INVALID_DATA);
        ANET_LOG(WARN, "Broken stream! invalid method!");
        return false;
    }
    packet->setPacketType(HTTPPacket::PT_REQUEST);
    packet->setMethod(start);
    start = skipWhiteSpaces(p + 1, end);
    if ( start == end) {
        context->setErrorNo(AnetError::INVALID_DATA);
        ANET_LOG(WARN, "Broken stream! Only one token in request line!");
        return false;
    }
        
    //now processing URI
    p = findNextWhiteSpaceAndReplaceZero(start, end);
    if (p == end) {
        context->setErrorNo(AnetError::INVALID_DATA);
        ANET_LOG(WARN, "Broken stream! Only two tokens in request line!");
        return false;
    }
    *p = '\0';
    if ((size_t)(p - start) > URI_LIMIT) {
        context->setErrorNo(AnetError::URI_TOO_LARGE);
        return false;
    }
    packet->setURI(start);
        
    start = skipWhiteSpaces(p + 1, end);
    if (strcmp(start, "HTTP/1.0") == 0 ) {
        packet->setVersion(HTTPPacket::HTTP_1_0);
    } else if (strcmp(start, "HTTP/1.1") == 0) {
        packet->setVersion(HTTPPacket::HTTP_1_1);
    } else {
        packet->setVersion(HTTPPacket::HTTP_UNSUPPORTED);
        context->setErrorNo(AnetError::VERSION_NOT_SUPPORT);
        ANET_LOG(WARN, "Broken stream? Unsupported HTTP version!");
        return false;
    }
    return true;
}

bool HTTPStreamer::processStatusLine(HTTPStreamingContext *context, 
                                      char *start, char *end) 
{
    if (end - start < 128) {
        ANET_LOG(SPAM, "Status Line: |%s|", start);
    }
    char *p  = findNextWhiteSpace(start, end);
    if (p == end) {
        context->setErrorNo(AnetError::INVALID_DATA);
        ANET_LOG(WARN ,"Broken stream! Only one token in start line!");
        return false;
    }
    *p = '\0';
    HTTPPacket *packet = dynamic_cast<HTTPPacket*>(context->getPacket());
    assert(packet);

    packet->setPacketType(HTTPPacket::PT_RESPONSE);
    if (strcmp(start, "HTTP/1.0") == 0 ) {
        packet->setVersion(HTTPPacket::HTTP_1_0);
    } else if (strcmp(start, "HTTP/1.1") == 0) {
        packet->setVersion(HTTPPacket::HTTP_1_1);
    } else {
        packet->setVersion(HTTPPacket::HTTP_UNSUPPORTED);
        context->setErrorNo(AnetError::VERSION_NOT_SUPPORT);
        ANET_LOG(WARN, "Broken stream? Unsupported HTTP version!");
        return false;
    }
    start = skipWhiteSpaces(p + 1, end);
    if ( start == end) {
        context->setErrorNo(AnetError::INVALID_DATA);
        ANET_LOG(WARN, "Broken stream! Only one token in status line!");
        return false;
    }

    //now processing status-code
    p = findNextWhiteSpace(start, end);
//     if (p == end) {
//         context->setErrorNo(AnetError::INVALID_DATA);
//         ANET_LOG(WARN, "Broken stream! Only two tokens in statusa line!");
//         return false;
//     }
    if (p != end) {
        *p = '\0';
    }
    if (!isValidStatusCode(start)) {
        context->setErrorNo(AnetError::INVALID_DATA);
        ANET_LOG(WARN, "Broken stream! invalid status code|%s|!", start);
        return false;
    }
    int statusCode = atoi(start);
    packet->setStatusCode(statusCode);
    ANET_LOG(SPAM,"status code(%d),start(%p),end(%p)", statusCode, p, end);
    if (p == end) {
        return true;
    }
    start = skipWhiteSpaces(p + 1, end);
    if (start != end) {
        packet->setReasonPhrase(start);
    }
    return true;
}

/**
 * @ret: true: parser completed! false: parser not complete;
 */
bool HTTPStreamer::processHeaders(DataBuffer *databuffer, 
                                  HTTPStreamingContext *context) {
    if (processHeadersOrTrailers(databuffer, context)) {
        return messageHeadersEnd(context);
    }
    return false;
}

bool HTTPStreamer::processBody(DataBuffer *databuffer,
                               HTTPStreamingContext *context) 
{
    switch (context->_encodingType) {
    case HTTPStreamingContext::HET_NO_BODY:
        return true;
    case HTTPStreamingContext::HET_LENGTH:
        return processLengthBody(databuffer, context);
    case HTTPStreamingContext::HET_CHUNKED:
        return processChunkedBody(databuffer, context);
    case HTTPStreamingContext::HET_EOF:
        return processEOFBody(databuffer, context);
    default:
        return false;
    }
}

bool HTTPStreamer::processLengthBody(DataBuffer *databuffer,
                                     HTTPStreamingContext *context) 
{
    HTTPPacket *packet = dynamic_cast<HTTPPacket*>(context->getPacket());
    assert(packet);
    size_t bufferLength = databuffer->getDataLen();
    if (bufferLength >= context->_dataLength) {//we have enough data
        packet->appendBody(databuffer->getData(), context->_dataLength);
        databuffer->drainData(context->_dataLength);
        context->_drainedLength += context->_dataLength;
        return true;
    } else {
        packet->appendBody(databuffer->getData(), bufferLength);
        databuffer->clear();
        context->_dataLength -= bufferLength;
        context->_drainedLength += bufferLength;
        return false;
    }
}

bool HTTPStreamer::processEOFBody(DataBuffer *databuffer, 
                                  HTTPStreamingContext *context) 
{
    HTTPPacket *packet = dynamic_cast<HTTPPacket*>(context->getPacket());
    assert(packet);
    packet->appendBody(databuffer->getData(), databuffer->getDataLen());
    databuffer->clear();
    return context->isEndOfFile();
}

bool HTTPStreamer::processChunkedBody(DataBuffer *databuffer, 
                                      HTTPStreamingContext *context) 
{
    do {
        switch (context->_chunkState) {
        case HTTPStreamingContext::CHUNK_SIZE:
            ANET_LOG(SPAM, "Process Chunk Size");
            if (!processChunkSize(databuffer, context)) {
                return false;
            } 
            //use switch() to determinate whether this is the lastChunk;
            break;
        case HTTPStreamingContext::CHUNK_DATA:
            ANET_LOG(SPAM, "Process Chunk Data");
            if (!processLengthBody(databuffer,context)) {
                ANET_LOG(SPAM, "Process Chunk Data NOT finished");
                return false;
            }
            context->_chunkState = HTTPStreamingContext::CHUNK_DATA_CRLF;
            ANET_LOG(SPAM, "Process Chunk Data FINISHED");
            break;
        case HTTPStreamingContext::CHUNK_DATA_CRLF:
            ANET_LOG(SPAM, "Process CRLF");
            if (!getCRLF(databuffer, context)) {
                ANET_LOG(SPAM, "Process CRLF not finished");
                return false;
            }
            ANET_LOG(SPAM, "Process CRLF FINISHED");
            context->_chunkState = HTTPStreamingContext::CHUNK_SIZE;
            break;
        case HTTPStreamingContext::TRAILER:
            ANET_LOG(SPAM, "Process TRAILER");
            return processHeadersOrTrailers(databuffer, context);
        default:
            ANET_LOG(ERROR, "We SHOULD NOT get here!");
            return false;
        }
    } while(true);
}

bool HTTPStreamer::processChunkSize(DataBuffer *databuffer,
                                    HTTPStreamingContext *context) 
{
    char *pstart = databuffer->getData();
    char *pend = pstart + databuffer->getDataLen();
    char *cr = NULL;
    size_t drainLength = 0;
    if (!findCRLF(pstart, pend, cr, drainLength)) {
        if (databuffer->getDataLen() + context->_drainedLength > getPkgLimit()) {
            context->setErrorNo(AnetError::PKG_TOO_LARGE);
        } else {
            ANET_LOG(SPAM, "chunk size not completed");
        }
        return false;
    }
    *cr = '\0';
    ANET_LOG(SPAM, "Chunk Size Line:|%s|", pstart);
    if (context->_drainedLength + drainLength > getPkgLimit()) {
        context->setErrorNo(AnetError::PKG_TOO_LARGE);
        return false;
    }
    
    pstart = skipWhiteSpaces(pstart, cr);
    int chunkSize = -1;
    char c = 0;
    sscanf(pstart, "%x%c", &chunkSize, &c); 
    if(chunkSize < 0 || (0 != c && ';' != c)) {
      context->setErrorNo(AnetError::INVALID_DATA);
      ANET_LOG(WARN,"Invalid data in chunk size, %d,%c", chunkSize, c);
      return false;
    }

    context->_dataLength = chunkSize;
    databuffer->drainData(drainLength);
    context->_drainedLength += drainLength;
    ANET_LOG(SPAM, "Chunk size:%lu", context->_dataLength);
    if (context->_dataLength == 0) {
        ANET_LOG(SPAM, "Last Chunk Found!");
        context->_chunkState = HTTPStreamingContext::TRAILER;        
        return true;
    }

    if (context->_drainedLength + context->_dataLength > getPkgLimit()) {
        context->setErrorNo(AnetError::PKG_TOO_LARGE);
        return false;
    }
    ANET_LOG(SPAM, "Chunk Size processing finished!");
    context->_chunkState = HTTPStreamingContext::CHUNK_DATA;
    return true;
}

bool HTTPStreamer::getCRLF(DataBuffer *databuffer,
                           HTTPStreamingContext *context) 
{
    char *pdata = databuffer->getData();
    size_t length = databuffer->getDataLen();
    if (length < 1) {
        return false;
    }
    
    //ANET_LOG(SPAM, "length(%lu)|%c|%c|", length, *pdata, *(pdata+1));

    if (length == 1) {
        if ('\n' == *pdata) {
        databuffer->drainData(1);
        context->_drainedLength += 1;
        return true;
        } else if ('\r' != *pdata) {
            context->setErrorNo(AnetError::INVALID_DATA);
            return false;
        } else { // *pdata == '\r'
            databuffer->clear();
            context->_drainedLength += 1;
            return false;
        }
    } 
    
    if ('\n' == *pdata) {
        databuffer->drainData(1);
        context->_drainedLength += 1;
        return true;        
    }

    if ('\r' == *pdata && '\n' == *(pdata +1)) {
        databuffer->drainData(2);
        context->_drainedLength += 2;
        return true;        
    }        
    context->setErrorNo(AnetError::INVALID_DATA);
    return false;
}

bool HTTPStreamer::processHeadersOrTrailers(DataBuffer *databuffer, 
        HTTPStreamingContext *context) {
    HTTPPacket *packet = dynamic_cast<HTTPPacket*>(context->getPacket());
    assert(packet);
    while (1) {
        char *pstart = databuffer->getData();
        char *pend = pstart + databuffer->getDataLen();
        char *cr = NULL;
        size_t drainLength = 0;
        if (!findCRLF(pstart, pend, cr, drainLength)) {
            if (databuffer->getDataLen() + context->_drainedLength > getPkgLimit()) {
                context->setErrorNo(AnetError::PKG_TOO_LARGE);
            } else {
                ANET_LOG(SPAM, "header not completed");
            }
            return false;
        }
        if (context->_drainedLength + drainLength > getPkgLimit()) {
            context->setErrorNo(AnetError::PKG_TOO_LARGE);
            return false;
        }
        ANET_LOG(SPAM, "a header line completed");
        *cr = '\0';
        if (cr == pstart) { //a empty line
            databuffer->drainData(drainLength);
            context->_drainedLength += drainLength;
            return true;
        }
        processPerHeaderLine(pstart, cr, packet);
        databuffer->drainData(drainLength);
        context->_headersCount ++;
        if(context->_headersCount > HTTPStreamer::HEADERS_LIMIT) {
            context->setErrorNo(AnetError::TOO_MANY_HEADERS);
        }
        context->_drainedLength += drainLength;
    }
    return false;
}

bool HTTPStreamer::messageHeadersEnd(HTTPStreamingContext *context) {
    ANET_LOG(SPAM, "Message-Headers END");
    HTTPPacket *packet = dynamic_cast<HTTPPacket*>(context->getPacket());
    assert(packet);
    const char *encodingType = packet->getHeader("Transfer-Encoding");
    if (NULL == encodingType) {
        const char *contentLength = packet->getHeader("Content-Length");
        if (NULL == contentLength) {
            if (HTTPPacket::PT_RESPONSE  == packet->getPacketType()) {
                ANET_LOG(SPAM, "Set HTTPStreamingContext::HET_EOF");
                context->_encodingType = HTTPStreamingContext::HET_EOF;
            } else { //fix ticket #146
                ANET_LOG(SPAM, "Set HTTPStreamingContext::HET_NO_BODY");
                context->_encodingType = HTTPStreamingContext::HET_NO_BODY;
            }
            return true;
        } else {
            if (!isValidDigits(contentLength) ) {
                ANET_LOG(ERROR, "error content length! %s", contentLength);
                context->setErrorNo(AnetError::INVALID_DATA);
                return false;
            }
            int  length = atoi(contentLength);
            if (length > 0) {
                ANET_LOG(SPAM, "Set HTTPStreamingContext::HET_LENGTH");
                context->_encodingType = HTTPStreamingContext::HET_LENGTH;
                context->_dataLength = length;
                if (length + context->_drainedLength > getPkgLimit()) {
                    context->setErrorNo(AnetError::PKG_TOO_LARGE);
                    return false;
                }
            } else {
                ANET_LOG(SPAM, "Set HTTPStreamingContext::HET_NO_BODY");
                context->_encodingType = HTTPStreamingContext::HET_NO_BODY;
            }            
            return true;
        }
    } else {
        if (strcmp("chunked", encodingType) == 0) {
            context->_encodingType = HTTPStreamingContext::HET_CHUNKED;
            return true;
        } else {
            ANET_LOG(ERROR, "error transfer encoding type!");
            context->setErrorNo(AnetError::INVALID_DATA);
            return false;
        }
    }

}

bool HTTPStreamer::findCRLF(char *start, char *end, char *&CR,  size_t &length) {
    length = 0;
    while (start < end) {
        length ++;
        if ('\n' == *start) {
            break;
        }
        start ++;
    }
    if (start == end) {
        return false;
    }

    if (length > 1 && '\r' == *(start - 1)) {
        CR = start -1;
    } else {
        CR = start;
    }
    return true;
}

/**
 * process per HeaderLine
 * will not drain data in the databuffer
 * @ret: true: not broken; false: broken
 */
bool HTTPStreamer::processPerHeaderLine(char *pstart, char *pend, 
                                        HTTPPacket *packet) 
{
    char *key = pstart;
    char *value = NULL;
    while (pstart < pend) {
        if (':' == *pstart) {
            break;
        }
        pstart ++;
    }
    if (pstart >= pend) {
        ANET_LOG(WARN, "lack \":\" in header");
        return false;
    }
    *pstart = 0;
    pstart ++;
    trim(pstart, pend, true, true);
    replaceZero(pstart, pend);
    value = pstart;
    *pend = 0;
    packet->addHeader(key, value);
    
    return true;
}

void HTTPStreamer::replaceZero(char *begin, char *end) {
    while (begin < end) {
        if (0 == *begin) {
            *begin = ' ';
        }
        begin ++;
    }
}

void HTTPStreamer::trim(char *&begin, char *&end, bool left, bool right) {
    if (left) {
        while (begin < end && (' ' == *begin || '\t' == *begin) ) {
            begin ++;
        }
    }
    if (right) {
        while (begin < end && (' ' == *(end-1) || '\t' == *(end-1)) ) {
            end --;
        }
    }
}

char* HTTPStreamer::findNextWhiteSpace(char *begin, char *end) {
    while (begin < end) {
        if ( ' ' == *begin || '\t' == *begin ) {
            break;
        }
        begin ++;
    }
    return begin;
}

char* HTTPStreamer::findNextWhiteSpaceAndReplaceZero(char *begin, char *end ) {
    while (begin < end) {
        if ( ' ' == *begin || '\t' == *begin ) {
            break;
        }
        if ('\0' == *begin) {
            *begin = ' ';
        }
        begin ++;
    }
    return begin;
}

char* HTTPStreamer::skipWhiteSpaces(char *begin, char *end ) {
    while (begin < end && (' ' == *begin || '\t' == *begin )) {
        begin ++;
    }
    return begin;
}

bool HTTPStreamer::isValidStatusCode(char *p) {
    assert(p);
    if (strlen(p) != 3) {
        return false;
    }
    return  isdigit(p[0]) && isdigit(p[1]) &&isdigit(p[2]);
}

bool HTTPStreamer::isValidToken(char *begin, char *end) {
    while  (begin < end) {
        if (!isTokenCharacter(*begin)) {
            return false;
        }
        begin ++;
    }
    return true;
}

bool HTTPStreamer::isValidDigits(const char *dist) {
    for (size_t i = 0; i < strlen(dist); i ++) {
        if (!isdigit(dist[i])) {
            return false;
        }
    }
    return true;
}

void HTTPStreamer::setPkgLimit(size_t size){
    _pkgLimit = size;
}

size_t HTTPStreamer::getPkgLimit() const{
    if (_pkgLimit == 0) {
        return PKG_LIMIT;
    }
    return _pkgLimit;
}

}/*end namespace anet*/
