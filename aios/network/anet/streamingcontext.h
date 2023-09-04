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
#ifndef ANET_STREAMINGCONTEXT_H_
#define ANET_STREAMINGCONTEXT_H_
namespace anet {
class Packet;

class StreamingContext {
public:
    StreamingContext();
    virtual ~StreamingContext();

    virtual bool isCompleted();
    void setCompleted(bool compleled);
    virtual bool isBroken();
    void setBroken(bool broken);
    void setEndOfFile(bool eof);
    bool isEndOfFile();
    Packet *getPacket();

    virtual void setErrorNo(int errorNo);
    int getErrorNo();
    void setErrorString(const char *errorString);
    const char *getErrorString();

    /**
     * get the packet from context, then set _packet to NULL.
     **/
    virtual Packet *stealPacket();
    void setPacket(Packet *packet);
    virtual void reset();

protected:
    Packet *_packet;
    bool _completed;
    bool _broken;
    bool _eof;
    int _errorNo;
    const char *_errorString;
};

} /*end namespace anet*/
#endif /*ANET_STREAMINGCONTEXT_H_*/
