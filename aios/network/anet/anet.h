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
#ifndef ANET_ANET_H
#define ANET_ANET_H
#include "aios/network/anet/advancepacket.h"
#include "aios/network/anet/advancepacketfactory.h"
#include "aios/network/anet/connection.h"
#include "aios/network/anet/connectionpriority.h"
#include "aios/network/anet/controlpacket.h"
#include "aios/network/anet/databuffer.h"
#include "aios/network/anet/databufferserializable.h"
#include "aios/network/anet/defaultpacket.h"
#include "aios/network/anet/defaultpacketfactory.h"
#include "aios/network/anet/defaultpacketstreamer.h"
#include "aios/network/anet/delaydecodepacket.h"
#include "aios/network/anet/delaydecodepacketfactory.h"
#include "aios/network/anet/httppacket.h"
#include "aios/network/anet/httppacketfactory.h"
#include "aios/network/anet/httpstreamer.h"
#include "aios/network/anet/iocomponent.h"
#include "aios/network/anet/ipacketfactory.h"
#include "aios/network/anet/ipackethandler.h"
#include "aios/network/anet/ipacketstreamer.h"
#include "aios/network/anet/iserveradapter.h"
#include "aios/network/anet/packet.h"
#include "aios/network/anet/streamingcontext.h"
#include "aios/network/anet/timeutil.h"
#include "aios/network/anet/transport.h"

// DO NOT export interfaces about logging implicitly
//#include "aios/network/anet/log.h"

/**legacy http related header files*/
#include "aios/network/anet/httppacketstreamer.h"
#include "aios/network/anet/httprequestpacket.h"
#include "aios/network/anet/httpresponsepacket.h"

// for direct packet
#include "aios/network/anet/directpacket.h"
#include "aios/network/anet/directpacketstreamer.h"
#include "aios/network/anet/directstreamingcontext.h"
#include "aios/network/anet/directtcpconnection.h"

#define BEGIN_ANET_NS() namespace anet {
#define END_ANET_NS() }
#define USE_ANET_NS() using namespace anet;

BEGIN_ANET_NS()

/* Redefine advance packet type to make it easier to use. */
typedef AdvancePacket<DelayDecodePacket> AdvanceDelayDecodePacket;
typedef AdvancePacket<DefaultPacket> AdvanceDefaultPacket;
typedef AdvancePacketFactory<DelayDecodePacket> AdvanceDelayDecodePacketFactory;
typedef AdvancePacketFactory<DefaultPacket> AdvanceDefaultPacketFactory;

END_ANET_NS()

#define ANET_VERSION 040000
#endif /*End of ANET_ANET_H*/
