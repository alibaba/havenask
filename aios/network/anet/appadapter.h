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
#ifndef APPADAPTER_H
#define APPADAPTER_H

#include "aios/network/anet/iserveradapter.h"
#include <string>

namespace anet
{
class IConnection;
class IPacketStreamer;
class Transport;

class Acceptor
{
public:
    Acceptor(){};
    virtual ~Acceptor(){}

   /*
    * @note for anet, could just call transport function listen.
    *
    * @note for asio , this function should be implemented as following code:
    * @par Example
    * @code
    * void accept_handler(const asio::error_code& error)
    * {
    *   if (!error)
    *   {
    *     // Accept succeeded.
    *   }
    * }
    *
    * ...
    *
    * asio::ip::tcp::acceptor acceptor(io_service);
    * ...
    * asio::ip::tcp::socket socket(io_service);
    * acceptor.async_accept(socket, accept_handler);
    * @endcode
    */
    virtual void HandleAccept() = 0;
};

class Connector
{
public:
    Connector(){};
    virtual ~Connector(){}

    /*
     * NO handle the destruction of object connection.
     *
     * @note for anet, could just call transport function connect.
     *
     * @note for asio , should create connection object.
     */
    virtual IConnection* Connect() = 0;
};

class AnetAcceptor : public Acceptor, IServerAdapter
{
public:
    AnetAcceptor(const std::string& address,
        Transport* transport, IPacketStreamer* streamer,
        int postPacketTimeout, int maxIdleTime);

    /* override */ void HandleAccept();

private:
    std::string mAddress;
    Transport* mTransport;
    IPacketStreamer* mStreamer;
    int mPostPacketTimeout;
    int mMaxIdleTime;
};

class AnetConnector : public Connector
{
public:
    AnetConnector(const std::string& address, Transport* transport,
        IPacketStreamer* streamer, bool autoReconn);

    /* override */ IConnection* Connect();

private:
    std::string mAddress;
    Transport* mTransport;
    IPacketStreamer* mStreamer;
    bool mAutoReconn;
};

} // namespace anet


#endif /* APPADAPTER_H */
