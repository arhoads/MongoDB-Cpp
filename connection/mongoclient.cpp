/*!
 * \file mongoclient.cpp
 * \author Nathan Eloe
 * \brief Implementation of the basic, naive connection to the database
 */

#include "mongoclient.h"
#include "cursor.h"
#include <algorithm>
#include <memory>
#include <string>
#include <sstream>
#include "../zmq/socket.h"
#include "../zmq/messages/message.h"

namespace mongo
{
  int MongoClient::m_req_id = 1;
  
  MongoClient::MongoClient(const std::string & host, const std::string & port): MongoClient()
  {
    connect(host, port);
  }

  void MongoClient::connect(const std::string & host, const std::string & port)
  {
    std::string connstr = "tcp://" + host + ":" + port;
    m_sock.connect(connstr);
    m_id_size = _ID_MAX_SIZE;
    m_sock.raw_sock().getsockopt(ZMQ_IDENTITY, m_id, &m_id_size);
    return;
  }
  
  MongoClient::~MongoClient()
  {

  }
  
  void MongoClient::_msg_send(std::string message)
  {
    zmqcpp::Message m(std::string(m_id, m_id_size));
    m.add_frame(message);
    m_sock.send(m, ZMQ_SNDMORE);
  }
  void MongoClient::_msg_recv(reply_pre & intro, std::shared_ptr<unsigned char> & docs)
  {
    zmqcpp::Message reply;
    int consumed, goal, size;
    m_sock >> reply; // grab and kill the id
    m_sock >> reply;
    memcpy(&(intro.head), (char*)reply.frames().back()->c_str(), HEAD_SIZE);
    memcpy(&(intro.rFlags), (char*)reply.frames().back()->c_str() + HEAD_SIZE, sizeof(int));
    memcpy(&(intro.curID), (char*)reply.frames().back()->c_str() + HEAD_SIZE + sizeof(int), sizeof(long));
    memcpy(&(intro.start), (char*)reply.frames().back()->c_str() + HEAD_SIZE + sizeof(int) + sizeof(long), sizeof(int));
    memcpy(&(intro.numRet), (char*)reply.frames().back()->c_str() + HEAD_SIZE + sizeof(int) + sizeof(long)  + sizeof(int), sizeof(int));
    goal = intro.head.len - REPLYPRE_SIZE;
    docs = std::shared_ptr<unsigned char>(new unsigned char [intro.head.len - REPLYPRE_SIZE], []( unsigned char *p ) { delete[] p; });
    memcpy(docs.get(), (char*)reply.frames().back()->c_str() + REPLYPRE_SIZE, reply.frames().back()->size() - REPLYPRE_SIZE);
    consumed = reply.frames().back()->size() - REPLYPRE_SIZE;
    while (consumed < goal)
    {
      m_sock >> reply;
      m_sock >> reply;
      memcpy(docs.get() + consumed, (char*)reply.frames().back()->c_str(), std::min(static_cast<int>(reply.frames().back()->size()), goal - consumed));
      consumed += std::min(static_cast<int>(reply.frames().back()->size()), goal - consumed);
    }
    return;
  }
  void MongoClient::_kill_cursor(const long cursorID)
  {
    std::ostringstream kill;
    _encode_header(kill, HEAD_SIZE, KILL_CURSORS);
    bson::Element::encode(kill, 0);
    bson::Element::encode(kill, 1);
    bson::Element::encode(kill, cursorID);
    _msg_send(kill.str());
  }
  
  void MongoClient::_encode_header(std::ostringstream&ss, const int size, const int type)
  {
    bson::Element::encode(ss, size + HEAD_SIZE);
    bson::Element::encode(ss, m_req_id++);
    bson::Element::encode(ss, 0);
    bson::Element::encode(ss, type);
  }
  
  void MongoClient::_more_cursor(Cursor& c)
  {
    reply_pre intro;
    std::ostringstream ss;
    _encode_header(ss, 17 + c.m_coll.size(), GET_MORE);
    bson::Element::encode(ss, 0);
    ss << c.m_coll.c_str() << bson::X00;
    bson::Element::encode(ss, 0);
    bson::Element::encode(ss, c.m_id);
    _msg_send(ss.str());
    _msg_recv(intro, c.m_docs);
    c.m_id = intro.curID;
    c.m_strsize = intro.head.len - REPLYPRE_SIZE;
    c.m_lastpos = 0;
    return;
  }

  
  bson::Document MongoClient::findOne(const std::string & collection, const bson::Document & query, 
					  const bson::Document & projection, const int flags, const int skip)
  {
    std::ostringstream querystream, header;
    bson::Document qd;
    zmq::message_t reply;
    reply_pre intro;
    int num_returned;
    int docsize, headsize;
    bson::Document result;
    std::shared_ptr<unsigned char> data;
    
    qd.add("$query", query);
    bson::Element::encode(querystream, flags);
    querystream << collection.c_str() << bson::X00;
    bson::Element::encode(querystream, skip);
    bson::Element::encode(querystream, 1);
    bson::Element::encode(querystream, qd);
    if (projection.field_names().size() > 0)
      bson::Element::encode(querystream, projection);
    _encode_header(header, static_cast<int>(querystream.tellp()), QUERY);
    _msg_send(header.str() + querystream.str());
    _msg_recv(intro, data);
    if (intro.numRet == 1)
    {
      bson::Element e;
      e.decode(data.get(), bson::DOCUMENT);
      result = e.data<bson::Document>();
    }
    if (intro.curID != 0)
      _kill_cursor(intro.curID);
    return result;
    
  }
  
  Cursor MongoClient::find(const std::string & collection, const bson::Document & query, 
				   const bson::Document & projection, const int flags, const int skip)
  {
    std::ostringstream querystream, header;
    bson::Document qd;
    zmq::message_t reply;
    reply_pre intro;
    int num_returned;
    int docsize, headsize;
    bson::Document result;
    std::shared_ptr<unsigned char> data;
    
    qd.add("$query", query);
    bson::Element::encode(querystream, flags);
    querystream << collection.c_str() << bson::X00;
    bson::Element::encode(querystream, skip);
    bson::Element::encode(querystream, 0);
    bson::Element::encode(querystream, qd);
    if (projection.field_names().size() > 0)
      bson::Element::encode(querystream, projection);
    _encode_header(header, static_cast<int>(querystream.tellp()), QUERY);
    _msg_send(header.str() + querystream.str());
    _msg_recv(intro, data);
    return Cursor(intro.curID, data, intro.head.len - REPLYPRE_SIZE, collection, *this);
  }
  
  void MongoClient::update(const std::string & collection, const bson::Document & selector, const bson::Document & update,
			   const bool upsert, const bool multi)
  {
    std::ostringstream msg, header;
    bson::Element::encode(msg, 0);
    msg << collection.c_str() << bson::X00;
    bson::Element::encode(msg, static_cast<int>(upsert) | (static_cast<int>(multi)<<1));
    bson::Element::encode(msg, selector);
    bson::Element::encode(msg, update);
    _encode_header(header, static_cast<int>(msg.tellp()), UPDATE);
    _msg_send(header.str() + msg.str());
    return;
  }
  
  void MongoClient::insert(const std::string & collection, const bson::Document & toinsert)
  {
    std::ostringstream msg, header;
    bson::Element::encode(msg, 0);
    msg << collection.c_str() << bson::X00;
    bson::Element::encode(msg, toinsert);
    _encode_header(header, static_cast<int>(msg.tellp()), INSERT);
    _msg_send(header.str() + msg.str());
    return;
  }
  void MongoClient::remove(const std::string & collection, const bson::Document & selector, const bool rm_one)
  {
    std::ostringstream msg, header;
    bson::Element::encode(msg, 0);
    msg << collection.c_str() << bson::X00;
    bson::Element::encode(msg, static_cast<int>(rm_one));
    bson::Element::encode(msg, selector);
    _encode_header(header, static_cast<int>(msg.tellp()), DELETE);
    _msg_send(header.str() + msg.str());
    return;
  }
  
  bson::Document MongoClient::runCommand(const std::string & dbname, const bson::Document & cmd)
  {
    return findOne(dbname + ".$cmd", cmd);
  }
}