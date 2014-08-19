/*!
 * \file basic_connection.h
 * \author Nathan Eloe
 * \brief Declaration of a basic, naive connection to the database
 */

#pragma once
#include <memory>
#include <string>
#include <zmq.hpp>

#include "../bson/document.h"
#include "../bson/element.h"
#include "../zmq/socket.h"



namespace mongo
{
  class Cursor;
  
  const int HEAD_SIZE = 16;
  const int REPLYPRE_SIZE = 36;
  
  class MongoClient
  {
    private:
      //Internal types and codes
      friend class Cursor;
      //MongoDB codes
      enum opcodes {REPLY=1, MSG=1000, UPDATE=2001, INSERT, RESERVED, QUERY, GET_MORE, DELETE, KILL_CURSORS};
      struct msg_header {int len, reqID, reTo, opCode;};
      struct reply_pre {msg_header head; int rFlags; long curID; int start, numRet;};
      
      //The maximum size a TCP id can have
      const static size_t _ID_MAX_SIZE = 256;
      //The current request ID
      static int m_req_id;
      //The connection pool (ZMQ sockets)
      zmqcpp::Socket m_sock;
      //The id of the server to send to
      char m_id[_ID_MAX_SIZE];
      //The size of the server's ID
      size_t m_id_size;
      
      /*!
       * \brief sends a message over the zmq socket (abstracts the messiness caused by using ZMQ_STREAM
       * \pre The internal zmq socket should be connected to a database
       * \post The message is sent over the internal zmq socket
       */
      void _msg_send(std::string message);
      /*!
       * \brief receives a message from the zmq socket (handles multiple frames)
       * \pre The internal zmq socket should be connected to a database and be expecting a response
       * \post A (possibly multi-framed) message is received and stored in the array of unsigned characters
       */
      void _msg_recv(reply_pre & intro, std::shared_ptr<unsigned char> & docs);
      /*!
       * \brief Kills a database cursor
       * \pre The internal zmq socket should be connected to a database
       * \post sends the kill cursor message to the connected database
       */
      void _kill_cursor(const long cursorID);
      /*!
       * \brief encodes the common message header
       * \pre None
       * \post the message header is encoded in the output string stream
       */
      void _encode_header(std::ostringstream & ss, const int size, const int type);
      /*!
       * \brief gets more information for the cursor to load
       * \pre this connection should be the same one that created the cursor (same host/port pair)
       * \post iterates the database cursor
       */
      void _more_cursor(Cursor & c);
    public:
      /*!
       * \brief Constructors
       * \pre The context should be passed if it has been created for other ZMQ sockets
       * \post The client is constructed using the context specified.  If *ctx == nullptr, creates a new zmq context
       */
      MongoClient():m_sock(ZMQ_STREAM), m_id(), m_id_size(0) {}
      /*!
       * \brief Connection Constructors
       * \pre None
       * \post constructs the client object and connects to the database
       */
      MongoClient(const std::string & host, const std::string & port = "27017");
      
      /*!
       * \brief Destructor
       * \pre None
       * \post Destructs and cleans up the object
       */
      ~MongoClient();
      
      /*!
       * \brief connects to the database
       * \pre None
       * \post creates a connection to the database at the specified host and port
       */
      
      void connect(const std::string & host, const std::string & port = "27017");
      
      // Database Operations (CRUD)
      
      /*!
       * \brief finds and returns a single document
       * \pre None
       * \post Performs a query on the database
       * \return a single matching Document; if no match, returns an empty Document
       */
      bson::Document findOne(const std::string & collection, const bson::Document & query = bson::Document(), 
			     const bson::Document & projection = bson::Document(), const int flags = 0,
			     const int skip = 0);
      
      /*!
       * \brief Finds and returns a cursor to multiple documents
       * \pre None
       * \post Performs a query on the database
       * \return A Cursor object containing the database cursor to get data out of
       */
      Cursor find(const std::string & collection, const bson::Document & query = bson::Document(), 
		  const bson::Document & projection = bson::Document(), const int flags = 0,
		  const int skip = 0);
      
      /*!
       * \brief Runs an update operation on the database
       * \pre None
       * \post Runs the update operation on the database
       */
      void update(const std::string & collection, const bson::Document & selector, const bson::Document & update,
		  const bool upsert = false, const bool multi = false);
      
      /*!
       * \brief Runs an insertion operation on the database
       * \pre None
       * \post Inserts the document into the database
       */
      void insert(const std::string & collection, const bson::Document & toinsert);
      
      /*!
       * \brief Runs a removal operation on the database
       * \pre None
       * \post Removes the specified selector from the database (defaults to removing a single element)
       */
      void remove(const std::string & collection, const bson::Document & selector, const bool rm_one=true);
      
      // Database Command functions
      
      /*!
       * \brief runs the specified database command
       * \pre None
       * \post Runs the database command
       * \return the resulting bson document from running the command
       */
      bson::Document runCommand(const std::string & dbname, const bson::Document & cmd);
      /*!
       * \brief runs the specified database command
       * \pre None
       * \post Runs the database command {cmd: args}
       * \return the resulting bson document from running the command
       */
      bson::Document runCommand(const std::string & dbname, const std::string & cmd, const bson::Element args = 1) {return runCommand(dbname, {{cmd, args}});}
      
  };
}