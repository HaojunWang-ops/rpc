#include "EventLoop.h"
#include "TcpServer.h"
#include "InetAddress.h"
#include "Buffer.h"
#include "Logging.h"
#include "AsyncLogging.h"

#include <memory>

using namespace reactor;
using namespace reactor::net;

class EchoServer
{
  public:
    EchoServer(EventLoop* loop, const InetAddress& listenAddr);
    
    void start();

    void setNum(int num);
  private:
    void onConnection(const TcpConnectionPtr& conn);
    
    void onMessage(const TcpConnectionPtr& conn, Buffer* buf, Timestamp time);

    TcpServer server_;
};

EchoServer::EchoServer(EventLoop* loop, const InetAddress& listenAddr)
  : server_(loop, listenAddr, "EchoServer")
{
  server_.setConnectionCallback([this](const TcpConnectionPtr& conn){
    this->onConnection(conn);
  });
  server_.setMessageCallback([this](const TcpConnectionPtr& conn, Buffer* buf, Timestamp time){
    this->onMessage(conn, buf, time);
  });
}

void EchoServer::start()
{
  server_.start();
}

void EchoServer::setNum(int num)
{
  server_.setThreadNum(num);
}

void EchoServer::onConnection(const TcpConnectionPtr& conn)
{
  LOG_INFO << "EchoServer - " << conn->peerAddress().toIpPort() << " -> "
           << conn->localAddress().toIpPort() << " is "
           << (conn->connected() ? "UP" : "DOWN");
}

void EchoServer::onMessage(const TcpConnectionPtr& conn, Buffer* buf, Timestamp time)
{
  string msg(buf->retrieveAsString());
  LOG_INFO << conn->name() << " echo " << msg.size() << " bytes, "
           << "data received at " << time.toString();
  conn->send(msg);
}

std::unique_ptr<AsyncLogging> g_asynclogging;

void output(const char* logline, int len)
{
  g_asynclogging->append(logline, len);
}

int main(int argc, char *argv[])
{
  if (argc != 3)
  {
    printf("Usage: %s <port> <ThreadNum> \n", argv[0]);
    printf("Example: %s 9981 4\n", argv[0]);
    return 1;
  }

  int port = atoi(argv[1]);
  int num = atoi(argv[2]);

  g_asynclogging.reset(new AsyncLogging("server", 500 * 1000 * 1000));
  g_asynclogging->start();
  Logger::setOutput(output);

  EventLoop loop;
  InetAddress listenAddr(port);
  EchoServer server(&loop, listenAddr);
  server.setNum(num);

  server.start();

  loop.loop();

  return 0;
}