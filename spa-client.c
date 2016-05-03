#include <assert.h>
#include <strings.h>

#include "spa_raft.h"

#define INITIAL_SERVER_IP "127.0.0.1"
#define INITIAL_SERVER_PORT 7001

struct sockaddr_in server;

spa_state_t get() {
  int sockfd = socket(AF_INET, SOCK_DGRAM, 0);

  spa_msg_t msg;
  bzero(&msg, sizeof(msg));
  msg.type = GET;

  sendto(sockfd, &msg, sizeof(msg), 0, (struct sockaddr *)&server,
         sizeof(server));

  ssize_t resplen = recv(sockfd, &msg, sizeof(msg), 0);
  assert(resplen >= 0 && "No response.");

  assert(resplen == sizeof(msg));
  assert(msg.type == GET_RESPONSE);
  return msg.content.get_response;
}

void set(spa_state_t state) {
  int sockfd = socket(AF_INET, SOCK_DGRAM, 0);

  spa_msg_t msg;
  bzero(&msg, sizeof(msg));
  msg.type = ENTRY;
  msg.content.entry.data.buf = &state;
  msg.content.entry.data.len = sizeof(state);

  do {
    sendto(sockfd, &msg, sizeof(msg), 0, (struct sockaddr *)&server,
           sizeof(server));

    ssize_t resplen = recv(sockfd, &msg, sizeof(msg), 0);
    assert(resplen >= 0 && "No response.");

    assert(resplen == sizeof(msg));
    switch (msg.type) {
    case ENTRY_REDIRECT:
      server = msg.content.entry_redirect;
      continue;
      break;
    case ENTRY_RESPONSE:
      break;
    default:
      assert(0);
      break;
    }
  } while (0);
}

int main(int argc, char **argv) {
  bzero(&server, sizeof(server));
  server.sin_family = AF_INET;
  inet_pton(AF_INET, INITIAL_SERVER_IP, &server.sin_addr);
  server.sin_port = htons(INITIAL_SERVER_PORT);

  set(0);
  assert(get() == 0);

  set(1);
  assert(get() == 1);

  return 0;
}
