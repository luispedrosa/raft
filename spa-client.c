#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <assert.h>

#include "spa_raft.h"

#define INITIAL_SERVER_IP "127.0.0.1"
#define INITIAL_SERVER_PORT 7001

struct sockaddr_in server;

void send_request(spa_msg_t *request, spa_msg_t *response) {
  int sockfd = socket(AF_INET, SOCK_DGRAM, 0);

#ifndef ENABLE_KLEE
  struct timeval tv = { 1, 0 };
  setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, (char *)&tv,
             sizeof(struct timeval));
#endif

  ssize_t resplen;
  while (1) {
#ifndef ENABLE_KLEE
    char srvstr[INET_ADDRSTRLEN + 1 + 5 + 1];
    inet_ntop(AF_INET, &server.sin_addr.s_addr, srvstr, sizeof(srvstr));
    size_t pos = strlen(srvstr);
    srvstr[pos++] = ':';
    snprintf(&srvstr[pos], 6, "%d", ntohs(server.sin_port));

    printf("Sending request to %s\n", srvstr);
#endif
    sendto(sockfd, request, sizeof(spa_msg_t), 0, (struct sockaddr *)&server,
           sizeof(server));

    resplen = recv(sockfd, response, sizeof(spa_msg_t), 0);

    if (resplen < 0) {
      printf("Time out\n");
      continue;
    }

    assert(resplen == sizeof(spa_msg_t));
    if (response->type == REDIRECT) {
      printf("Node not leader, redirecting\n");
      server = response->content.redirect;
#ifndef ENABLE_KLEE
      sleep(1);
#endif
      continue;
      break;
    }
    return;
  }
}

spa_state_t get() {
  printf("Get state\n");

  spa_msg_t request, response;
  bzero(&request, sizeof(spa_msg_t));
  request.type = GET;

  send_request(&request, &response);

  assert(response.type == GET_RESPONSE);
  printf("state == %d\n", response.content.get_response);
  return response.content.get_response;
}

void set(spa_state_t state) {
  printf("Set state = %d\n", state);

  spa_msg_t request, response;
  bzero(&request, sizeof(spa_msg_t));
  request.type = SET;
  request.content.set = state;

  send_request(&request, &response);

  assert(response.type == SET_RESPONSE);
  printf("Success\n");
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
