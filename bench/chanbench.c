/* Costs of the primitives a task channel could use, per platform: making
 * and closing a channel, signaling and consuming with nobody waiting, and
 * a wake-up of a thread parked in poll(). Two descriptors per task, one
 * per side, each signaled by the other side. */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#ifdef __linux__
#include <sys/eventfd.h>
#endif
#ifdef __APPLE__
#include <sys/event.h>
#endif

enum kind { SOCKETPAIR, PIPES, EVENTFD, KQUEUE_USER, KIND_MAX };
static const char *kind_name[] = {"socketpair", "pipes", "eventfd",
                                  "kqueue+EVFILT_USER"};

/* one side of a channel: the descriptor it waits on, and how to signal it */
typedef struct {
  int wait_fd;  /* polled by this side */
  int wake_fd;  /* written by the other side, == wait_fd when self-signaled */
} side;

typedef struct {
  side a, b;
  enum kind kind;
} chan;

static bool chan_supported(enum kind k) {
  switch (k) {
#ifdef __linux__
  case EVENTFD:
    return true;
#endif
#ifdef __APPLE__
  case KQUEUE_USER:
    return true;
#endif
  case SOCKETPAIR:
  case PIPES:
    return true;
  default:
    return false;
  }
}

static int chan_open(chan *c, enum kind k) {
  c->kind = k;
  switch (k) {
  case SOCKETPAIR: {
    int s[2];
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, s) != 0) {
      return -1;
    }
    /* each side waits on its own end, a signal is a byte to the other */
    c->a.wait_fd = s[0];
    c->a.wake_fd = s[1];
    c->b.wait_fd = s[1];
    c->b.wake_fd = s[0];

    return 0;
  }
  case PIPES: {
    int p1[2], p2[2];
    if (pipe(p1) != 0) {
      return -1;
    }
    if (pipe(p2) != 0) {
      close(p1[0]);
      close(p1[1]);

      return -1;
    }
    c->a.wait_fd = p1[0];
    c->a.wake_fd = p1[1];
    c->b.wait_fd = p2[0];
    c->b.wake_fd = p2[1];

    return 0;
  }
#ifdef __linux__
  case EVENTFD: {
    int a = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK | EFD_SEMAPHORE);
    int b = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK | EFD_SEMAPHORE);
    if (a < 0 || b < 0) {
      return -1;
    }
    c->a.wait_fd = c->a.wake_fd = a;
    c->b.wait_fd = c->b.wake_fd = b;

    return 0;
  }
#endif
#ifdef __APPLE__
  case KQUEUE_USER: {
    int fds[2] = {kqueue(), kqueue()};
    if (fds[0] < 0 || fds[1] < 0) {
      return -1;
    }
    for (int i = 0; i < 2; i++) {
      struct kevent ev;
      EV_SET(&ev, 1, EVFILT_USER, EV_ADD | EV_CLEAR, 0, 0, NULL);
      if (kevent(fds[i], &ev, 1, NULL, 0, NULL) != 0) {
        return -1;
      }
    }
    c->a.wait_fd = c->a.wake_fd = fds[0];
    c->b.wait_fd = c->b.wake_fd = fds[1];

    return 0;
  }
#endif
  default:
    return -1;
  }
}

static void chan_close(chan *c) {
  close(c->a.wait_fd);
  close(c->b.wait_fd);
  if (c->kind == PIPES) {
    close(c->a.wake_fd);
    close(c->b.wake_fd);
  }
}

static void chan_signal(chan *c, side *s) {
  switch (c->kind) {
  case SOCKETPAIR:
  case PIPES: {
    char one = 1;
    (void)!write(s->wake_fd, &one, 1);
    break;
  }
  case EVENTFD: {
    uint64_t one = 1;
    (void)!write(s->wake_fd, &one, sizeof(one));
    break;
  }
#ifdef __APPLE__
  case KQUEUE_USER: {
    struct kevent ev;
    EV_SET(&ev, 1, EVFILT_USER, 0, NOTE_TRIGGER, 0, NULL);
    kevent(s->wake_fd, &ev, 1, NULL, 0, NULL);
    break;
  }
#endif
  default:
    break;
  }
}

static bool chan_consume(chan *c, side *s) {
  switch (c->kind) {
  case SOCKETPAIR:
  case PIPES: {
    char buf;

    return read(s->wait_fd, &buf, 1) == 1;
  }
  case EVENTFD: {
    uint64_t v;

    return read(s->wait_fd, &v, sizeof(v)) == (ssize_t)sizeof(v);
  }
#ifdef __APPLE__
  case KQUEUE_USER: {
    struct kevent ev;
    struct timespec zero = {0, 0};

    return kevent(s->wait_fd, NULL, 0, &ev, 1, &zero) == 1;
  }
#endif
  default:
    return false;
  }
}

static double now_us(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);

  return ts.tv_sec * 1e6 + ts.tv_nsec / 1e3;
}

struct pinger {
  chan *c;
  int rounds;
};

/* the waiter: parks in poll() on its side, consumes, answers */
static void *waiter(void *arg) {
  struct pinger *p = arg;
  for (int i = 0; i < p->rounds; i++) {
    struct pollfd pfd = {.fd = p->c->b.wait_fd, .events = POLLIN};
    while (poll(&pfd, 1, 1000) <= 0) {
    }
    chan_consume(p->c, &p->c->b);
    chan_signal(p->c, &p->c->a);
  }

  return NULL;
}

int main(int argc, char **argv) {
  int rounds = argc > 1 ? atoi(argv[1]) : 20000;

  for (enum kind k = 0; k < KIND_MAX; k++) {
    if (!chan_supported(k)) {
      continue;
    }

    chan c;
    double t0 = now_us();
    for (int i = 0; i < rounds; i++) {
      if (chan_open(&c, k) != 0) {
        fprintf(stderr, "%s: open failed: %s\n", kind_name[k], strerror(errno));

        return 1;
      }
      chan_close(&c);
    }
    double open_us = (now_us() - t0) / rounds;

    if (chan_open(&c, k) != 0) {
      return 1;
    }
    t0 = now_us();
    for (int i = 0; i < rounds; i++) {
      chan_signal(&c, &c.a);
      chan_consume(&c, &c.a);
    }
    double hot_us = (now_us() - t0) / rounds;
    chan_close(&c);

    if (chan_open(&c, k) != 0) {
      return 1;
    }
    struct pinger p = {.c = &c, .rounds = rounds / 10};
    pthread_t th;
    pthread_create(&th, NULL, waiter, &p);
    t0 = now_us();
    for (int i = 0; i < p.rounds; i++) {
      chan_signal(&c, &c.b);
      struct pollfd pfd = {.fd = c.a.wait_fd, .events = POLLIN};
      while (poll(&pfd, 1, 1000) <= 0) {
      }
      chan_consume(&c, &c.a);
    }
    double rt_us = (now_us() - t0) / p.rounds;
    pthread_join(th, NULL);
    chan_close(&c);

    printf("%-20s open+close %6.2fus  signal+consume %6.3fus  round trip "
           "%7.2fus\n",
           kind_name[k], open_us, hot_us, rt_us);
  }

  return 0;
}
