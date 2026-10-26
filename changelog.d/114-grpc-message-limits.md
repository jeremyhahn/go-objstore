### Added

- The gRPC server's message limits are settable: `grpc.max_recv_msg_size` and
  `grpc.max_send_msg_size` (`OBJSTORE_GRPC_MAX_RECV_MSG_SIZE`,
  `OBJSTORE_GRPC_MAX_SEND_MSG_SIZE`; `-grpc-max-recv-msg-size` and
  `-grpc-max-send-msg-size` on objstore-server, `-max-recv-msg-size` and
  `-max-send-msg-size` on objstore-grpc-server), issue #114. A value below
  1 MiB or above 2147483647 bytes is a configuration error.
  `grpcserver.WithMaxReceiveMessageSize` and `grpcserver.WithMaxSendMessageSize`
  set each limit on its own.

### Changed

- The gRPC server's default receive limit is 32 MiB, up from 10 MiB, so the
  backup agent's default 16 MiB packs upload over gRPC. The send limit stays
  10 MiB.
