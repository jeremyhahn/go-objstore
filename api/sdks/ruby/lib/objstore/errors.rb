module ObjectStore
  class Error < StandardError; end
  class ConnectionError < Error; end
  class NotFoundError < Error; end
  class ValidationError < Error; end
  class ServerError < Error; end
  class TimeoutError < Error; end
  class ProtocolError < Error; end
  class AuthenticationError < Error; end
  class AuthorizationError < Error; end
  class AlreadyExistsError < Error; end
  class RateLimitError < Error; end
end
