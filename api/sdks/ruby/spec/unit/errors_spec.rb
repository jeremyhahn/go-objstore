require "spec_helper"

RSpec.describe "Error Classes" do
  describe ObjectStore::Error do
    it "is a StandardError" do
      expect(ObjectStore::Error.new).to be_a(StandardError)
    end

    it "accepts a message" do
      error = ObjectStore::Error.new("test message")
      expect(error.message).to eq("test message")
    end
  end

  describe ObjectStore::ConnectionError do
    it "is an ObjectStore::Error" do
      expect(ObjectStore::ConnectionError.new).to be_a(ObjectStore::Error)
    end
  end

  describe ObjectStore::NotFoundError do
    it "is an ObjectStore::Error" do
      expect(ObjectStore::NotFoundError.new).to be_a(ObjectStore::Error)
    end

    it "can be raised and caught" do
      expect { raise ObjectStore::NotFoundError, "Object not found" }
        .to raise_error(ObjectStore::NotFoundError, "Object not found")
    end
  end

  describe ObjectStore::ValidationError do
    it "is an ObjectStore::Error" do
      expect(ObjectStore::ValidationError.new).to be_a(ObjectStore::Error)
    end

    it "can be raised and caught" do
      expect { raise ObjectStore::ValidationError, "Invalid key" }
        .to raise_error(ObjectStore::ValidationError, "Invalid key")
    end
  end

  describe ObjectStore::ServerError do
    it "is an ObjectStore::Error" do
      expect(ObjectStore::ServerError.new).to be_a(ObjectStore::Error)
    end

    it "can be raised and caught" do
      expect { raise ObjectStore::ServerError, "Internal server error" }
        .to raise_error(ObjectStore::ServerError, "Internal server error")
    end
  end

  describe ObjectStore::TimeoutError do
    it "is an ObjectStore::Error" do
      expect(ObjectStore::TimeoutError.new).to be_a(ObjectStore::Error)
    end

    it "can be raised and caught" do
      expect { raise ObjectStore::TimeoutError, "Request timed out" }
        .to raise_error(ObjectStore::TimeoutError, "Request timed out")
    end
  end

  describe ObjectStore::ProtocolError do
    it "is an ObjectStore::Error" do
      expect(ObjectStore::ProtocolError.new).to be_a(ObjectStore::Error)
    end

    it "can be raised and caught" do
      expect { raise ObjectStore::ProtocolError, "Protocol mismatch" }
        .to raise_error(ObjectStore::ProtocolError, "Protocol mismatch")
    end
  end

  describe ObjectStore::AuthenticationError do
    it "is an ObjectStore::Error" do
      expect(ObjectStore::AuthenticationError.new).to be_a(ObjectStore::Error)
    end

    it "can be raised and caught" do
      expect { raise ObjectStore::AuthenticationError, "Unauthenticated" }
        .to raise_error(ObjectStore::AuthenticationError, "Unauthenticated")
    end
  end

  describe ObjectStore::AuthorizationError do
    it "is an ObjectStore::Error" do
      expect(ObjectStore::AuthorizationError.new).to be_a(ObjectStore::Error)
    end

    it "can be raised and caught" do
      expect { raise ObjectStore::AuthorizationError, "Forbidden" }
        .to raise_error(ObjectStore::AuthorizationError, "Forbidden")
    end
  end

  describe ObjectStore::AlreadyExistsError do
    it "is an ObjectStore::Error" do
      expect(ObjectStore::AlreadyExistsError.new).to be_a(ObjectStore::Error)
    end

    it "can be raised and caught" do
      expect { raise ObjectStore::AlreadyExistsError, "Already exists" }
        .to raise_error(ObjectStore::AlreadyExistsError, "Already exists")
    end
  end

  describe ObjectStore::RateLimitError do
    it "is an ObjectStore::Error" do
      expect(ObjectStore::RateLimitError.new).to be_a(ObjectStore::Error)
    end

    it "can be raised and caught" do
      expect { raise ObjectStore::RateLimitError, "Rate limited" }
        .to raise_error(ObjectStore::RateLimitError, "Rate limited")
    end
  end
end
